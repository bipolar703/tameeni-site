import asyncio
import logging
from pathlib import Path
from typing import Dict, Any, List, Set, Optional
from urllib.parse import urljoin, urlparse

import aiohttp
import aiofiles
from bs4 import BeautifulSoup, Tag
from esprima import parseScript, parseModule
from astunparse import replace
from escodegen import generate
from swc import parse as swc_parse, transform as swc_transform
from swc import Options, JscConfig, TransformConfig
from functools import lru_cache
import hashlib
import json
from pydantic import BaseModel

from utils import ensure_url_scheme, sanitize_filename, get_content_hash
from directory_manager import DirectoryManager

logger = logging.getLogger(__name__)

class JSModule(BaseModel):
    url: str
    content: str
    dependencies: Set[str] = set()

class JSProcessor:
    def __init__(self, directory_manager: DirectoryManager, base_url: str):
        self.directory_manager = directory_manager
        self.base_url = ensure_url_scheme(base_url)
        self.processed_scripts: Dict[str, str] = {}
        self.modules: Dict[str, JSModule] = {}
        self.module_graph: Dict[str, Set[str]] = {}
        self.swc_options = Options(
            jsc=JscConfig(
                parser={"syntax": "ecmascript", "jsx": True},
                transform=TransformConfig(react={"pragma": "React.createElement"})
            ),
            minify=True
        )

    async def process_javascript(self, soup: BeautifulSoup, page_url: str) -> BeautifulSoup:
        logger.info(f"Processing JavaScript for {page_url}")
        tasks = [self.process_script(tag, page_url) for tag in soup.find_all('script')]
        await asyncio.gather(*tasks)

        await self.resolve_module_dependencies()
        await self.bundle_modules(soup)
        await self.process_dynamic_imports(soup)
        await self.handle_web_workers(soup)
        await self.optimize_async_code(soup)

        return soup

    async def process_script(self, tag: Tag, page_url: str):
        if tag.get('src'):
            await self.process_external_script(tag, page_url)
        elif tag.string:
            await self.process_inline_script(tag, page_url)

    async def process_external_script(self, tag: Tag, page_url: str):
        src = tag['src']
        full_url = urljoin(page_url, src)
        if full_url.startswith(self.base_url) and full_url not in self.processed_scripts:
            content = await self.fetch_js(full_url)
            if content:
                processed_content = await self.process_js_content(content, full_url)
                file_path = await self.save_js_file(full_url, processed_content)
                self.processed_scripts[full_url] = processed_content
                tag['src'] = str(file_path.relative_to(self.directory_manager.root_directory))

                if tag.get('type') == 'module':
                    module = JSModule(url=full_url, content=processed_content)
                    module.dependencies = await self.extract_module_dependencies(processed_content)
                    self.modules[full_url] = module

    async def process_inline_script(self, tag: Tag, page_url: str):
        content = tag.string
        if content:
            processed_content = await self.process_js_content(content, page_url)
            tag.string = processed_content

            if tag.get('type') == 'module':
                module_id = f"inline-{get_content_hash(content)}"
                module = JSModule(url=module_id, content=processed_content)
                module.dependencies = await self.extract_module_dependencies(processed_content)
                self.modules[module_id] = module

    async def fetch_js(self, url: str) -> Optional[str]:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        return await response.text()
                    else:
                        logger.warning(f"Failed to fetch JS from {url}: HTTP {response.status}")
            except aiohttp.ClientError as e:
                logger.error(f"Error fetching JS from {url}: {e}")
        return None

    async def process_js_content(self, content: str, url: str) -> str:
        try:
            # Parse the JavaScript content
            ast = self.parse_js(content)

            # Transform the AST
            transformed_ast = await self.transform_ast(ast, url)

            # Generate code from the transformed AST
            generated_code = self.generate_code(transformed_ast)

            # Use SWC for transpilation and minification
            minified = await self.swc_process(generated_code)

            return minified
        except Exception as e:
            logger.error(f"Error processing JS content from {url}: {e}")
            return content

    def parse_js(self, content: str):
        try:
            return parseModule(content, {'jsx': True, 'range': True, 'tokens': True})
        except:
            return parseScript(content, {'jsx': True, 'range': True, 'tokens': True})

    async def transform_ast(self, ast, base_url: str):
        def visit(node):
            if node.type == 'CallExpression':
                if node.callee.type == 'Identifier':
                    if node.callee.name in ['require', 'import']:
                        return self.transform_module_import(node, base_url)
                    elif node.callee.name in ['fetch', 'axios.get']:
                        return self.transform_fetch_call(node, base_url)
            elif node.type == 'ImportDeclaration':
                return self.transform_import_declaration(node, base_url)
            elif node.type in ['ExportNamedDeclaration', 'ExportDefaultDeclaration']:
                return self.transform_export_declaration(node)
            return node

        return replace(ast, {'enter': visit})

    def transform_module_import(self, node, base_url: str):
        if node.arguments[0].type == 'Literal':
            module_path = node.arguments[0].value
            full_path = urljoin(base_url, module_path)
            relative_path = self.get_relative_path(base_url, full_path)
            node.arguments[0].value = relative_path
        return node

    def transform_fetch_call(self, node, base_url: str):
        if node.arguments[0].type == 'Literal':
            url = node.arguments[0].value
            full_url = urljoin(base_url, url)
            relative_url = self.get_relative_path(base_url, full_url)
            node.arguments[0].value = relative_url
        return node

    def transform_import_declaration(self, node, base_url: str):
        source = node.source.value
        full_path = urljoin(base_url, source)
        relative_path = self.get_relative_path(base_url, full_path)
        node.source.value = relative_path
        return node

    def transform_export_declaration(self, node):
        # Implement export transformation logic if needed
        return node

    def generate_code(self, ast):
        return generate(ast)

    async def swc_process(self, code: str) -> str:
        # Use SWC for transpilation and minification
        result = swc_transform(code, self.swc_options)
        return result.code

    async def resolve_module_dependencies(self):
        for module in self.modules.values():
            for dep in module.dependencies:
                full_dep_url = urljoin(self.base_url, dep)
                if full_dep_url not in self.modules:
                    content = await self.fetch_js(full_dep_url)
                    if content:
                        processed_content = await self.process_js_content(content, full_dep_url)
                        dep_module = JSModule(url=full_dep_url, content=processed_content)
                        dep_module.dependencies = await self.extract_module_dependencies(processed_content)
                        self.modules[full_dep_url] = dep_module

                if module.url not in self.module_graph:
                    self.module_graph[module.url] = set()
                self.module_graph[module.url].add(full_dep_url)

    @lru_cache(maxsize=None)
    def get_relative_path(self, base_url: str, full_url: str) -> str:
        parsed_base = urlparse(base_url)
        parsed_full = urlparse(full_url)
        relative_path = parsed_full.path.replace(parsed_base.path, '')
        return relative_path.lstrip('/')

    async def extract_module_dependencies(self, content: str) -> Set[str]:
        ast = self.parse_js(content)
        dependencies = set()

        def visit(node):
            if node.type == 'ImportDeclaration':
                dependencies.add(node.source.value)
            elif node.type == 'CallExpression' and node.callee.name == 'require':
                if node.arguments[0].type == 'Literal':
                    dependencies.add(node.arguments[0].value)

        replace(ast, {'enter': visit})
        return dependencies

    async def bundle_modules(self, soup: BeautifulSoup):
        bundle_content = "".join(f"// Module: {module.url}\n{module.content}\n\n"
                                 for module in self.modules.values())

        bundle_path = self.directory_manager.root_directory / 'bundle.js'
        await self.save_js_file(str(bundle_path), bundle_content)

        # Replace all module scripts with the bundled script
        for tag in soup.find_all('script', type='module'):
            tag['src'] = str(bundle_path.relative_to(self.directory_manager.root_directory))
            del tag['type']

    async def process_dynamic_imports(self, soup: BeautifulSoup):
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string:
                processed_content = await self.handle_dynamic_import(script.string)
                script.string = processed_content

    async def handle_dynamic_import(self, content: str) -> str:
        ast = self.parse_js(content)
        
        def transform(node):
            if node.type == 'ImportExpression':
                # Transform dynamic import to a custom function
                return {
                    'type': 'CallExpression',
                    'callee': {
                        'type': 'Identifier',
                        'name': '__customDynamicImport'
                    },
                    'arguments': [node.source]
                }
            return node

        transformed_ast = replace(ast, {'enter': transform})
        return self.generate_code(transformed_ast)

    async def handle_web_workers(self, soup: BeautifulSoup):
        workers = soup.find_all('script', type='worker')
        for worker in workers:
            if worker.get('src'):
                worker_url = urljoin(self.base_url, worker['src'])
                worker_content = await self.fetch_js(worker_url)
                if worker_content:
                    processed_content = await self.process_js_content(worker_content, worker_url)
                    worker_path = await self.save_js_file(worker_url, processed_content)
                    worker['src'] = str(worker_path.relative_to(self.directory_manager.root_directory))

    async def optimize_async_code(self, soup: BeautifulSoup):
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string:
                optimized_content = await self.optimize_async_functions(script.string)
                script.string = optimized_content

    async def optimize_async_functions(self, content: str) -> str:
        ast = self.parse_js(content)

        def transform(node):
            # Implement transformation logic for async functions optimization
            return node

        transformed_ast = replace(ast, {'enter': transform})
        return self.generate_code(transformed_ast)

    async def save_js_file(self, url: str, content: str) -> Path:
        file_path = self.directory_manager.get_file_path(url)
        await self.directory_manager.ensure_parent_directory(file_path)
        async with aiofiles.open(file_path, 'w') as f:
            await f.write(content)
        logger.info(f"Saved JS file: {file_path}")
        return file_path

async def process_javascript(session: aiohttp.ClientSession,
                             soup: BeautifulSoup,
                             directory_manager: DirectoryManager,
                             base_url: str) -> BeautifulSoup:
    js_processor = JSProcessor(directory_manager, base_url)
    return await js_processor.process_javascript(soup, base_url)