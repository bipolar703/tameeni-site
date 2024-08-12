import asyncio
import logging
import re
import hashlib
from pathlib import Path
from typing import Set, Dict, List, Union
from urllib.parse import urlparse, urljoin
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
import cssutils
from cssmin import cssmin
import sass
import lesscpy
from stylus import Stylus
from directory_manager import DirectoryManager
from utils import sanitize_filename, ensure_url_scheme

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress cssutils logging
cssutils.log.setLevel(logging.CRITICAL)

class CSSResourceManager:
    def __init__(self, session: aiohttp.ClientSession, directory_manager: DirectoryManager, root_url: str):
        self.session = session
        self.directory_manager = directory_manager
        self.root_url = ensure_url_scheme(root_url)
        self.resource_url_set: Set[str] = set()
        self.css_cache: Dict[str, str] = {}
        self.stylus_compiler = Stylus()

    async def process_css_resources(self, soup: BeautifulSoup, page_url: str, output_dir: Path) -> BeautifulSoup:
        css_tasks = []
        inline_css_tasks = []

        # Process external CSS files
        for link in soup.find_all('link', rel='stylesheet'):
            if link.get('href'):
                css_url = urljoin(page_url, link['href'])
                task = self.process_css_file(css_url, output_dir, page_url)
                css_tasks.append(task)

        # Process inline styles
        for style in soup.find_all('style'):
            if style.string:
                task = self.process_inline_css(style.string, page_url, output_dir)
                inline_css_tasks.append(task)

        # Process style attributes
        for tag in soup.find_all(style=True):
            task = self.process_inline_css(tag['style'], page_url, output_dir, is_attribute=True)
            inline_css_tasks.append((tag, task))

        # Wait for all CSS processing tasks to complete
        processed_css_files = await asyncio.gather(*css_tasks)
        processed_inline_styles = await asyncio.gather(*[task for task in inline_css_tasks])

        # Update soup with processed CSS files
        for link, processed_file in zip(soup.find_all('link', rel='stylesheet'), processed_css_files):
            if processed_file:
                link['href'] = processed_file

        # Update soup with processed inline styles and style attributes
        for style, processed_css in zip(soup.find_all('style'), processed_inline_styles):
            style.string = processed_css
        
        for (tag, task), processed_css in zip(soup.find_all(style=True), processed_inline_styles):
            if 'style' in tag.attrs:
                tag['style'] = processed_css

        return soup

    async def process_css_file(self, url: str, output_dir: Path, base_url: str) -> Union[str, None]:
        cache_key = hashlib.md5(url.encode()).hexdigest()
        if cache_key in self.css_cache:
            return self.css_cache[cache_key]

        if url in self.resource_url_set:
            return self.css_cache.get(url)

        self.resource_url_set.add(url)
        css_content = await self.fetch_url(url)
        if not css_content:
            return None

        try:
            processed_css = await self.process_css_content(css_content, url, base_url)
            file_path = output_dir / Path(urlparse(url).path.lstrip('/'))
            await self.save_css_file(file_path, processed_css)
            relative_path = file_path.relative_to(output_dir)
            self.css_cache[url] = str(relative_path)
            self.css_cache[cache_key] = str(relative_path)
            return str(relative_path)
        except Exception as e:
            logger.error(f"Error processing CSS file {url}: {e}")
            return None

    async def process_inline_css(self, css_content: str, base_url: str, output_dir: Path, is_attribute: bool = False) -> str:
        try:
            processed_css = await self.process_css_content(css_content, base_url, base_url, is_attribute)
            return processed_css
        except Exception as e:
            logger.error(f"Error processing inline CSS: {e}")
            return css_content

    async def process_css_content(self, content: str, url: str, base_url: str, is_attribute: bool = False) -> str:
        # Preprocess CSS if necessary
        if url.endswith(('.scss', '.sass')):
            content = await self.preprocess_sass(content)
        elif url.endswith('.less'):
            content = await self.preprocess_less(content)
        elif url.endswith('.styl'):
            content = await self.preprocess_stylus(content)

        # Resolve imports and URLs
        content = await self.resolve_css_imports_and_urls(content, base_url)

        # Parse CSS
        css_sheet = cssutils.parseString(content)

        # Process CSS rules
        for rule in css_sheet:
            if rule.type == rule.STYLE_RULE:
                self.process_style_rule(rule, base_url)
            elif rule.type == rule.IMPORT_RULE:
                await self.process_import_rule(rule, url, base_url)

        # Generate processed CSS
        processed_css = css_sheet.cssText.decode('utf-8')

        # Minify CSS if it's not a style attribute
        if not is_attribute:
            processed_css = await self.minify_css(processed_css)

        return processed_css

    def process_style_rule(self, rule, base_url: str):
        for property in rule.style:
            if property.name in ['background-image', 'background', 'border-image', 'list-style-image']:
                urls = cssutils.getUrls(property.value)
                for old_url in urls:
                    new_url = self.get_absolute_url(old_url, base_url)
                    property.value = property.value.replace(old_url, new_url)

    async def process_import_rule(self, rule, current_url: str, base_url: str):
        import_url = urljoin(current_url, rule.href)
        if import_url not in self.resource_url_set:
            self.resource_url_set.add(import_url)
            imported_css = await self.fetch_url(import_url)
            if imported_css:
                processed_css = await self.process_css_content(imported_css, import_url, base_url)
                rule.cssText = f"@import url('{import_url}');\n{processed_css}"

    async def preprocess_sass(self, content: str) -> str:
        return sass.compile(string=content)

    async def preprocess_less(self, content: str) -> str:
        return lesscpy.compile_string(content)

    async def preprocess_stylus(self, content: str) -> str:
        return self.stylus_compiler.compile(content)

    async def resolve_css_imports_and_urls(self, content: str, base_url: str) -> str:
        # Placeholder for actual implementation to resolve relative URLs
        return content

    async def minify_css(self, content: str) -> str:
        return cssmin(content)

    async def fetch_url(self, url: str) -> str:
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logger.warning(f"Failed to fetch {url}: HTTP {response.status}")
                    return ""
        except aiohttp.ClientError as e:
            logger.error(f"Error fetching {url}: {e}")
            return ""

    async def save_css_file(self, file_path: Path, content: str) -> None:
        await self.directory_manager.ensure_parent_directory(file_path)
        async with aiofiles.open(file_path, mode='w', encoding='utf-8') as f:
            await f.write(content)
        logger.info(f"Saved processed CSS: {file_path}")

    def get_absolute_url(self, url: str, base_url: str) -> str:
        if url.startswith(('http://', 'https://', '//')):
            return url
        return urljoin(base_url, url)

    async def process_css_in_js(self, js_content: str, base_url: str) -> str:
        css_in_js_pattern = r'css`([^`]+)`'
        matches = re.findall(css_in_js_pattern, js_content)

        for css_content in matches:
            processed_css = await self.process_css_content(css_content, base_url, base_url)
            js_content = js_content.replace(f'css`{css_content}`', f'css`{processed_css}`')

        return js_content

async def process_css_resources(session: aiohttp.ClientSession,
                                 soup: BeautifulSoup,
                                 directory_manager: DirectoryManager,
                                 root_url: str,
                                 output_dir: Path) -> BeautifulSoup:
    css_manager = CSSResourceManager(session, directory_manager, root_url)
    return await css_manager.process_css_resources(soup, root_url, output_dir)

# Example usage
if __name__ == "__main__":
    async def main():
        async with aiohttp.ClientSession() as session:
            directory_manager = DirectoryManager(Path("./output"))
            root_url = "https://example.com"
            output_dir = Path("./output")

            html_content = """
            <html>
                <head>
                    <link rel="stylesheet" href="/styles/main.css">
                    <style>
                        body { font-family: Arial, sans-serif; }
                    </style>
                </head>
                <body>
                    <div style="background: url('/images/bg.png');">Hello, World!</div>
                </body>
            </html>
            """

            soup = BeautifulSoup(html_content, 'html.parser')
            processed_soup = await process_css_resources(session, soup, directory_manager, root_url, output_dir)

            print(processed_soup.prettify())

    asyncio.run(main())