import asyncio
from concurrent.futures import ThreadPoolExecutor
import hashlib
import logging
from pathlib import Path
import re
from typing import Any, Set, Dict, List, Union, Optional
from urllib.parse import urlparse, urljoin
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
import cssutils
from csscompressor import compress as css_compress
from tinycss2 import parse_stylesheet, serialize
from py_mini_racer import MiniRacer
from directory_manager import DirectoryManager
from utils import sanitize_filename, ensure_url_scheme
import lesscpy
import sass
from stylus import Stylus
import brotli
import zstandard as zstd
from PIL import Image
from io import BytesIO
import aioredis
from cachetools import TTLCache
from prometheus_client import Counter, Histogram
from jsmin import jsmin
from css_html_js_minify import html_minify as advanced_html_minify, css_minify
import fontTools.ttLib
import rjsmin

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress cssutils logging
cssutils.log.setLevel(logging.CRITICAL)

# Prometheus metrics
RESOURCE_DOWNLOAD_COUNT = Counter('resource_download_count', 'Number of resources downloaded', ['resource_type'])
RESOURCE_DOWNLOAD_TIME = Histogram('resource_download_time', 'Time taken to download resources', ['resource_type'])

class ResourceManager:
    def __init__(self, session: aiohttp.ClientSession, directory_manager: DirectoryManager, root_url: str, config: Dict[str, Any]):
        self.session = session
        self.directory_manager = directory_manager
        self.root_url = ensure_url_scheme(root_url)
        self.resource_url_set: Set[str] = set()
        self.css_cache: TTLCache = TTLCache(maxsize=1000, ttl=3600)
        self.config = config
        self.js_context = MiniRacer()
        self.executor = ThreadPoolExecutor(max_workers=self.config['max_workers'])
        self.semaphore = asyncio.Semaphore(self.config['max_concurrent_downloads'])
        self.redis = aioredis.from_url(self.config['redis_url'])
        self.stylus_compiler = Stylus()

    async def process_resources(self, soup: BeautifulSoup, page_url: str, output_dir: Path) -> BeautifulSoup:
        tasks = [self.process_tag(tag, page_url, output_dir) for tag in soup.find_all(['img', 'script', 'link', 'video', 'audio', 'source', 'iframe'])]
        await asyncio.gather(*tasks)
        return soup

    async def process_tag(self, tag, page_url: str, output_dir: Path):
        if tag.name == 'img' and tag.get('src'):
            return self.download_and_replace_resource(tag, 'src', page_url, output_dir, 'image')
        elif tag.name == 'script':
            if tag.get('src'):
                return self.download_and_replace_resource(tag, 'src', page_url, output_dir, 'script')
            elif tag.string:
                return self.process_inline_script(tag, page_url, output_dir)
        elif tag.name == 'link' and tag.get('href'):
            if tag.get('rel') == ['stylesheet']:
                return self.download_and_replace_resource(tag, 'href', page_url, output_dir, 'css')
            elif tag.get('rel') == ['icon']:
                return self.download_and_replace_resource(tag, 'href', page_url, output_dir, 'favicon')
        elif tag.name in ['video', 'audio'] and tag.get('src'):
            return self.download_and_replace_resource(tag, 'src', page_url, output_dir, 'media')
        elif tag.name == 'source' and tag.get('src'):
            return self.download_and_replace_resource(tag, 'src', page_url, output_dir, 'media')
        elif tag.name == 'iframe' and tag.get('src'):
            return self.process_iframe(tag, page_url, output_dir)

    async def download_and_replace_resource(self, tag, attr: str, page_url: str, output_dir: Path, resource_type: str):
        url = urljoin(page_url, tag[attr])
        if url in self.resource_url_set:
            return

        self.resource_url_set.add(url)
        try:
            async with self.semaphore:
                with RESOURCE_DOWNLOAD_TIME.labels(resource_type).time():
                    content = await self.fetch_resource(url)
                    if content:
                        file_path = await self.save_resource(url, content, output_dir, resource_type)
                        tag[attr] = str(file_path.relative_to(output_dir))
                        RESOURCE_DOWNLOAD_COUNT.labels(resource_type).inc()
        except Exception as e:
            logger.error(f"Error processing resource {url}: {str(e)}")

    async def process_inline_script(self, tag, page_url: str, output_dir: Path):
        try:
            script_content = tag.string
            processed_content = await self.process_js_content(script_content, page_url)
            tag.string = processed_content
        except Exception as e:
            logger.error(f"Error processing inline script: {str(e)}")

    async def process_iframe(self, tag, page_url: str, output_dir: Path):
        iframe_url = urljoin(page_url, tag['src'])
        try:
            async with self.semaphore:
                content = await self.fetch_resource(iframe_url)
                if content:
                    iframe_soup = BeautifulSoup(content, 'lxml')
                    processed_iframe = await self.process_resources(iframe_soup, iframe_url, output_dir)
                    iframe_path = output_dir / f"iframe_{hashlib.md5(iframe_url.encode()).hexdigest()}.html"
                    await self.save_processed_html(processed_iframe, iframe_path)
                    tag['src'] = str(iframe_path.relative_to(output_dir))
        except Exception as e:
            logger.error(f"Error processing iframe {iframe_url}: {str(e)}")

    async def fetch_resource(self, url: str) -> Optional[bytes]:
        try:
            cached_content = await self.redis.get(url)
            if cached_content:
                return cached_content

            async with self.session.get(url, timeout=self.config['resource_timeout']) as response:
                if response.status == 200:
                    content = await response.read()
                    await self.redis.set(url, content, expire=3600)  # Cache for 1 hour
                    return content
                else:
                    logger.warning(f"Failed to fetch {url}: HTTP {response.status}")
        except Exception as e:
            logger.error(f"Error fetching {url}: {str(e)}")
        return None

    async def save_resource(self, url: str, content: bytes, output_dir: Path, resource_type: str) -> Path:
        file_path = output_dir / resource_type / sanitize_filename(urlparse(url).path.lstrip('/'))
        await self.directory_manager.ensure_parent_directory(file_path)

        if resource_type == 'image':
            content = await self.optimize_image(content, file_path)
        elif resource_type == 'script':
            content = await self.process_js_content(content.decode(), url)
            content = content.encode()
        elif resource_type == 'css':
            content = await self.process_css_content(content.decode(), url)
            content = content.encode()
        elif resource_type == 'favicon':
            content = await self.optimize_favicon(content, file_path)

        compressed_content = await self.compress_content(content)

        async with aiofiles.open(file_path, 'wb') as f:
            await f.write(compressed_content)

        logger.info(f"Saved resource: {file_path}")
        return file_path

    async def optimize_image(self, content: bytes, file_path: Path) -> bytes:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self._optimize_image_sync, content, file_path)

    def _optimize_image_sync(self, content: bytes, file_path: Path) -> bytes:
        with Image.open(BytesIO(content)) as img:
            if img.mode in ('RGBA', 'LA'):
                background = Image.new(img.mode[:-1], img.size, (255, 255, 255))
                background.paste(img, img.split()[-1])
                img = background
            if file_path.suffix.lower() in ['.jpg', '.jpeg']:
                format_to_save = 'JPEG'
            elif file_path.suffix.lower() == '.png':
                format_to_save = 'PNG'
            else:
                format_to_save = 'WEBP'
            buffer = BytesIO()
            img.save(buffer, format=format_to_save, quality=85, optimize=True)
        return buffer.getvalue()

    async def optimize_favicon(self, content: bytes, file_path: Path) -> bytes:
        if file_path.suffix.lower() == '.ico':
            return content  # ICO files are already optimized
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self._optimize_favicon_sync, content, file_path)

    def _optimize_favicon_sync(self, content: bytes, file_path: Path) -> bytes:
        with Image.open(BytesIO(content)) as img:
            buffer = BytesIO()
            img.save(buffer, format='ICO', sizes=[(16, 16), (32, 32), (48, 48)])
        return buffer.getvalue()

    async def process_js_content(self, content: str, url: str) -> str:
        try:
            # Use rjsmin for faster minification
            minified = rjsmin.jsmin(content)
            
            # Apply custom transformations
            transformed = await self.transform_js(minified, url)
            
            return transformed
        except Exception as e:
            logger.error(f"Error processing JavaScript from {url}: {str(e)}")
            return content

    async def transform_js(self, content: str, url: str) -> str:
        # Implement custom JavaScript transformations here
        # For example, you could replace certain API endpoints, modify global variables, etc.
        return content

    async def process_css_content(self, content: str, url: str) -> str:
        try:
            if url.endswith(('.scss', '.sass')):
                content = self.compile_sass(content)
            elif url.endswith('.less'):
                content = self.compile_less(content)
            elif url.endswith('.styl'):
                content = self.compile_stylus(content)

            content = await self.resolve_css_imports_and_urls(content, url)
            return css_minify(content)
        except Exception as e:
            logger.error(f"Error processing CSS from {url}: {str(e)}")
            return content

    def compile_sass(self, content: str) -> str:
        return sass.compile(string=content)

    def compile_less(self, content: str) -> str:
        return lesscpy.compile_string(content)

    def compile_stylus(self, content: str) -> str:
        return self.stylus_compiler.compile(content)

    async def resolve_css_imports_and_urls(self, content: str, base_url: str) -> str:
        stylesheet = parse_stylesheet(content)
        for rule in stylesheet:
            if rule.type == 'at-rule' and rule.lower_at_keyword == 'import':
                await self.resolve_css_import(rule, base_url)
            elif rule.type == 'qualified-rule':
                self.resolve_css_urls(rule, base_url)
        return serialize(stylesheet)

    async def resolve_css_import(self, rule, base_url: str):
        if rule.prelude:
            import_url = rule.prelude[0].value.strip('"\'')
            absolute_url = urljoin(base_url, import_url)
            imported_content = await self.fetch_resource(absolute_url)
            if imported_content:
                rule.content = parse_stylesheet(imported_content.decode('utf-8'))

    def resolve_css_urls(self, rule, base_url: str):
        for token in rule.prelude + rule.content:
            if token.type == 'function' and token.lower_name == 'url':
                url_token = token.arguments[0]
                if url_token.type == 'string':
                    url = url_token.value.strip('"\'')
                    absolute_url = urljoin(base_url, url)
                    url_token.value = f'"{absolute_url}"'

    async def compress_content(self, content: bytes) -> bytes:
        if self.config['compression'] == 'brotli':
            return brotli.compress(content)
        elif self.config['compression'] == 'zstd':
            return zstd.compress(content)
        return content

    async def process_fonts(self, soup: BeautifulSoup, page_url: str, output_dir: Path) -> BeautifulSoup:
        font_tags = soup.find_all('link', rel='stylesheet', href=lambda href: href and 'fonts' in href.lower())
        for tag in font_tags:
            font_url = urljoin(page_url, tag['href'])
            processed_font = await self.optimize_font(font_url, output_dir)
            if processed_font:
                tag['href'] = str(processed_font.relative_to(output_dir))
        return soup

    async def optimize_font(self, font_url: str, output_dir: Path) -> Optional[Path]:
        content = await self.fetch_resource(font_url)
        if not content:
            return None

        font_file = output_dir / 'fonts' / sanitize_filename(urlparse(font_url).path.lstrip('/'))
        await self.directory_manager.ensure_parent_directory(font_file)

        # Subset the font based on the characters used in the webpage
        used_chars = await self.get_used_characters()
        loop = asyncio.get_event_loop()
        optimized_font = await loop.run_in_executor(self.executor, self._subset_font, content, used_chars)

        async with aiofiles.open(font_file, 'wb') as f:
            await f.write(optimized_font)

        return font_file

    def _subset_font(self, font_content: bytes, used_chars: str) -> bytes:
        font = fontTools.ttLib.TTFont(BytesIO(font_content))
        subsetter = fontTools.subset.Subsetter()
        subsetter.populate(text=used_chars)
        subsetter.subset(font)
        output = BytesIO()
        font.save(output)
        return output.getvalue()

    async def get_used_characters(self) -> str:
        # Implement logic to extract used characters from the webpage
        # This could involve parsing the HTML and extracting text content
        # For simplicity, we'll return a basic set of characters
        return "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

    async def handle_web_workers(self, soup: BeautifulSoup, page_url: str, output_dir: Path) -> BeautifulSoup:
        script_tags = soup.find_all('script')
        for tag in script_tags:
            if tag.string and 'new Worker(' in tag.string:
                processed_script = await self.process_web_worker_script(tag.string, page_url, output_dir)
                tag.string = processed_script
        return soup

    async def process_web_worker_script(self, script_content: str, page_url: str, output_dir: Path) -> str:
        worker_url_match = re.search(r'new Worker\([\'"](.+?)[\'"]\)', script_content)
        if worker_url_match:
            worker_url = urljoin(page_url, worker_url_match.group(1))
            worker_content = await self.fetch_resource(worker_url)
            if worker_content:
                worker_file = output_dir / 'js' / f"worker_{hashlib.md5(worker_url.encode()).hexdigest()}.js"
                await self.directory_manager.ensure_parent_directory(worker_file)
                async with aiofiles.open(worker_file, 'wb') as f:
                    await f.write(worker_content)
                relative_path = worker_file.relative_to(output_dir)
                script_content = script_content.replace(worker_url_match.group(1), str(relative_path))
        return script_content

    async def handle_service_workers(self, soup: BeautifulSoup, page_url: str, output_dir: Path) -> BeautifulSoup:
        script_tags = soup.find_all('script')
        for tag in script_tags:
            if tag.string and 'navigator.serviceWorker.register' in tag.string:
                processed_script = await self.process_service_worker_script(tag.string, page_url, output_dir)
                tag.string = processed_script
        return soup

    async def process_service_worker_script(self, script_content: str, page_url: str, output_dir: Path) -> str:
        sw_url_match = re.search(r'navigator\.serviceWorker\.register\([\'"](.+?)[\'"]\)', script_content)
        if sw_url_match:
            sw_url = urljoin(page_url, sw_url_match.group(1))
            sw_content = await self.fetch_resource(sw_url)
            if sw_content:
                sw_file = output_dir / 'js' / f"sw_{hashlib.md5(sw_url.encode()).hexdigest()}.js"
                await self.directory_manager.ensure_parent_directory(sw_file)
                async with aiofiles.open(sw_file, 'wb') as f:
                    await f.write(sw_content)
                relative_path = sw_file.relative_to(output_dir)
                script_content = script_content.replace(sw_url_match.group(1), str(relative_path))
        return script_content

    async def prioritize_resources(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        resources = []
        for tag in soup.find_all(['link', 'script', 'img']):
            if tag.name == 'link' and tag.get('rel') == ['stylesheet']:
                resources.append({'type': 'css', 'url': tag.get('href'), 'priority': 'high'})
            elif tag.name == 'script':
                resources.append({'type': 'js', 'url': tag.get('src'), 'priority': 'medium'})
            elif tag.name == 'img':
                resources.append({'type': 'image', 'url': tag.get('src'), 'priority': 'low'})

        # Sort resources by priority
        priority_order = {'high': 0, 'medium': 1, 'low': 2}
        resources.sort(key=lambda x: priority_order[x['priority']])

        return resources

    async def process_all_resources(self, soup: BeautifulSoup, page_url: str, output_dir: Path) -> BeautifulSoup:
        prioritized_resources = await self.prioritize_resources(soup)
        
        async def process_resource(resource):
            if resource['type'] == 'css':
                return await self.process_css_file(resource['url'], output_dir, page_url)
            elif resource['type'] == 'js':
                return await self.process_js_file(resource['url'], output_dir, page_url)
            elif resource['type'] == 'image':
                return await self.process_image_file(resource['url'], output_dir)

        tasks = [process_resource(resource) for resource in prioritized_resources]
        processed_resources = await asyncio.gather(*tasks)

        for original, processed in zip(prioritized_resources, processed_resources):
            if processed:
                soup = self.update_resource_reference(soup, original, processed)

        return soup

    def update_resource_reference(self, soup: BeautifulSoup, original: Dict[str, Any], processed: Path) -> BeautifulSoup:
        if original['type'] == 'css':
            for tag in soup.find_all('link', rel='stylesheet', href=original['url']):
                tag['href'] = str(processed)
        elif original['type'] == 'js':
            for tag in soup.find_all('script', src=original['url']):
                tag['src'] = str(processed)
        elif original['type'] == 'image':
            for tag in soup.find_all('img', src=original['url']):
                tag['src'] = str(processed)
        return soup

    async def save_processed_html(self, soup: BeautifulSoup, output_path: Path):
        html_content = str(soup)
        minified_html = advanced_html_minify(html_content)
        async with aiofiles.open(output_path, 'w', encoding='utf-8') as f:
            await f.write(minified_html)
        logger.info(f"Saved processed HTML: {output_path}")

async def process_resources(session: aiohttp.ClientSession,
                            soup: BeautifulSoup,
                            directory_manager: DirectoryManager,
                            root_url: str,
                            output_dir: Path,
                            config: Dict[str, Any]) -> BeautifulSoup:
    resource_manager = ResourceManager(session, directory_manager, root_url, config)
    processed_soup = await resource_manager.process_all_resources(soup, root_url, output_dir)
    processed_soup = await resource_manager.process_fonts(processed_soup, root_url, output_dir)
    processed_soup = await resource_manager.handle_web_workers(processed_soup, root_url, output_dir)
    processed_soup = await resource_manager.handle_service_workers(processed_soup, root_url, output_dir)
    return processed_soup

# Example usage
if __name__ == "__main__":
    async def main():
        config = {
            'max_workers': 10,
            'max_concurrent_downloads': 20,
            'resource_timeout': 30,
            'compression': 'brotli',
            'use_cloud_storage': False,  # Disable cloud storage
            'redis_url': 'redis://localhost:6379'
        }

        async with aiohttp.ClientSession() as session:
            directory_manager = DirectoryManager(Path("./output"))
            root_url = "https://example.com"
            output_dir = Path("./output")

            html_content = """
            <html>
                <head>
                    <link rel="stylesheet" href="/styles/main.css">
                    <script src="/js/script.js"></script>
                    <link href="https://fonts.googleapis.com/css?family=Roboto" rel="stylesheet">
                </head>
                <body>
                    <img src="/images/logo.png">
                    <video src="/media/video.mp4"></video>
                    <script>
                        if ('serviceWorker' in navigator) {
                            navigator.serviceWorker.register('/sw.js');
                        }
                        const worker = new Worker('/js/worker.js');
                    </script>
                </body>
            </html>
            """

            soup = BeautifulSoup(html_content, 'lxml')
            processed_soup = await process_resources(session, soup, directory_manager, root_url, output_dir, config)

            print(processed_soup.prettify())

    asyncio.run(main())