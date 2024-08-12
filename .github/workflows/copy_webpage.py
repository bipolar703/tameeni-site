import asyncio
import datetime
import logging
from pathlib import Path
from typing import Dict, Any, List, Set
from urllib.parse import urlparse, urljoin
import aiohttp
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
import httpx
import json
import aiofiles
import asyncpg
from css_html_js_minify import html_minify, js_minify, css_minify

from directory_manager import DirectoryManager
from resource_acquisition_manager import ResourceManager
from js_processor import JSProcessor
from utils import ensure_url_scheme, setup_logging, normalize_url, get_content_hash

# Configure logging
setup_logging(level="DEBUG", filename="webpage_cloner.log")
logger = logging.getLogger(__name__)

class AdvancedSinglePageCloner:
    def __init__(self, url: str, output_dir: Path, config: Dict[str, Any]):
        self.url = ensure_url_scheme(url)
        self.output_dir = output_dir
        self.config = config
        self.directory_manager = DirectoryManager(self.output_dir)
        self.resource_manager = ResourceManager(self.directory_manager, self.url)
        self.js_processor = JSProcessor(self.directory_manager, self.url)
        self.visited_urls: Set[str] = set()
        self.dynamic_elements: List[Dict[str, Any]] = []
        self.ajax_requests: List[Dict[str, Any]] = []
        self.websocket_messages: List[Dict[str, Any]] = []
        self.db_pool = None

    async def setup(self):
        self.db_pool = await asyncpg.create_pool(self.config['database_url'])

    async def clone(self):
        await self.setup()
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.config['headless'])
            context = await self._create_browser_context(browser)
            page = await context.new_page()
            await self._setup_page_interception(page)

            try:
                await self._navigate_and_wait(page)
                await self._process_page_content(page)
                await self._handle_dynamic_elements(page)
                await self._capture_ajax_and_websockets(page)
                await self._save_final_state(page)
            finally:
                await browser.close()
                await self.db_pool.close()

    async def _create_browser_context(self, browser: Browser) -> BrowserContext:
        return await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent=self.config['user_agent'],
            ignore_https_errors=True,
            java_script_enabled=True,
            bypass_csp=True
        )

    async def _setup_page_interception(self, page: Page):
        await page.route('**/*', self._intercept_request)
        page.on('console', lambda msg: logger.debug(f"Console {msg.type}: {msg.text}"))
        page.on('pageerror', lambda err: logger.error(f"Page error: {err}"))
        page.on('request', self._on_request)
        page.on('response', self._on_response)
        page.on('websocket', self._on_websocket)

    async def _navigate_and_wait(self, page: Page):
        response = await page.goto(self.url, wait_until='networkidle', timeout=60000)
        if not response.ok:
            raise Exception(f"Failed to load page: {response.status}")
        await self._wait_for_page_load(page)

    async def _wait_for_page_load(self, page: Page):
        await page.wait_for_load_state('networkidle')
        await page.evaluate('document.readyState === "complete"')
        await self._scroll_page(page)

    async def _scroll_page(self, page: Page):
        await page.evaluate('''
            async () => {
                await new Promise((resolve) => {
                    let totalHeight = 0;
                    const distance = 100;
                    const timer = setInterval(() => {
                        const scrollHeight = document.body.scrollHeight;
                        window.scrollBy(0, distance);
                        totalHeight += distance;
                        if(totalHeight >= scrollHeight){
                            clearInterval(timer);
                            resolve();
                        }
                    }, 100);
                });
            }
        ''')

    async def _process_page_content(self, page: Page):
        content = await page.content()
        soup = BeautifulSoup(content, 'lxml')
        await self._process_resources(soup)
        await self._process_styles(soup)
        await self._process_scripts(soup)
        await self._save_processed_html(soup)

    async def _process_resources(self, soup: BeautifulSoup):
        tasks = [self.resource_manager.download_and_replace(tag, attr) 
                 for tag in soup.find_all(['img', 'video', 'audio', 'source', 'link']) 
                 for attr in ['src', 'href'] if tag.get(attr)]
        await asyncio.gather(*tasks)

    async def _process_styles(self, soup: BeautifulSoup):
        tasks = [self._optimize_and_minify_tag(tag, "style") for tag in soup.find_all('style')]
        inline_styles = [self._optimize_attribute(tag, "style") for tag in soup.find_all(style=True)]
        await asyncio.gather(*tasks, *inline_styles)

    async def _optimize_and_minify_tag(self, tag, attribute):
        tag.string = await self._optimize_css(tag.string)
    
    async def _optimize_attribute(self, tag, attribute):
        tag[attribute] = await self._optimize_css(tag[attribute])

    async def _process_scripts(self, soup: BeautifulSoup):
        tasks = [self.js_processor.process_script(tag.string) 
                 if tag.string else self.js_processor.download_and_process_script(tag) 
                 for tag in soup.find_all('script') if tag.string or tag.get('src')]
        await asyncio.gather(*tasks)

    async def _optimize_css(self, css_content: str) -> str:
        optimized = css_minify(css_content)
        return optimized

    async def _save_processed_html(self, soup: BeautifulSoup):
        html_content = str(soup)
        minified_html = html_minify(html_content)
        file_path = self.output_dir / 'index.html'
        async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
            await f.write(minified_html)
        logger.info(f"Saved processed HTML: {file_path}")

    async def _handle_dynamic_elements(self, page: Page):
        self.dynamic_elements = await page.evaluate('''
            () => {
                const dynamicElements = [];
                const observer = new MutationObserver(mutations => {
                    for (let mutation of mutations) {
                        if (mutation.type === 'childList') {
                            for (let node of mutation.addedNodes) {
                                if (node.nodeType === Node.ELEMENT_NODE) {
                                    dynamicElements.push({
                                        tagName: node.tagName,
                                        innerHTML: node.innerHTML,
                                        attributes: Array.from(node.attributes).map(attr => ({
                                            name: attr.name,
                                            value: attr.value
                                        }))
                                    });
                                }
                            }
                        }
                    }
                });
                observer.observe(document.body, { childList: true, subtree: true });
                return new Promise(resolve => setTimeout(() => {
                    observer.disconnect();
                    resolve(dynamicElements);
                }, 5000));
            }
        ''')
        await self._save_dynamic_elements()

    async def _save_dynamic_elements(self):
        file_path = self.output_dir / 'dynamic_elements.json'
        async with aiofiles.open(file_path, 'w') as f:
            await f.write(json.dumps(self.dynamic_elements, indent=2))
        logger.info(f"Saved dynamic elements: {file_path}")

    async def _capture_ajax_and_websockets(self, page: Page):
        self.ajax_requests = await page.evaluate('''
            () => {
                const ajaxRequests = [];
                const open = XMLHttpRequest.prototype.open;
                XMLHttpRequest.prototype.open = function() {
                    this.addEventListener('load', function() {
                        ajaxRequests.push({
                            url: this.responseURL,
                            method: this._method,
                            status: this.status,
                            response: this.responseText
                        });
                    });
                    open.apply(this, arguments);
                };
                return new Promise(resolve => setTimeout(() => resolve(ajaxRequests), 5000));
            }
        ''')
        await self._save_ajax_requests()

    async def _save_ajax_requests(self):
        file_path = self.output_dir / 'ajax_requests.json'
        async with aiofiles.open(file_path, 'w') as f:
            await f.write(json.dumps(self.ajax_requests, indent=2))
        logger.info(f"Saved AJAX requests: {file_path}")

    async def _save_final_state(self, page: Page):
        await self._save_screenshot(page)
        await self._save_pdf(page)
        await self._save_metadata()

    async def _save_screenshot(self, page: Page):
        screenshot_path = self.output_dir / 'screenshot.png'
        await page.screenshot(path=str(screenshot_path), full_page=True)
        logger.info(f"Saved screenshot: {screenshot_path}")

    async def _save_pdf(self, page: Page):
        pdf_path = self.output_dir / 'page.pdf'
        await page.pdf(path=str(pdf_path))
        logger.info(f"Saved PDF: {pdf_path}")

    async def _save_metadata(self):
        metadata = {
            'url': self.url,
            'timestamp': datetime.datetime.now().isoformat(),
            'user_agent': self.config['user_agent'],
            'dynamic_elements_count': len(self.dynamic_elements),
            'ajax_requests_count': len(self.ajax_requests),
            'websocket_messages_count': len(self.websocket_messages)
        }
        metadata_path = self.output_dir / 'metadata.json'
        async with aiofiles.open(metadata_path, 'w') as f:
            await f.write(json.dumps(metadata, indent=2))
        logger.info(f"Saved metadata: {metadata_path}")

    async def _intercept_request(self, route):
        if route.request.resource_type in ['stylesheet', 'script', 'image', 'font']:
            await self.resource_manager.process_resource(route.request)
        await route.continue_()

    async def _on_request(self, request):
        if request.resource_type == 'xhr':
            self.ajax_requests.append({
                'url': request.url,
                'method': request.method,
                'headers': request.headers
            })

    async def _on_response(self, response):
        if response.request.resource_type == 'xhr':
            content = await response.text()
            self.ajax_requests[-1].update({
                'status': response.status,
                'content': content
            })

    async def _on_websocket(self, ws):
        self.websocket_messages.append({
            'url': ws.url,
            'messages': []
        })
        ws.on('framesent', lambda event: self.websocket_messages[-1]['messages'].append({
            'type': 'sent',
            'payload': event.payload
        }))
        ws.on('framereceived', lambda event: self.websocket_messages[-1]['messages'].append({
            'type': 'received',
            'payload': event.payload
        }))

async def clone_webpage(url: str, output_dir: Path, config: Dict[str, Any]):
    cloner = AdvancedSinglePageCloner(url, output_dir, config)
    await cloner.clone()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python copy_webpage.py <url> <output_directory>")
        sys.exit(1)

    url = sys.argv[1]
    output_dir = Path(sys.argv[2])
    config = {
        'headless': True,
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'database_url': 'postgresql://user:password@localhost/webpage_cloner'
    }

    asyncio.run(clone_webpage(url, output_dir, config))