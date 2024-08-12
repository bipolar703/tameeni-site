import asyncio
import argparse
import logging
from pathlib import Path
from urllib.parse import urlparse
import aiofiles
import aiohttp
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import json
import re

from config import ConfigManager, get_config
from directory_manager import DirectoryManager
from resource_acquisition_manager import ResourceManager
from js_processor import JSProcessor
from css_resource_acquisition_manager import CSSResourceManager
from utils import ensure_url_scheme, setup_logging, normalize_url
from link_customizer import LinkCustomizer

# Configure logging
setup_logging(level="INFO", filename="website_cloner.log")
logger = logging.getLogger(__name__)

class WebsiteCloner:
    def __init__(self, url: str, output_dir: Path, config: dict):
        self.url = ensure_url_scheme(url)
        self.output_dir = output_dir
        self.config = config
        self.directory_manager = DirectoryManager(self.output_dir)
        self.link_customizer = LinkCustomizer(self.directory_manager)

    async def clone(self):
        async with aiohttp.ClientSession() as session:
            resource_manager = ResourceManager(session, self.directory_manager, self.url)
            js_processor = JSProcessor(session, self.directory_manager, self.url)
            css_manager = CSSResourceManager(session, self.directory_manager, self.url)

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=self.config['javascript_settings']['use_headless_browser'])
                context = await browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent=self.config['network_settings']['user_agent']
                )

                page = await context.new_page()
                await page.route('**/*', lambda route: self.intercept_request(route, resource_manager))

                try:
                    response = await page.goto(self.url, wait_until='networkidle', timeout=self.config['network_settings']['page_load_timeout'])
                    if not response.ok:
                        raise Exception(f"Failed to load page: {response.status}")

                    # Wait for dynamic content to load
                    await self.wait_for_dynamic_content(page)

                    # Process the page content
                    content = await page.content()
                    soup = BeautifulSoup(content, 'lxml')

                    # Process resources
                    await resource_manager.process_resources(soup, self.url, self.output_dir)
                    await js_processor.process_javascript(soup, self.url)
                    await css_manager.process_css_resources(soup, self.url, self.output_dir)
                    await self.link_customizer.process_links(soup, self.output_dir)

                    # Handle AJAX requests
                    ajax_data = await self.capture_ajax_requests(page)
                    await self.save_ajax_data(ajax_data)

                    # Save the processed HTML
                    await self.save_processed_html(soup)

                    logger.info(f"Successfully cloned page: {self.url}")
                except Exception as e:
                    logger.error(f"Error cloning page {self.url}: {str(e)}")
                finally:
                    await browser.close()

    async def intercept_request(self, route, resource_manager):
        if route.request.resource_type in ['stylesheet', 'script', 'image', 'font']:
            await resource_manager.process_resource(route.request)
        await route.continue_()

    async def wait_for_dynamic_content(self, page):
        # Wait for network connections to settle
        await page.wait_for_load_state('networkidle')

        # Scroll the page to trigger lazy-loaded content
        await page.evaluate('''async () => {
            await new Promise((resolve) => {
                let totalHeight = 0;
                let distance = 100;
                let timer = setInterval(() => {
                    let scrollHeight = document.body.scrollHeight;
                    window.scrollBy(0, distance);
                    totalHeight += distance;
                    if(totalHeight >= scrollHeight){
                        clearInterval(timer);
                        resolve();
                    }
                }, 100);
            });
        }''')

        # Wait for any remaining dynamic content
        await page.wait_for_timeout(self.config['javascript_settings']['wait_for_js_timeout'])

    async def capture_ajax_requests(self, page):
        ajax_requests = await page.evaluate('''() => {
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
            return ajaxRequests;
        }''')
        return ajax_requests

    async def save_ajax_data(self, ajax_data):
        ajax_file = self.output_dir / 'ajax_data.json'
        async with aiofiles.open(ajax_file, 'w') as f:
            await f.write(json.dumps(ajax_data, indent=2))
        logger.info(f"Saved AJAX data: {ajax_file}")

    async def save_processed_html(self, soup):
        file_path = self.output_dir / 'index.html'
        async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
            await f.write(str(soup))
        logger.info(f"Saved processed HTML: {file_path}")

async def main():
    parser = argparse.ArgumentParser(description="Clone a single webpage with all its resources.")
    parser.add_argument("url", help="URL of the webpage to clone")
    parser.add_argument("output_dir", help="Directory to save the cloned webpage")
    args = parser.parse_args()

    config = get_config()
    cloner = WebsiteCloner(args.url, Path(args.output_dir), config)
    await cloner.clone()

if __name__ == "__main__":
    asyncio.run(main())