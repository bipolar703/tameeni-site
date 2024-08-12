import asyncio
import logging
from pathlib import Path
from typing import Set
from urllib.parse import urljoin, urlparse
import aiofiles
import aiohttp
from bs4 import BeautifulSoup, Tag
from tenacity import retry, stop_after_attempt, wait_exponential

from directory_manager import DirectoryManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PageResourceCopier:
    def __init__(self, session: aiohttp.ClientSession, directory_manager: DirectoryManager, root_url: str):
        self.session = session
        self.directory_manager = directory_manager
        self.root_url = root_url
        self.processed_resources: Set[str] = set()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def download_resource(self, url: str, file_path: Path) -> None:
        try:
            async with self.session.get(url, timeout=10) as response:
                if response.status == 200:
                    content = await response.read()
                    await self.directory_manager.ensure_parent_directory(file_path)
                    async with aiofiles.open(file_path, mode='wb') as f:
                        await f.write(content)
                    logger.info(f"Downloaded: {url} -> {file_path}")
                else:
                    logger.warning(f"Failed to download {url}: HTTP {response.status}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Error downloading {url}: {e}")

    async def process_resource(self, tag: Tag, attr: str, url: str, file_depth: int) -> None:
        if not url.startswith(('http://', 'https://')):
            url = urljoin(self.root_url, url)

        if url in self.processed_resources:
            return

        self.processed_resources.add(url)

        file_path = await self.directory_manager.get_file_path(url)
        await self.download_resource(url, file_path)

        relative_path = Path(urlparse(url).path.lstrip('/'))
        depth_prefix = '../' * file_depth
        tag[attr] = f"{depth_prefix}{relative_path}"

    async def process_page_resources(self, soup: BeautifulSoup, file_depth: int) -> BeautifulSoup:
        tasks = []

        for tag in soup.find_all(['img', 'script', 'link']):
            url_attr = None
            if tag.name == 'img' and tag.get('src'):
                url_attr = 'src'
            elif tag.name == 'script' and tag.get('src'):
                url_attr = 'src'
            elif tag.name == 'link' and tag.get('href') and 'stylesheet' in tag.get('rel', []):
                url_attr = 'href'

            if url_attr:
                tasks.append(self.process_resource(tag, url_attr, tag[url_attr], file_depth))

        await asyncio.gather(*tasks)
        return soup

async def get_page_resources(session: aiohttp.ClientSession,
                             soup: BeautifulSoup,
                             directory_manager: DirectoryManager,
                             root_url: str,
                             file_depth: int) -> BeautifulSoup:
    copier = PageResourceCopier(session, directory_manager, root_url)
    return await copier.process_page_resources(soup, file_depth)

async def main():
    async with aiohttp.ClientSession() as session:
        directory_manager = DirectoryManager(Path("./output"))
        root_url = "https://example.com"

        sample_html = """
        <html>
            <head>
                <link rel="stylesheet" href="/styles/main.css">
                <script src="/js/script.js"></script>
            </head>
            <body>
                <img src="/images/logo.png">
            </body>
        </html>
        """

        soup = BeautifulSoup(sample_html, 'html.parser')
        updated_soup = await get_page_resources(session, soup, directory_manager, root_url, 0)

        print(updated_soup.prettify())

if __name__ == "__main__":
    asyncio.run(main())
