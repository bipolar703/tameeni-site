import json
import logging
from pathlib import Path
from typing import List, Dict
from bs4 import BeautifulSoup, Tag
from directory_manager import DirectoryManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LinkCustomizer:
    def __init__(self, directory_manager: DirectoryManager):
        self.directory_manager = directory_manager
        self.customizable_links: List[Dict[str, str]] = []

    def prepare_links_for_customization(self, soup: BeautifulSoup) -> None:
        """Prepare links in the soup for customization."""
        for index, a_tag in enumerate(soup.find_all('a', href=True)):
            link_id = f"custom-link-{index}"
            original_url = a_tag['href']
            a_tag['data-custom-link'] = link_id
            self.customizable_links.append({
                'id': link_id,
                'original_url': original_url,
                'text': a_tag.text.strip()
            })
            a_tag['href'] = f"javascript:void(0);"  # Prevent navigation

        logger.info(f"Prepared {len(self.customizable_links)} links for customization")

    async def inject_link_customization_script(self, soup: BeautifulSoup) -> None:
        """Inject a script to handle customized link behavior."""
        script = soup.new_tag('script')
        script.string = '''
            document.addEventListener('click', function(e) {
                var target = e.target.closest('[data-custom-link]');
                if (target) {
                    e.preventDefault();
                    var linkId = target.getAttribute('data-custom-link');
                    // Here you can add logic to handle the click, e.g., send to parent frame
                    window.parent.postMessage({type: 'linkClicked', linkId: linkId}, '*');
                }
            });
        '''
        soup.body.append(script)
        logger.info("Injected link customization script")

    async def save_customizable_links(self, output_dir: Path) -> None:
        """Save the customizable links to a JSON file."""
        links_file = output_dir / 'customizable_links.json'
        try:
            await self.directory_manager.write_file(links_file, json.dumps(self.customizable_links, indent=2))
            logger.info(f"Saved customizable links to {links_file}")
        except Exception as e:
            logger.error(f"Error saving customizable links: {e}")

    async def process_links(self, soup: BeautifulSoup, output_dir: Path) -> BeautifulSoup:
        """Process all links in the soup, prepare them for customization, and save the data."""
        self.prepare_links_for_customization(soup)
        await self.inject_link_customization_script(soup)
        await self.save_customizable_links(output_dir)
        return soup

    async def update_customized_links(self, soup: BeautifulSoup, custom_links: Dict[str, str]) -> BeautifulSoup:
        """Update the soup with customized link URLs."""
        for a_tag in soup.find_all('a', attrs={'data-custom-link': True}):
            link_id = a_tag['data-custom-link']
            if link_id in custom_links:
                a_tag['href'] = custom_links[link_id]
                del a_tag['data-custom-link']  # Remove the data attribute

        logger.info(f"Updated {len(custom_links)} customized links")
        return soup