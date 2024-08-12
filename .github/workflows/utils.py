import re
import os
import logging
from urllib.parse import urlparse, urlunparse, urljoin
from typing import Optional, Dict, Any
from pathlib import Path
import hashlib
import aiofiles
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_logging(level: str = "INFO", filename: Optional[str] = None) -> None:
    """Sets up logging for the application.

    Args:
        level (str): The logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL).
        filename (Optional[str]): Optional log file name; if provided, logs will be written to this file.
    """
    logger = logging.getLogger()
    logger.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    if filename:
        # File handler
        fh = logging.FileHandler(filename)
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    logger.info("Logging is set up.")

def ensure_url_scheme(url: str) -> str:
    """Ensure the URL has a scheme (http or https). Defaults to https if no scheme is provided.

    Args:
        url (str): The input URL.

    Returns:
        str: The URL with the scheme ensured.
    """
    parsed_url = urlparse(url)
    if not parsed_url.scheme:
        return f'https://{url}'
    return url

def sanitize_filename(filename: str, max_length: int = 255) -> str:
    """Sanitize a filename by removing invalid characters and truncating it if too long.

    Args:
        filename (str): The input filename.
        max_length (int): Maximum allowed length for the filename.

    Returns:
        str: The sanitized filename.
    """
    # Remove invalid characters
    sanitized = re.sub(r'[<>:"/\\|?*]', '', filename)
    # Replace spaces with underscores
    sanitized = sanitized.replace(' ', '_')
    # Truncate to a maximum length
    return sanitized[:max_length]

def get_domain(url: str) -> Optional[str]:
    """Extract the domain from a URL.

    Args:
        url (str): The input URL.

    Returns:
        Optional[str]: The domain of the URL, or None if invalid.
    """
    if is_valid_url(url):
        parsed_url = urlparse(url)
        return parsed_url.netloc
    return None

def is_valid_url(url: str) -> bool:
    """Check if a given string is a valid URL.

    Args:
        url (str): The input URL.

    Returns:
        bool: True if the URL is valid, False otherwise.
    """
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False

def get_file_extension(url: str) -> str:
    """Get the file extension from a URL.

    Args:
        url (str): The input URL.

    Returns:
        str: The file extension, or an empty string if none is found.
    """
    parsed = urlparse(url)
    path = parsed.path
    return os.path.splitext(path)[1].lower() if '.' in path else ''

def normalize_url(url: str) -> str:
    """Normalize the URL by ensuring it's well-formed and standardized.

    Args:
        url (str): The input URL.

    Returns:
        str: The normalized URL.
    """
    parsed_url = urlparse(url)
    # Remove fragments and query strings for a cleaner URL
    normalized_url = urlunparse(parsed_url._replace(fragment='', query=''))
    return normalized_url.strip().rstrip('/')

def extract_path_from_url(url: str) -> Optional[str]:
    """Extract the path component of the URL.

    Args:
        url (str): The input URL.

    Returns:
        Optional[str]: The path part of the URL, or None if invalid.
    """
    if is_valid_url(url):
        parsed_url = urlparse(url)
        return parsed_url.path
    return None

def get_filename_from_url(url: str) -> Optional[str]:
    """Extract the filename from the URL path.

    Args:
        url (str): The input URL.

    Returns:
        Optional[str]: The filename or None if no filename is present.
    """
    path = extract_path_from_url(url)
    return os.path.basename(path) if path else None

def resolve_relative_url(base_url: str, relative_url: str) -> str:
    """Resolve a relative URL to an absolute URL based on a given base URL.

    Args:
        base_url (str): The base URL.
        relative_url (str): The relative URL to resolve.

    Returns:
        str: The absolute URL.
    """
    return urljoin(base_url, relative_url)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
async def download_file(session: aiohttp.ClientSession, url: str, file_path: Path) -> None:
    """Download a file from a URL and save it to the specified path.

    Args:
        session (aiohttp.ClientSession): The aiohttp client session.
        url (str): The URL of the file to download.
        file_path (Path): The path where the file should be saved.

    Raises:
        Exception: If the download fails after retries.
    """
    try:
        async with session.get(url) as response:
            if response.status == 200:
                content = await response.read()
                async with aiofiles.open(file_path, mode='wb') as f:
                    await f.write(content)
                logger.info(f"Downloaded: {url} -> {file_path}")
            else:
                raise Exception(f"Failed to download {url}: HTTP {response.status}")
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        raise

def get_file_hash(file_path: Path) -> str:
    """Calculate the SHA256 hash of a file.

    Args:
        file_path (Path): The path to the file.

    Returns:
        str: The hexadecimal representation of the file's SHA256 hash.
    """
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def create_directory_if_not_exists(directory: Path) -> None:
    """Create a directory if it doesn't exist.

    Args:
        directory (Path): The directory path to create.
    """
    directory.mkdir(parents=True, exist_ok=True)

def get_mime_type(file_path: Path) -> str:
    """Get the MIME type of a file based on its extension.

    Args:
        file_path (Path): The path to the file.

    Returns:
        str: The MIME type of the file.
    """
    mime_types = {
        '.html': 'text/html',
        '.css': 'text/css',
        '.js': 'application/javascript',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.png': 'image/png',
        '.gif': 'image/gif',
        '.svg': 'image/svg+xml',
        '.json': 'application/json',
        '.xml': 'application/xml',
        '.pdf': 'application/pdf',
        '.txt': 'text/plain',
    }
    return mime_types.get(file_path.suffix.lower(), 'application/octet-stream')

def is_binary_file(file_path: Path) -> bool:
    """Check if a file is binary.

    Args:
        file_path (Path): The path to the file.

    Returns:
        bool: True if the file is binary, False otherwise.
    """
    try:
        with open(file_path, 'tr') as check_file:
            check_file.read()
            return False
    except:
        return True

async def save_json_file(data: Dict[str, Any], file_path: Path) -> None:
    """Save a dictionary as a JSON file.

    Args:
        data (Dict[str, Any]): The dictionary to save.
        file_path (Path): The path where the JSON file should be saved.
    """
    async with aiofiles.open(file_path, mode='w', encoding='utf-8') as f:
        await f.write(json.dumps(data, indent=2))
    logger.info(f"Saved JSON file: {file_path}")

# Example usage
if __name__ == "__main__":
    setup_logging(level="DEBUG", filename="app.log")
    test_url = "example.com/path/to/resource"
    print("Ensured URL:", ensure_url_scheme(test_url))
    print("Sanitized Filename:", sanitize_filename("Invalid/File:Name*Test.txt"))
    print("Domain:", get_domain(test_url))
    print("Is Valid URL:", is_valid_url(test_url))
    print("File Extension:", get_file_extension("http://example.com/file.txt"))
    print("Normalized URL:", normalize_url("http://example.com/?query=1#fragment"))
    print("Extracted Path:", extract_path_from_url("http://example.com/path/to/resource"))
    print("Filename from URL:", get_filename_from_url("http://example.com/path/to/resource.txt"))
    print("Resolved URL:", resolve_relative_url("http://example.com/path/", "resource.txt"))