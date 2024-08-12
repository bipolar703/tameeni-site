import asyncio
import logging
from pathlib import Path
from typing import Union, List, Dict, Any, Optional
from urllib.parse import urlparse
import aiofiles
import aiofiles.os as aios
import brotli
import zstandard as zstd
import lzma
import gzip
import hashlib
import shutil
import tempfile
from tenacity import retry, stop_after_attempt, wait_exponential
import boto3
from botocore.exceptions import ClientError
from azure.storage.blob.aio import BlobServiceClient
#from google.cloud import storage # pylint: disable=no-name-in-module
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import redis
from cachetools import TTLCache
from concurrent.futures import ThreadPoolExecutor
import xxhash
from diskcache import Cache
import aioredis
import asyncpg
from dask.distributed import Client as DaskClient

from utils import sanitize_filename, is_binary_file, get_mime_type, get_file_hash

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, directory_manager: 'DirectoryManager'):
        self.directory_manager = directory_manager

    async def on_modified(self, event):
        if event.is_directory:
            return

        file_path = Path(event.src_path)
        if file_path.is_file():
            await self.directory_manager.handle_file_change(file_path)

    async def on_created(self, event):
        if event.is_directory:
            return

        file_path = Path(event.src_path)
        if file_path.is_file():
            await self.directory_manager.handle_file_change(file_path)

    async def on_deleted(self, event):
        if event.is_directory:
            return

        file_path = Path(event.src_path)
        if file_path.is_file():
            await self.directory_manager.handle_file_deletion(file_path)

class DirectoryManager:
    def __init__(self, root_directory: Path, config: Dict[str, Any]):
        self.root_directory = root_directory
        self.config = config
        self.created_directories = TTLCache(maxsize=1000, ttl=3600)
        self.file_cache = Cache(directory=str(root_directory / '.cache'), size_limit=int(1e9))  # 1GB cache
        self.lock = asyncio.Lock()
        self.executor = ThreadPoolExecutor(max_workers=config.get('max_workers', 4))
        self.observer = Observer()
        self.redis_client = redis.Redis.from_url(config['redis_url'])
        self.db_pool = None
        self.dask_client = None

        if config['use_cloud_storage']:
            self.s3_client = boto3.client('s3')
            self.azure_client = BlobServiceClient.from_connection_string(config['azure_connection_string'])
            self.gcs_client = storage.Client()

        self.start_file_watcher()
        self.init_compression()

    async def setup(self):
        self.db_pool = await asyncpg.create_pool(self.config['database_url'])
        self.dask_client = await DaskClient(self.config['dask_scheduler'])

    def init_compression(self):
        self.compression_level = self.config.get('compression_level', 9)
        self.compressors = {
            'br': brotli.Compressor(quality=self.compression_level),
            'zstd': zstd.ZstdCompressor(level=self.compression_level),
            'lzma': lzma.LZMACompressor(preset=self.compression_level),
            'gzip': gzip.GzipFile(fileobj=tempfile.TemporaryFile(), mode='wb', compresslevel=self.compression_level)
        }

    def start_file_watcher(self):
        event_handler = FileChangeHandler(self)
        self.observer.schedule(event_handler, str(self.root_directory), recursive=True)
        self.observer.start()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def create_directory(self, path: Path) -> None:
        try:
            await aios.makedirs(path, exist_ok=True)
            logger.info(f"Created directory: {path}")
        except OSError as e:
            logger.error(f"Error creating directory {path}: {e}")
            raise

    async def get_or_create_directory(self, web_path: str) -> Path:
        async with self.lock:
            if web_path in self.created_directories:
                return self.created_directories[web_path]

            relative_path = self._get_relative_path(web_path)
            full_path = self.root_directory / relative_path

            if not full_path.exists():
                await self.create_directory(full_path)

            self.created_directories[web_path] = full_path
            return full_path

    async def get_file_path(self, web_path: str) -> Path:
        relative_path = self._get_relative_path(web_path)
        if not relative_path.suffix:
            relative_path = relative_path / 'index.html'
        return self.root_directory / relative_path

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def ensure_parent_directory(self, file_path: Path) -> None:
        await self.create_directory(file_path.parent)

    async def clean_empty_directories(self) -> None:
        for dirpath in sorted(self.root_directory.rglob('*'), key=lambda x: len(x.parts), reverse=True):
            if dirpath.is_dir() and await self.is_directory_empty(dirpath):
                try:
                    await aios.rmdir(dirpath)
                    logger.info(f"Removed empty directory: {dirpath}")
                except OSError:
                    pass

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def move_file(self, source: Path, destination: Path) -> None:
        await self.ensure_parent_directory(destination)
        try:
            await aios.rename(source, destination)
            logger.info(f"Moved file: {source} -> {destination}")
        except OSError as e:
            logger.error(f"Error moving file {source} to {destination}: {e}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def copy_file(self, source: Path, destination: Path) -> None:
        await self.ensure_parent_directory(destination)
        try:
            await aiofiles.copyfile(source, destination)
            logger.info(f"Copied file: {source} -> {destination}")
        except OSError as e:
            logger.error(f"Error copying file {source} to {destination}: {e}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def delete_file(self, file_path: Path) -> None:
        try:
            await aios.unlink(file_path)
            logger.info(f"Deleted file: {file_path}")
        except OSError as e:
            logger.error(f"Error deleting file {file_path}: {e}")
            raise

    async def list_directory(self, directory: Path) -> List[Path]:
        try:
            return [item async for item in aios.scandir(directory)]
        except OSError as e:
            logger.error(f"Error listing directory {directory}: {e}")
            return []

    async def get_file_size(self, file_path: Path) -> int:
        try:
            return (await aios.stat(file_path)).st_size
        except OSError as e:
            logger.error(f"Error getting file size for {file_path}: {e}")
            return 0

    async def is_directory_empty(self, directory: Path) -> bool:
        try:
            return not any(await self.list_directory(directory))
        except OSError as e:
            logger.error(f"Error checking if directory {directory} is empty: {e}")
            return False

    def _get_relative_path(self, web_path: str) -> Path:
        url_components = urlparse(web_path)
        relative_path = Path(sanitize_filename(url_components.path.lstrip('/')))
        return relative_path.parent if relative_path.name and not relative_path.suffix else relative_path

    async def compress_file(self, file_path: Path, compression_type: str = 'zstd') -> Path:
        compressed_path = file_path.with_suffix(f"{file_path.suffix}.{compression_type}")
        try:
            async with aiofiles.open(file_path, 'rb') as f_in:
                content = await f_in.read()

            if compression_type == 'zstd':
                compressed_data = self.compressors['zstd'].compress(content)
            elif compression_type == 'br':
                compressed_data = self.compressors['br'].process(content) + self.compressors['br'].finish()
            elif compression_type == 'lzma':
                compressed_data = self.compressors['lzma'].compress(content) + self.compressors['lzma'].flush()
            elif compression_type == 'gzip':
                self.compressors['gzip'].write(content)
                self.compressors['gzip'].fileobj.seek(0)
                compressed_data = self.compressors['gzip'].fileobj.read()
            else:
                raise ValueError(f"Unsupported compression type: {compression_type}")

            async with aiofiles.open(compressed_path, 'wb') as f_out:
                await f_out.write(compressed_data)

            logger.info(f"Compressed file: {file_path} -> {compressed_path}")
            return compressed_path
        except Exception as e:
            logger.error(f"Error compressing file {file_path}: {e}")
            raise

    async def decompress_file(self, file_path: Path) -> Path:
        decompressed_path = file_path.with_suffix('')
        try:
            async with aiofiles.open(file_path, 'rb') as f_in:
                compressed_data = await f_in.read()

            if file_path.suffix == '.zst':
                decompressor = zstd.ZstdDecompressor()
                decompressed_data = decompressor.decompress(compressed_data)
            elif file_path.suffix == '.br':
                decompressed_data = brotli.decompress(compressed_data)
            elif file_path.suffix == '.xz':
                decompressed_data = lzma.decompress(compressed_data)
            elif file_path.suffix == '.gz':
                decompressed_data = gzip.decompress(compressed_data)
            else:
                raise ValueError(f"Unsupported compression format: {file_path.suffix}")

            async with aiofiles.open(decompressed_path, 'wb') as f_out:
                await f_out.write(decompressed_data)

            logger.info(f"Decompressed file: {file_path} -> {decompressed_path}")
            return decompressed_path
        except Exception as e:
            logger.error(f"Error decompressing file {file_path}: {e}")
            raise

    async def upload_to_cloud(self, file_path: Path, cloud_path: str) -> None:
        if not self.config['use_cloud_storage']:
            logger.warning("Cloud upload requested but cloud storage is not enabled.")
            return

        try:
            if self.config['cloud_storage'] == 'aws':
                await self.upload_to_s3(file_path, cloud_path)
            elif self.config['cloud_storage'] == 'azure':
                await self.upload_to_azure(file_path, cloud_path)
            elif self.config['cloud_storage'] == 'gcp':
                await self.upload_to_gcs(file_path, cloud_path)
            else:
                raise ValueError(f"Unsupported cloud storage: {self.config['cloud_storage']}")
        except Exception as e:
            logger.error(f"Error uploading file to cloud storage: {e}")
            raise

    async def upload_to_s3(self, file_path: Path, s3_key: str) -> None:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            self.executor,
            self.s3_client.upload_file,
            str(file_path),
            self.config['s3_bucket'],
            s3_key
        )
        logger.info(f"Uploaded file to S3: {file_path} -> s3://{self.config['s3_bucket']}/{s3_key}")

    async def upload_to_azure(self, file_path: Path, blob_name: str) -> None:
        blob_client = self.azure_client.get_blob_client(container=self.config['azure_container'], blob=blob_name)
        async with aiofiles.open(file_path, 'rb') as data:
            await blob_client.upload_blob(data)
        logger.info(f"Uploaded file to Azure: {file_path} -> {blob_name}")

    async def upload_to_gcs(self, file_path: Path, gcs_path: str) -> None:
        bucket = self.gcs_client.bucket(self.config['gcs_bucket'])
        blob = bucket.blob(gcs_path)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self.executor, blob.upload_from_filename, str(file_path))
        logger.info(f"Uploaded file to GCS: {file_path} -> gs://{self.config['gcs_bucket']}/{gcs_path}")

    async def download_from_cloud(self, cloud_path: str, file_path: Path) -> None:
        if not self.config['use_cloud_storage']:
            logger.warning("Cloud download requested but cloud storage is not enabled.")
            return

        try:
            if self.config['cloud_storage'] == 'aws':
                await self.download_from_s3(cloud_path, file_path)
            elif self.config['cloud_storage'] == 'azure':
                await self.download_from_azure(cloud_path, file_path)
            elif self.config['cloud_storage'] == 'gcp':
                await self.download_from_gcs(cloud_path, file_path)
            else:
                raise ValueError(f"Unsupported cloud storage: {self.config['cloud_storage']}")
        except Exception as e:
            logger.error(f"Error downloading file from cloud storage: {e}")
            raise

    async def download_from_s3(self, s3_key: str, file_path: Path) -> None:
        await self.ensure_parent_directory(file_path)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            self.executor,
            self.s3_client.download_file,
            self.config['s3_bucket'],
            s3_key,
            str(file_path)
        )
        logger.info(f"Downloaded file from S3: s3://{self.config['s3_bucket']}/{s3_key} -> {file_path}")

    async def download_from_azure(self, blob_name: str, file_path: Path) -> None:
        await self.ensure_parent_directory(file_path)
        blob_client = self.azure_client.get_blob_client(container=self.config['azure_container'], blob=blob_name)
        async with aiofiles.open(file_path, 'wb') as data:
            stream = await blob_client.download_blob()
            await data.write(await stream.readall())
        logger.info(f"Downloaded file from Azure: {blob_name} -> {file_path}")

    async def download_from_gcs(self, gcs_path: str, file_path: Path) -> None:
        await self.ensure_parent_directory(file_path)
        bucket = self.gcs_client.bucket(self.config['gcs_bucket'])
        blob = bucket.blob(gcs_path)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self.executor, blob.download_to_filename, str(file_path))
        logger.info(f"Downloaded file from GCS: gs://{self.config['gcs_bucket']}/{gcs_path} -> {file_path}")

    async def handle_file_change(self, file_path: Path):
        # Implement your logic for handling file changes here
        # For example, you might want to re-compress the file, update a database record, etc.
        logger.info(f"File changed: {file_path}")

    async def handle_file_deletion(self, file_path: Path):
        # Implement your logic for handling file deletions here
        # For example, you might want to delete the file from cloud storage, remove database entries, etc.
        logger.info(f"File deleted: {file_path}")

    async def close(self):
        if self.observer:
            self.observer.stop()
            self.observer.join()

        if self.db_pool:
            await self.db_pool.close()

        if self.dask_client:
            await self.dask_client.close()