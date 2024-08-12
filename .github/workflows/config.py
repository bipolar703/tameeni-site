import os
import json
import yaml
import toml
from pathlib import Path
from typing import Any, Dict, List, Union
from dataclasses import dataclass, field, asdict
from marshmallow import Schema, fields, ValidationError
from dotenv import load_dotenv
from pydantic import BaseModel, Field, validator
from cryptography.fernet import Fernet
from appdirs import user_config_dir
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger: logging.Logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

@dataclass
class ResourceSettings:
    download_images: bool = True
    download_css: bool = True
    download_js: bool = True
    download_fonts: bool = True
    download_videos: bool = False
    download_documents: bool = False
    max_file_size: int = Field(default=50 * 1024 * 1024, description="Maximum file size in bytes (default 50MB)")

@dataclass
class CrawlerSettings:
    max_depth: int = 3
    max_pages: int = 100
    respect_robots_txt: bool = False
    crawl_delay: float = 0.5

@dataclass
class JavaScriptSettings:
    use_headless_browser: bool = True
    wait_for_js_timeout: int = 5000
    scroll_for_lazy_loading: bool = True
    handle_dynamic_content: bool = True

@dataclass
class NetworkSettings:
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    page_load_timeout: int = 60000
    resource_download_timeout: int = 30
    max_concurrent_requests: int = 10
    retry_attempts: int = 3
    retry_delay: float = 1.0

@dataclass
class StorageSettings:
    output_dir: Path = Path("./website_copy")
    use_compression: bool = True
    compression_level: int = 6
    use_encryption: bool = False
    encryption_key: str = field(default_factory=lambda: Fernet.generate_key().decode())

@dataclass
class AdvancedSettings:
    handle_single_page_apps: bool = True
    handle_infinite_scroll: bool = True
    extract_metadata: bool = True
    generate_sitemap: bool = True
    preserve_url_structure: bool = True
    handle_ajax_requests: bool = True

class Config(BaseModel):
    app_name: str = "WebsiteCopier"
    app_version: str = "1.0.0"
    debug_mode: bool = Field(default=False, env="DEBUG_MODE")
    resource_settings: ResourceSettings = ResourceSettings()
    crawler_settings: CrawlerSettings = CrawlerSettings()
    javascript_settings: JavaScriptSettings = JavaScriptSettings()
    network_settings: NetworkSettings = NetworkSettings()
    storage_settings: StorageSettings = StorageSettings()
    advanced_settings: AdvancedSettings = AdvancedSettings()

    @validator("storage_settings")
    def validate_output_dir(cls, v: StorageSettings) -> StorageSettings:
        v.output_dir.mkdir(parents=True, exist_ok=True)
        return v

class ConfigManager:
    def __init__(self):
        self.config: Config = Config()
        self.config_dir: Path = Path(user_config_dir(self.config.app_name))
        self.config_file: Path = self.config_dir / "config.yaml"
        self.load_config()

    def load_config(self):
        if self.config_file.exists():
            with open(self.config_file, "r") as f:
                config_data: Dict[str, Any] = yaml.safe_load(f)
            self.config = Config(**config_data)
        else:
            self.save_config()

    def save_config(self):
        self.config_dir.mkdir(parents=True, exist_ok=True)
        with open(self.config_file, "w") as f:
            yaml.dump(asdict(self.config), f)

    def update_config(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
        self.save_config()

    def get_encrypted_value(self, key: str) -> str:
        if not self.config.storage_settings.use_encryption:
            return getattr(self.config, key)
        
        fernet = Fernet(self.config.storage_settings.encryption_key.encode())
        encrypted_value = getattr(self.config, key).encode()
        return fernet.decrypt(encrypted_value).decode()

    def set_encrypted_value(self, key: str, value: str):
        if not self.config.storage_settings.use_encryption:
            setattr(self.config, key, value)
            return
        
        fernet = Fernet(self.config.storage_settings.encryption_key.encode())
        encrypted_value = fernet.encrypt(value.encode()).decode()
        setattr(self.config, key, encrypted_value)

    def export_config(self, file_path: Path, format: str = "yaml"):
        config_data = asdict(self.config)
        
        if format == "json":
            with open(file_path, "w") as f:
                json.dump(config_data, f, indent=2)
        elif format == "toml":
            with open(file_path, "w") as f:
                toml.dump(config_data, f)
        else:  # default to yaml
            with open(file_path, "w") as f:
                yaml.dump(config_data, f)

    def import_config(self, file_path: Path):
        if file_path.suffix == ".json":
            with open(file_path, "r") as f:
                config_data = json.load(f)
        elif file_path.suffix == ".toml":
            with open(file_path, "r") as f:
                config_data = toml.load(f)
        else:  # assume yaml
            with open(file_path, "r") as f:
                config_data = yaml.safe_load(f)
        
        self.config = Config(**config_data)
        self.save_config()

def get_config() -> Config:
    return ConfigManager().config