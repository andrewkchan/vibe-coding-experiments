import logging
from pathlib import Path
import hashlib
import time
from typing import Optional
import aiofiles
import redis.asyncio as redis

from .config import CrawlerConfig

logger = logging.getLogger(__name__)

REDIS_SCHEMA_VERSION = 1
MAX_CONTENT_LENGTH = 40_000 # truncate to 40k chars to prevent the system from dying during demonstration run

class StorageManager:
    """Redis-based storage manager for visited URLs and content."""
    
    def __init__(self, config: CrawlerConfig, redis_client: redis.Redis):
        self.config = config
        self.data_dir = Path(config.data_dir)
        self.content_dir = self.data_dir / "content"
        self.redis = redis_client
        
        self._init_storage()
    
    def _init_storage(self):
        """Initializes the data directory and content directory."""
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            self.content_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Storage directories initialized: {self.data_dir}")
        except OSError as e:
            logger.error(f"Error creating storage directories: {e}")
            raise
    
    async def init_db_schema(self):
        """Initializes Redis schema version."""
        try:
            logger.info("Initializing Redis schema")
            # Check and set schema version
            current_version = await self.redis.get("schema_version")
            if current_version:
                current_version = int(current_version)
                if current_version != REDIS_SCHEMA_VERSION:
                    logger.warning(f"Redis schema version mismatch. Current: {current_version}, Expected: {REDIS_SCHEMA_VERSION}")
                    # In the future, handle migrations here
            else:
                # First time initialization
                await self.redis.set("schema_version", str(REDIS_SCHEMA_VERSION))
                logger.info(f"Set Redis schema version to {REDIS_SCHEMA_VERSION}")
        except Exception as e:
            logger.error(f"Error during Redis schema initialization: {e}")
            raise
    
    def get_url_sha256(self, url: str) -> str:
        """Generates a SHA256 hash for a given URL."""
        return hashlib.sha256(url.encode('utf-8')).hexdigest()
    
    async def save_content_to_file(self, url_hash: str, text_content: str) -> Optional[Path]:
        """Saves extracted text content to a file asynchronously."""
        if not text_content:  # Do not save if no text content
            return None
        
        file_path = self.content_dir / f"{url_hash}.txt"
        try:
            async with aiofiles.open(file_path, mode='w', encoding='utf-8') as f:
                await f.write(text_content)
                await f.truncate(MAX_CONTENT_LENGTH)
            logger.debug(f"Saved content for {url_hash} to {file_path}")
            return file_path
        except IOError as e:
            logger.error(f"IOError saving content file {file_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error saving content file {file_path}: {e}")
            return None
    
    async def add_visited_page(
        self, 
        url: str, 
        status_code: int, 
        crawled_timestamp: int,
        content_type: Optional[str] = None, 
        content_storage_path_str: Optional[str] = None
    ):
        """Adds a record of a visited page to Redis."""
        url_sha256 = self.get_url_sha256(url)
        # Use first 16 chars of SHA256 for the key (as per architecture doc)
        url_hash = url_sha256[:16]
        
        try:
            # Prepare the hash data
            visited_data = {
                'url': url,
                'status_code': str(status_code),
                'fetched_at': str(crawled_timestamp),
                'content_path': content_storage_path_str or '',
            }
            
            # Add optional fields if present
            if content_type:
                visited_data['content_type'] = content_type
            
            # Store in Redis using pipeline for atomicity
            pipe = self.redis.pipeline()
            
            # Store visited metadata in hash
            pipe.hset(f'visited:{url_hash}', mapping=visited_data)
            
            # Mark URL as seen in bloom filter (if it exists)
            # Note: In production, the bloom filter should be created during migration
            # For now, we'll try to add and ignore if it doesn't exist
            pipe.execute_command('BF.ADD', 'seen:bloom', url)
            
            await pipe.execute()
            
            logger.info(f"Recorded visited URL: {url} (Status: {status_code})")
            
        except redis.ResponseError as e:
            if "not found" in str(e).lower():
                # Bloom filter doesn't exist, log warning but continue
                logger.warning(f"Bloom filter 'seen:bloom' not found. URL {url} not added to bloom filter.")
                # Still store the visited data without bloom filter
                await self.redis.hset(f'visited:{url_hash}', mapping=visited_data)
            else:
                logger.error(f"Redis error adding visited page {url}: {e}")
                raise
        except Exception as e:
            logger.error(f"Unexpected error adding visited page {url}: {e}")
            raise
    
    async def close(self):
        """Close Redis connection."""
        await self.redis.close() 