import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List

DEFAULT_DATA_DIR = "./crawler_data"
DEFAULT_FETCHER_WORKERS = 500
DEFAULT_PARSER_WORKERS = 80
DEFAULT_NUM_FETCHER_PROCESSES = 2
DEFAULT_NUM_PARSER_PROCESSES = 1
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_REDIS_HOST = "localhost"
DEFAULT_REDIS_PORT = 6379
DEFAULT_REDIS_DB = 0

def parse_args(sys_args: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="An experimental web crawler.")

    parser.add_argument(
        "--seed-file",
        type=Path,
        required=True,
        help="Path to the seed file (newline-separated domains/URLs)."
    )
    parser.add_argument(
        "--email",
        type=str,
        required=True,
        help="Contact email for the User-Agent string."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(DEFAULT_DATA_DIR),
        help=f"Directory to store database and crawled content (default: {DEFAULT_DATA_DIR})"
    )
    parser.add_argument(
        "--exclude-file",
        type=Path,
        default=None,
        help="Optional path to a file of domains to exclude (newline-separated)."
    )
    parser.add_argument(
        "--fetcher-workers",
        type=int,
        default=DEFAULT_FETCHER_WORKERS,
        help=f"Number of concurrent fetcher tasks per fetcher process (default: {DEFAULT_FETCHER_WORKERS})"
    )
    parser.add_argument(
        "--parser-workers",
        type=int,
        default=DEFAULT_PARSER_WORKERS,
        help=f"Number of concurrent parser tasks per parser process (default: {DEFAULT_PARSER_WORKERS})"
    )
    parser.add_argument(
        "--num-fetcher-processes",
        type=int,
        default=DEFAULT_NUM_FETCHER_PROCESSES,
        help=f"Number of fetcher processes to run (default: {DEFAULT_NUM_FETCHER_PROCESSES})"
    )
    parser.add_argument(
        "--num-parser-processes",
        type=int,
        default=DEFAULT_NUM_PARSER_PROCESSES,
        help=f"Number of parser processes to run (default: {DEFAULT_NUM_PARSER_PROCESSES})"
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum number of pages to crawl."
    )
    parser.add_argument(
        "--max-duration",
        type=int,
        default=None,
        help="Maximum duration to run the crawl in seconds. Stops after this time."
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=DEFAULT_LOG_LEVEL,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help=f"Logging level (default: {DEFAULT_LOG_LEVEL})"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Attempt to resume a previous crawl from the data in data_dir.",
    )
    parser.add_argument(
        "--seeded-urls-only",
        action="store_true",
        help="Only crawl URLs that were provided in the seed file. Do not crawl any discovered URLs.",
    )
    parser.add_argument(
        "--redis-host",
        type=str,
        default="localhost",
        help="Redis host."
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
        help="Redis port."
    )
    parser.add_argument(
        "--redis-db",
        type=int,
        default=0,
        help="Redis database."
    )
    parser.add_argument(
        "--redis-password",
        type=str,
        default=None,
        help="Redis password."
    )
    parser.add_argument(
        "--frontier-type",
        type=str,
        default="hybrid",
        choices=["hybrid", "redis"],
        help="Frontier storage type: 'hybrid' (filesystem+Redis) or 'redis' (Redis-only). Default: hybrid"
    )

    args = parser.parse_args(sys_args)
    
    # Validation for email
    if not args.email:
        parser.error("--email is required.")
        
    return args

@dataclass
class CrawlerConfig:
    """Typed configuration class for the web crawler."""
    seed_file: Path
    email: str
    data_dir: Path
    exclude_file: Optional[Path]
    fetcher_workers: int
    parser_workers: int
    num_fetcher_processes: int
    num_parser_processes: int
    max_pages: Optional[int]
    max_duration: Optional[int]
    log_level: str
    resume: bool
    seeded_urls_only: bool
    user_agent: str
    # Redis-specific configuration
    redis_host: str
    redis_port: int
    redis_db: int
    redis_password: Optional[str]
    frontier_type: str

    def get_redis_connection_kwargs(self) -> dict:
        """Get Redis connection parameters as kwargs dict."""
        kwargs = {
            'host': self.redis_host,
            'port': self.redis_port,
            'db': self.redis_db,
            'decode_responses': True,
            'max_connections': 100,
            'timeout': None, # Block forever waiting for a connection from the pool
            # Health check interval - helps detect stale connections
            'health_check_interval': 30,
        }
        
        if self.redis_password:
            kwargs['password'] = self.redis_password
            
        # Log configuration (without password)
        safe_kwargs = {k: v for k, v in kwargs.items() if k != 'password'}
        logging.info(f"Redis configuration: {safe_kwargs}")
        
        return kwargs

    @classmethod
    def from_args(cls, sys_args: Optional[List[str]] = None) -> "CrawlerConfig":
        args = parse_args(sys_args)
        user_agent = f"MyEducationalCrawler/1.0 (mailto:{args.email})"

        return cls(
            seed_file=Path(args.seed_file),
            email=args.email,
            data_dir=Path(args.data_dir),
            exclude_file=Path(args.exclude_file) if args.exclude_file else None,
            fetcher_workers=args.fetcher_workers,
            parser_workers=args.parser_workers,
            num_fetcher_processes=args.num_fetcher_processes,
            num_parser_processes=args.num_parser_processes,
            max_pages=args.max_pages,
            max_duration=args.max_duration,
            log_level=args.log_level.upper(),
            resume=args.resume,
            seeded_urls_only=args.seeded_urls_only,
            user_agent=user_agent,
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            redis_db=args.redis_db,
            redis_password=args.redis_password,
            frontier_type=args.frontier_type,
        ) 