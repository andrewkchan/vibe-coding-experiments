import argparse
import logging
from dataclasses import dataclass
from pathlib import Path

DEFAULT_DATA_DIR = "./crawler_data"
DEFAULT_MAX_WORKERS = 20
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_DB_TYPE = "sqlite"

@dataclass
class CrawlerConfig:
    seed_file: Path
    email: str
    data_dir: Path
    exclude_file: Path | None
    max_workers: int
    max_pages: int | None
    max_duration: int | None # in seconds
    log_level: str
    resume: bool
    user_agent: str # Will be constructed
    seeded_urls_only: bool
    db_type: str # sqlite, postgresql, or redis
    db_url: str | None # PostgreSQL connection URL

def parse_args() -> CrawlerConfig:
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
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Number of concurrent fetcher tasks (default: {DEFAULT_MAX_WORKERS})"
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
        help="Maximum duration for the crawl in seconds."
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
        help="Attempt to resume from existing data in data-dir. If not set and data-dir exists, crawler may exit."
    )
    parser.add_argument(
        "--seeded-urls-only",
        action="store_true",
        help="Only crawl seeded URLs."
    )
    parser.add_argument(
        "--db-type",
        type=str,
        default=DEFAULT_DB_TYPE,
        choices=["sqlite", "postgresql", "redis"],
        help=f"Database backend to use (default: {DEFAULT_DB_TYPE}). Use 'postgresql' for high concurrency, 'redis' for hybrid Redis+file storage."
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default=None,
        help="PostgreSQL connection URL (required if db-type is postgresql). Example: postgresql://user:pass@localhost/dbname"
    )
    parser.add_argument(
        "--pg-pool-size",
        type=int,
        default=None,
        help="PostgreSQL connection pool size override. If not set, automatically calculated based on worker count."
    )

    args = parser.parse_args()
    
    # Validate database configuration
    if args.db_type == "postgresql" and not args.db_url:
        parser.error("--db-url is required when using PostgreSQL (--db-type=postgresql)")
    
    # Store pool size override in environment variable if provided
    if args.db_type == "postgresql" and args.pg_pool_size:
        import os
        os.environ['CRAWLER_PG_POOL_SIZE'] = str(args.pg_pool_size)
    
    # Redis doesn't require db_url as it uses default localhost:6379
    if args.db_type == "redis" and args.db_url:
        parser.error("--db-url is not used with Redis backend (uses localhost:6379 by default)")
    
    # Construct User-Agent
    # Example: MyEducationalCrawler/1.0 (+http://example.com/crawler-info; mailto:user@example.com)
    # For now, a simpler version. We can make the URL part configurable later if needed.
    user_agent = f"MyEducationalCrawler/1.0 (mailto:{args.email})"

    return CrawlerConfig(
        seed_file=args.seed_file,
        email=args.email,
        data_dir=args.data_dir,
        exclude_file=args.exclude_file,
        max_workers=args.max_workers,
        max_pages=args.max_pages,
        max_duration=args.max_duration,
        log_level=args.log_level.upper(), # Ensure log level is uppercase for logging module
        resume=args.resume,
        user_agent=user_agent,
        seeded_urls_only=args.seeded_urls_only,
        db_type=args.db_type,
        db_url=args.db_url
    ) 