import argparse
import logging
from dataclasses import dataclass
from pathlib import Path

DEFAULT_DATA_DIR = "./crawler_data"
DEFAULT_MAX_WORKERS = 50
DEFAULT_LOG_LEVEL = "INFO"

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

    args = parser.parse_args()
    
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
        user_agent=user_agent
    ) 