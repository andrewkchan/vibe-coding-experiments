import argparse
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Dict, Any
import yaml
import os

DEFAULT_DATA_DIR = "./crawler_data"
DEFAULT_FETCHER_WORKERS = 500
DEFAULT_PARSER_WORKERS = 80
DEFAULT_NUM_FETCHER_PROCESSES = 2
DEFAULT_NUM_PARSER_PROCESSES = 1
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_REDIS_HOST = "localhost"
DEFAULT_REDIS_PORT = 6379
DEFAULT_REDIS_DB = 0
DEFAULT_POLITENESS_DELAY = 70

def parse_args(sys_args: Optional[List[str]] = None) -> tuple[argparse.Namespace, Dict[str, Any]]:
    parser = argparse.ArgumentParser(description="An experimental web crawler.")

    # Configuration file support
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to YAML configuration file. CLI args override config file values."
    )

    parser.add_argument(
        "--seed-file",
        type=Path,
        required=False,  # Not required if using config file
        help="Path to the seed file (newline-separated domains/URLs)."
    )
    parser.add_argument(
        "--email",
        type=str,
        required=False,  # Not required if using config file
        help="Contact email for the User-Agent string."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,  # Will be set from config or default
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
        default=None,
        help=f"Number of concurrent fetcher tasks per fetcher process (default: {DEFAULT_FETCHER_WORKERS})"
    )
    parser.add_argument(
        "--parser-workers",
        type=int,
        default=None,
        help=f"Number of concurrent parser tasks per parser process (default: {DEFAULT_PARSER_WORKERS})"
    )
    parser.add_argument(
        "--num-fetcher-processes",
        type=int,
        default=None,
        help=f"Number of fetcher processes to run (default: {DEFAULT_NUM_FETCHER_PROCESSES})"
    )
    parser.add_argument(
        "--num-parser-processes",
        type=int,
        default=None,
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
        default=None,
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
    
    # Legacy Redis args (for backward compatibility)
    parser.add_argument(
        "--redis-host",
        type=str,
        default=None,
        help="Redis host (deprecated: use --config file instead)."
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=None,
        help="Redis port (deprecated: use --config file instead)."
    )
    parser.add_argument(
        "--redis-db",
        type=int,
        default=None,
        help="Redis database (deprecated: use --config file instead)."
    )
    parser.add_argument(
        "--redis-password",
        type=str,
        default=None,
        help="Redis password (deprecated: use --config file instead)."
    )

    # Pod-specific overrides
    parser.add_argument(
        "--override-fetchers-per-pod",
        type=int,
        default=None,
        help="Override fetchers per pod from config file"
    )
    parser.add_argument(
        "--override-parsers-per-pod",
        type=int,
        default=None,
        help="Override parsers per pod from config file"
    )
    parser.add_argument(
        "--override-num-pods",
        type=int,
        default=None,
        help="Override number of pods (for testing with fewer pods)"
    )

    args = parser.parse_args(sys_args)
    
    # Load config file if provided
    config_data = {}
    if args.config and args.config.exists():
        with open(args.config, 'r') as f:
            config_data = yaml.safe_load(f) or {}
    
    # Validation - require email and seed file from either source
    if not args.email and 'email' not in config_data:
        parser.error("--email is required (either via CLI or config file).")
    if not args.seed_file and 'seed_file' not in config_data:
        parser.error("--seed-file is required (either via CLI or config file).")
        
    return args, config_data


def load_yaml_config(config_path: Path) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f) or {}


@dataclass
class PodConfig:
    """Configuration for a single pod."""
    pod_id: int
    redis_url: str
    
    def get_redis_kwargs(self) -> dict:
        """Parse Redis URL and return connection kwargs."""
        # Parse redis://[password@]host:port/db format
        from urllib.parse import urlparse
        parsed = urlparse(self.redis_url)
        
        kwargs = {
            'host': parsed.hostname or 'localhost',
            'port': parsed.port or 6379,
            'db': int(parsed.path.lstrip('/')) if parsed.path and parsed.path != '/' else 0,
            'decode_responses': True,
            'max_connections': 100,
            'timeout': None,
            'health_check_interval': 30,
        }
        
        if parsed.password:
            kwargs['password'] = parsed.password
            
        return kwargs


@dataclass
class CrawlerConfig:
    """Typed configuration class for the web crawler."""
    # Basic configuration
    seed_file: Path
    email: str
    exclude_file: Optional[Path]
    max_pages: Optional[int]
    max_duration: Optional[int]
    log_level: str
    resume: bool
    seeded_urls_only: bool
    user_agent: str
    
    # Pod configuration
    pod_configs: List[PodConfig] = field(default_factory=list)
    fetchers_per_pod: int = 6
    parsers_per_pod: int = 2
    fetcher_workers: int = DEFAULT_FETCHER_WORKERS
    parser_workers: int = DEFAULT_PARSER_WORKERS
    
    # Storage configuration
    data_dirs: List[Path] = field(default_factory=list)
    log_dir: Path = Path("/var/log/crawler")
    
    # CPU affinity
    enable_cpu_affinity: bool = False
    
    # Redis configuration (for backward compatibility and global settings)
    redis_db: int = DEFAULT_REDIS_DB
    redis_password: Optional[str] = None
    redis_max_connections: int = 100
    
    # Crawler behavior
    politeness_delay_seconds: int = DEFAULT_POLITENESS_DELAY
    robots_cache_ttl_seconds: int = 86400
    
    # Performance tuning
    parse_queue_soft_limit: int = 20000
    parse_queue_hard_limit: int = 80000
    
    # Bloom filter settings
    bloom_filter_capacity: int = 200_000_000
    bloom_filter_error_rate: float = 0.001
    
    # Global coordination
    global_coordination_redis_pod: int = 0
    global_metrics_update_interval: int = 10
    
    # Monitoring
    prometheus_port: int = 8001
    
    # Legacy single-Redis support (backward compatibility)
    redis_host: Optional[str] = None
    redis_port: Optional[int] = None
    
    # Computed properties
    @property
    def num_pods(self) -> int:
        """Number of pods in the system."""
        return len(self.pod_configs)
    
    @property
    def num_fetcher_processes(self) -> int:
        """Total number of fetcher processes across all pods."""
        return self.num_pods * self.fetchers_per_pod if self.pod_configs else DEFAULT_NUM_FETCHER_PROCESSES
    
    @property
    def num_parser_processes(self) -> int:
        """Total number of parser processes across all pods."""
        return self.num_pods * self.parsers_per_pod if self.pod_configs else DEFAULT_NUM_PARSER_PROCESSES
    
    @property
    def data_dir(self) -> Path:
        """Primary data directory (for backward compatibility)."""
        return self.data_dirs[0] if self.data_dirs else Path(DEFAULT_DATA_DIR)
    
    def get_data_dir_for_url(self, url: str) -> Path:
        """Get the data directory for a given URL based on sharding."""
        if len(self.data_dirs) <= 1:
            return self.data_dir
            
        import hashlib
        url_hash = hashlib.sha256(url.encode()).hexdigest()
        dir_index = int(url_hash[:8], 16) % len(self.data_dirs)
        return self.data_dirs[dir_index]

    def get_redis_connection_kwargs(self) -> dict:
        """Get Redis connection parameters for backward compatibility."""
        if self.redis_host:
            # Legacy single-Redis mode
            kwargs = {
                'host': self.redis_host,
                'port': self.redis_port or DEFAULT_REDIS_PORT,
                'db': self.redis_db,
                'decode_responses': True,
                'max_connections': self.redis_max_connections,
                'timeout': None,
                'health_check_interval': 30,
            }
            
            if self.redis_password:
                kwargs['password'] = self.redis_password
                
            return kwargs
        else:
            # In pod mode, this returns pod 0's config by default
            if self.pod_configs:
                return self.pod_configs[0].get_redis_kwargs()
            else:
                # Fallback to localhost
                return {
                    'host': DEFAULT_REDIS_HOST,
                    'port': DEFAULT_REDIS_PORT,
                    'db': DEFAULT_REDIS_DB,
                    'decode_responses': True,
                    'max_connections': self.redis_max_connections,
                    'timeout': None,
                    'health_check_interval': 30,
                }

    @classmethod
    def from_args(cls, sys_args: Optional[List[str]] = None) -> "CrawlerConfig":
        args, config_data = parse_args(sys_args)
        
        
        # Apply CLI overrides
        if args.override_num_pods and 'pods' in config_data:
            config_data['pods'] = config_data['pods'][:args.override_num_pods]
        if args.override_fetchers_per_pod:
            config_data['fetchers_per_pod'] = args.override_fetchers_per_pod
        if args.override_parsers_per_pod:
            config_data['parsers_per_pod'] = args.override_parsers_per_pod
        
        # Create PodConfig objects
        pod_configs = []
        if 'pods' in config_data:
            for i, pod_data in enumerate(config_data['pods']):
                pod_configs.append(PodConfig(
                    pod_id=i,
                    redis_url=pod_data['redis_url'] if isinstance(pod_data, dict) else pod_data
                ))
        
        # Handle data directories
        data_dirs = []
        if args.data_dir:
            # CLI arg takes precedence
            data_dirs = [Path(args.data_dir)]
        elif 'data_dirs' in config_data:
            data_dirs = [Path(d) for d in config_data['data_dirs']]
        else:
            data_dirs = [Path(DEFAULT_DATA_DIR)]
        
        # Create all data directories if they don't exist
        for data_dir in data_dirs:
            data_dir.mkdir(parents=True, exist_ok=True)
        
        # Build user agent
        email = args.email or config_data.get('email')
        if not email:
            raise ValueError("Email is required either via --email or in config file")
        user_agent_template = config_data.get('user_agent_template', 'ExperimentalCrawler/1.0 ({email})')
        user_agent = user_agent_template.format(email=email)
        
        # Determine if we're in legacy mode (backward compatibility)
        legacy_mode = args.redis_host is not None or not pod_configs
        
        if legacy_mode:
            # Create a single pod config for backward compatibility
            redis_url = f"redis://{args.redis_host or DEFAULT_REDIS_HOST}:{args.redis_port or DEFAULT_REDIS_PORT}/{args.redis_db or DEFAULT_REDIS_DB}"
            if args.redis_password:
                redis_url = f"redis://:{args.redis_password}@{args.redis_host or DEFAULT_REDIS_HOST}:{args.redis_port or DEFAULT_REDIS_PORT}/{args.redis_db or DEFAULT_REDIS_DB}"
            pod_configs = [PodConfig(pod_id=0, redis_url=redis_url)]

        # Get seed file path
        seed_file_path = args.seed_file or config_data.get('seed_file')
        if not seed_file_path:
            raise ValueError("Seed file is required either via --seed-file or in config file")
        
        return cls(
            # Basic configuration
            seed_file=Path(seed_file_path),
            email=email,
            exclude_file=Path(args.exclude_file) if args.exclude_file else (Path(config_data['exclude_file']) if 'exclude_file' in config_data else None),
            max_pages=args.max_pages or config_data.get('max_pages'),
            max_duration=args.max_duration or config_data.get('max_duration'),
            log_level=(args.log_level or config_data.get('log_level', DEFAULT_LOG_LEVEL)).upper(),
            resume=args.resume or config_data.get('resume', False),
            seeded_urls_only=args.seeded_urls_only or config_data.get('seeded_urls_only', False),
            user_agent=user_agent,
            
            # Pod configuration
            pod_configs=pod_configs,
            fetchers_per_pod=config_data.get('fetchers_per_pod', 6) if not legacy_mode else (args.num_fetcher_processes or DEFAULT_NUM_FETCHER_PROCESSES),
            parsers_per_pod=config_data.get('parsers_per_pod', 2) if not legacy_mode else (args.num_parser_processes or DEFAULT_NUM_PARSER_PROCESSES),
            fetcher_workers=args.fetcher_workers or config_data.get('fetcher_workers', DEFAULT_FETCHER_WORKERS),
            parser_workers=args.parser_workers or config_data.get('parser_workers', DEFAULT_PARSER_WORKERS),
            
            # Storage configuration
            data_dirs=data_dirs,
            log_dir=Path(config_data.get('log_dir', '/var/log/crawler')),
            
            # CPU affinity
            enable_cpu_affinity=config_data.get('enable_cpu_affinity', False),
            
            # Redis configuration
            redis_db=args.redis_db or config_data.get('redis_db', DEFAULT_REDIS_DB),
            redis_password=args.redis_password or config_data.get('redis_password'),
            redis_max_connections=config_data.get('redis_max_connections', 100),
            
            # Crawler behavior
            politeness_delay_seconds=config_data.get('politeness_delay_seconds', DEFAULT_POLITENESS_DELAY),
            robots_cache_ttl_seconds=config_data.get('robots_cache_ttl_seconds', 86400),
            
            # Performance tuning
            parse_queue_soft_limit=config_data.get('parse_queue_soft_limit', 20000),
            parse_queue_hard_limit=config_data.get('parse_queue_hard_limit', 80000),
            
            # Bloom filter
            bloom_filter_capacity=config_data.get('bloom_filter_capacity', 10000000000),
            bloom_filter_error_rate=config_data.get('bloom_filter_error_rate', 0.001),
            
            # Global coordination
            global_coordination_redis_pod=config_data.get('global_coordination_redis_pod', 0),
            global_metrics_update_interval=config_data.get('global_metrics_update_interval', 10),
            
            # Monitoring
            prometheus_port=config_data.get('prometheus_port', 8001),
            
            # Legacy support
            redis_host=args.redis_host,
            redis_port=args.redis_port,
        ) 