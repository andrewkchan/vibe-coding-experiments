"""Factory for creating frontier instances based on configuration."""

import logging
import redis.asyncio as redis
from typing import Union

from .config import CrawlerConfig
from .frontier import FrontierManager
from .frontier_redis import RedisFrontierManager
from .politeness import PolitenessEnforcer

logger = logging.getLogger(__name__)

def create_frontier(
    config: CrawlerConfig, 
    politeness: PolitenessEnforcer,
    redis_client: redis.Redis
) -> Union[FrontierManager, RedisFrontierManager]:
    """Create a frontier instance based on the configured type.
    
    Args:
        config: Crawler configuration
        politeness: Politeness enforcer instance
        redis_client: Redis client instance
        
    Returns:
        Either FrontierManager (hybrid) or RedisFrontierManager (redis-only)
    """
    if config.frontier_type == "redis":
        logger.info("Creating Redis-only frontier manager")
        return RedisFrontierManager(config, politeness, redis_client)
    else:
        logger.info("Creating hybrid (filesystem+Redis) frontier manager")
        return FrontierManager(config, politeness, redis_client) 