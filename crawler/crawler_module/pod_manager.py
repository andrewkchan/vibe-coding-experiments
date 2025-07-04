"""Pod management for distributed crawler architecture."""

import hashlib
import logging
from typing import List, Dict, Optional
import redis.asyncio as redis
from redis.asyncio import BlockingConnectionPool

from .config import CrawlerConfig, PodConfig
from .redis_shield import ShieldedRedis

logger = logging.getLogger(__name__)


class PodManager:
    """Manages pod assignment and Redis connections for all pods."""
    
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.pod_configs = config.pod_configs
        self.num_pods = len(self.pod_configs)
        
        # Redis clients for each pod (lazy-loaded)
        self._redis_clients: Dict[int, ShieldedRedis] = {}
        self._redis_clients_binary: Dict[int, ShieldedRedis] = {}
        
        logger.info(f"PodManager initialized with {self.num_pods} pods")
    
    def get_pod_for_domain(self, domain: str) -> int:
        """Get the pod ID responsible for a given domain.
        
        Uses MD5 hash for consistent domain-to-pod mapping.
        """
        if self.num_pods == 1:
            return 0
            
        # Use MD5 for speed (cryptographic security not needed here)
        domain_hash = hashlib.md5(domain.encode('utf-8')).hexdigest()
        # Use first 8 hex chars (32 bits) for better distribution
        hash_value = int(domain_hash[:8], 16)
        pod_id = hash_value % self.num_pods
        
        return pod_id
    
    def get_pod_config(self, pod_id: int) -> PodConfig:
        """Get configuration for a specific pod."""
        if pod_id < 0 or pod_id >= self.num_pods:
            raise ValueError(f"Invalid pod_id {pod_id}. Must be 0-{self.num_pods-1}")
        return self.pod_configs[pod_id]
    
    async def get_redis_client(self, pod_id: int, binary: bool = False) -> ShieldedRedis:
        """Get Redis client for a specific pod.
        
        Args:
            pod_id: Pod identifier
            binary: If True, returns a client configured for binary data
        
        Returns:
            ShieldedRedis client for the pod
        """
        cache = self._redis_clients_binary if binary else self._redis_clients
        
        if pod_id not in cache:
            pod_config = self.get_pod_config(pod_id)
            kwargs = pod_config.get_redis_kwargs()
            
            if binary:
                kwargs['decode_responses'] = False
            
            # Create blocking connection pool
            pool = BlockingConnectionPool(**kwargs)
            base_client = redis.Redis(connection_pool=pool)
            
            # Monkey-patch to remove decorator overhead
            import functools
            base_client.connection_pool.get_connection = functools.partial(
                base_client.connection_pool.get_connection.__wrapped__, 
                base_client.connection_pool
            )
            
            # Wrap with ShieldedRedis
            cache[pod_id] = ShieldedRedis(base_client)
            
            logger.info(f"Created {'binary' if binary else 'text'} Redis client for pod {pod_id}")
        
        return cache[pod_id]
    
    async def get_redis_for_domain(self, domain: str, binary: bool = False) -> ShieldedRedis:
        """Get Redis client for the pod responsible for a domain."""
        pod_id = self.get_pod_for_domain(domain)
        return await self.get_redis_client(pod_id, binary)
    
    def get_all_pod_ids(self) -> List[int]:
        """Get list of all pod IDs."""
        return list(range(self.num_pods))
    
    async def close_all(self):
        """Close all Redis connections."""
        logger.info("Closing all Redis connections...")
        
        for pod_id, client in self._redis_clients.items():
            try:
                await client.aclose()
                logger.debug(f"Closed text Redis client for pod {pod_id}")
            except Exception as e:
                logger.error(f"Error closing text Redis client for pod {pod_id}: {e}")
        
        for pod_id, client in self._redis_clients_binary.items():
            try:
                await client.aclose()
                logger.debug(f"Closed binary Redis client for pod {pod_id}")
            except Exception as e:
                logger.error(f"Error closing binary Redis client for pod {pod_id}: {e}")
        
        self._redis_clients.clear()
        self._redis_clients_binary.clear()
        
        logger.info("All Redis connections closed")
    
    def get_pod_distribution_stats(self, domains: List[str]) -> Dict[int, int]:
        """Get distribution of domains across pods for analysis.
        
        Useful for checking hot shard issues.
        """
        distribution = {pod_id: 0 for pod_id in range(self.num_pods)}
        
        for domain in domains:
            pod_id = self.get_pod_for_domain(domain)
            distribution[pod_id] += 1
        
        return distribution
    
    def log_pod_distribution(self, domains: List[str], label: str = "domains"):
        """Log pod distribution statistics."""
        stats = self.get_pod_distribution_stats(domains)
        total = len(domains)
        
        logger.info(f"Pod distribution for {total} {label}:")
        for pod_id in sorted(stats.keys()):
            count = stats[pod_id]
            percentage = (count / total * 100) if total > 0 else 0
            logger.info(f"  Pod {pod_id}: {count} ({percentage:.1f}%)")
        
        # Calculate and log imbalance
        if stats:
            min_count = min(stats.values())
            max_count = max(stats.values())
            avg_count = total / self.num_pods
            max_imbalance = ((max_count - avg_count) / avg_count * 100) if avg_count > 0 else 0
            
            logger.info(f"  Min: {min_count}, Max: {max_count}, Avg: {avg_count:.1f}")
            logger.info(f"  Max imbalance: {max_imbalance:.1f}% above average") 