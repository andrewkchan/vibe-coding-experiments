"""
Memory diagnostics utilities for the web crawler.
Provides detailed analysis of memory usage by component.
"""

import gc
import sys
import logging
import asyncio
import weakref
from typing import Dict, List, Tuple, Any
from collections import defaultdict
import psutil
import os
import pympler

logger = logging.getLogger(__name__)

class MemoryDiagnostics:
    """Provides detailed memory diagnostics for the crawler."""
    
    def __init__(self):
        self.baseline_rss = 0
        self.component_sizes: Dict[str, int] = {}
        
    def get_process_memory_info(self) -> Dict[str, float]:
        """Get current process memory statistics."""
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        
        return {
            'rss_mb': mem_info.rss / 1024 / 1024,
            'vms_mb': mem_info.vms / 1024 / 1024,
            'available_system_mb': psutil.virtual_memory().available / 1024 / 1024,
            'percent': process.memory_percent()
        }
    
    def analyze_object_types(self, limit: int = 20) -> List[Tuple[str, int, int]]:
        """Analyze object types in memory.
        Returns list of (type_name, count, total_size) tuples.
        """
        gc.collect()  # Force collection before analysis
        
        type_counts: Dict[str, int] = defaultdict(int)
        type_sizes: Dict[str, int] = defaultdict(int)
        
        for obj in gc.get_objects():
            obj_type = type(obj).__name__
            type_counts[obj_type] += 1
            
            try:
                # Get size estimate (not always accurate but useful)
                size = sys.getsizeof(obj)
                type_sizes[obj_type] += size
            except:
                # Some objects don't support getsizeof
                pass
        
        # Sort by total size
        results = [(name, type_counts[name], type_sizes[name]) 
                   for name in type_counts]
        results.sort(key=lambda x: x[2], reverse=True)
        
        return results[:limit]
    
    def analyze_large_objects(self, min_size_mb: float = 1.0) -> List[Dict[str, Any]]:
        """Find individual objects larger than min_size_mb."""
        gc.collect()
        
        large_objects = []
        min_size_bytes = min_size_mb * 1024 * 1024
        
        for obj in gc.get_objects():
            try:
                size = sys.getsizeof(obj)
                if size >= min_size_bytes:
                    obj_info = {
                        'type': type(obj).__name__,
                        'size_mb': size / 1024 / 1024,
                        'id': id(obj)
                    }
                    
                    # Add more details for specific types
                    if isinstance(obj, dict):
                        obj_info['len'] = len(obj)
                        # Sample keys for dictionaries
                        if len(obj) > 0:
                            sample_keys = list(obj.keys())[:5]
                            obj_info['sample_keys'] = [str(k)[:50] for k in sample_keys]
                    elif isinstance(obj, (list, tuple, set)):
                        obj_info['len'] = len(obj)
                    elif isinstance(obj, (str, bytes)):
                        obj_info['len'] = len(obj)
                        obj_info['preview'] = repr(obj[:100])
                    
                    large_objects.append(obj_info)
            except:
                pass
        
        large_objects.sort(key=lambda x: float(x['size_mb']), reverse=True)
        return large_objects
    
    def analyze_component_memory(self, orchestrator) -> Dict[str, Dict[str, Any]]:
        """Analyze memory usage by crawler components."""
        results = {}
        
        # Analyze fetcher session
        if hasattr(orchestrator, 'fetcher') and hasattr(orchestrator.fetcher, 'session'):
            session = orchestrator.fetcher.session
            if session:
                connector_info = {
                    'type': 'aiohttp_session',
                    'closed': session.closed
                }
                
                if hasattr(session, 'connector') and session.connector:
                    connector = session.connector
                    if hasattr(connector, '_acquired'):
                        connector_info['acquired_connections'] = len(connector._acquired)
                    if hasattr(connector, '_conns'):
                        connector_info['cached_connections'] = sum(len(conns) for conns in connector._conns.values())
                    if hasattr(connector, '_limit'):
                        connector_info['connection_limit'] = connector._limit
                    if hasattr(connector, '_limit_per_host'):
                        connector_info['limit_per_host'] = connector._limit_per_host
                
                results['aiohttp_session'] = connector_info
        
        # Analyze politeness enforcer caches
        if hasattr(orchestrator, 'politeness'):
            politeness = orchestrator.politeness
            
            # Robots parser cache
            robots_info = {
                'type': 'robots_cache',
                'count': len(politeness.robots_parsers),
                'max_size': politeness.robots_parsers_max_size,
                'estimated_size_mb': pympler.asizeof.asizeof(politeness.robots_parsers) / 1024 / 1024
            }
            
            results['robots_cache'] = robots_info
            
            # Exclusion cache
            results['exclusion_cache'] = {
                'type': 'exclusion_cache',
                'count': len(politeness.exclusion_cache),
                'max_size': politeness.exclusion_cache_max_size,
                'estimated_size_mb': pympler.asizeof.asizeof(politeness.exclusion_cache) / 1024 / 1024
            }
        
        # Analyze frontier
        if hasattr(orchestrator, 'frontier'):
            frontier = orchestrator.frontier
            frontier_info = {
                'type': 'frontier',
                'domain_queues': len(getattr(frontier, 'domain_queues', {})),
                'read_locks_count': len(getattr(frontier, '_read_locks', {})),
                'read_locks_size_mb': pympler.asizeof.asizeof(getattr(frontier, '_read_locks', {})) / 1024 / 1024
            }
            
            results['frontier'] = frontier_info
        
        # Analyze Redis connection pools
        for name, client in [('redis_text', getattr(orchestrator, 'redis_client', None)),
                            ('redis_binary', getattr(orchestrator, 'redis_client_binary', None))]:
            if client and hasattr(client, '_client') and hasattr(client._client, 'connection_pool'):
                pool = client._client.connection_pool
                pool_info = {
                    'type': f'{name}_pool',
                    'created_connections': len(pool._created_connections) if hasattr(pool, '_created_connections') else 'unknown',
                    'available_connections': len(pool._available_connections) if hasattr(pool, '_available_connections') else 'unknown',
                    'in_use_connections': len(pool._in_use_connections) if hasattr(pool, '_in_use_connections') else 'unknown'
                }
                results[name] = pool_info
        
        # Count asyncio tasks
        try:
            all_tasks = asyncio.all_tasks()
            task_states: Dict[str, int] = defaultdict(int)
            for task in all_tasks:
                if task.done():
                    task_states['done'] += 1
                elif task.cancelled():
                    task_states['cancelled'] += 1
                else:
                    task_states['pending'] += 1
            
            results['asyncio_tasks'] = {
                'type': 'asyncio_tasks',
                'total': len(all_tasks),
                'states': dict(task_states)
            }
        except:
            pass
        
        return results
    
    def generate_report(self, orchestrator) -> str:
        """Generate a comprehensive memory diagnostic report."""
        report_lines = ["=== Memory Diagnostics Report ===\n"]
        
        # Process memory info
        mem_info = self.get_process_memory_info()
        report_lines.append("Process Memory:")
        report_lines.append(f"  RSS: {mem_info['rss_mb']:.1f} MB")
        report_lines.append(f"  VMS: {mem_info['vms_mb']:.1f} MB")
        report_lines.append(f"  System Available: {mem_info['available_system_mb']:.1f} MB")
        report_lines.append(f"  Process %: {mem_info['percent']:.1f}%\n")
        
        # Component analysis
        report_lines.append("Component Memory Usage:")
        component_info = self.analyze_component_memory(orchestrator)
        for component, info in component_info.items():
            report_lines.append(f"\n  {component}:")
            for key, value in info.items():
                if key != 'type':
                    report_lines.append(f"    {key}: {value}")
        
        # Object type analysis
        report_lines.append("\n\nTop Object Types by Size:")
        object_types = self.analyze_object_types(20)
        for type_name, count, total_size in object_types[:10]:
            size_mb = total_size / 1024 / 1024
            report_lines.append(f"  {type_name}: {count:,} objects, {size_mb:.1f} MB total")
        
        # Large objects
        report_lines.append("\n\nLarge Individual Objects (>1MB):")
        large_objects = self.analyze_large_objects(1.0)
        for obj in large_objects[:10]:
            report_lines.append(f"  {obj['type']}: {obj['size_mb']:.1f} MB (id: {obj['id']})")
            for key in ['len', 'preview', 'sample_keys']:
                if key in obj:
                    report_lines.append(f"    {key}: {obj[key]}")
        
        # GC stats
        report_lines.append("\n\nGarbage Collector Stats:")
        for i, stats in enumerate(gc.get_stats()):
            report_lines.append(f"  Generation {i}: {stats}")
        
        return "\n".join(report_lines)

# Global instance for easy access
memory_diagnostics = MemoryDiagnostics() 