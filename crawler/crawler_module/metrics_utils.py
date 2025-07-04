import math
from typing import List, Optional
import os
from prometheus_client import CollectorRegistry, multiprocess

def calculate_percentiles(data: List[float], percentiles_to_calc: List[int]) -> List[float]:
    '''Calculates specified percentiles for a list of data.

    Args:
        data: A list of numbers.
        percentiles_to_calc: A list of percentiles to calculate (e.g., [50, 95, 99]).

    Returns:
        A list of calculated percentile values corresponding to percentiles_to_calc.
    '''
    if not data:
        return [0.0] * len(percentiles_to_calc) # Return 0 or NaN if data is empty, matching desired output type

    data.sort()
    results = []
    for p in percentiles_to_calc:
        if not (0 <= p <= 100):
            raise ValueError("Percentile must be between 0 and 100")
        
        k = (len(data) - 1) * (p / 100.0)
        f = math.floor(k)
        c = math.ceil(k)

        if f == c: # Exact index
            results.append(data[int(k)])
        else: # Interpolate
            d0 = data[int(f)] * (c - k)
            d1 = data[int(c)] * (k - f)
            results.append(d0 + d1)
    return results 

def get_aggregated_counter_value(metric_name: str) -> Optional[float]:
    """Get the aggregated value of a counter metric across all processes.
    
    Args:
        metric_name: The name of the counter metric (e.g., 'crawler_pages_crawled_total')
        
    Returns:
        The sum of the counter across all processes, or None if not found
    """
    multiproc_dir = os.environ.get('prometheus_multiproc_dir')
    if not multiproc_dir:
        # Not in multiprocess mode
        return None
    
    # Create a registry and collector to read multiprocess metrics
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    
    # Collect all metrics
    for metric_family in registry.collect():
        if metric_family.name == metric_name:
            # Sum all samples (counters are additive across processes)
            total = 0.0
            for sample in metric_family.samples:
                if sample.name == metric_name:
                    total += sample.value
            return total
    
    return None


def get_pages_crawled_total() -> int:
    """Get the total number of pages crawled across all processes.
    
    Returns:
        Total pages crawled, or 0 if metric not available
    """
    value = get_aggregated_counter_value('crawler_pages_crawled_total')
    return int(value) if value is not None else 0


def get_urls_added_total() -> int:
    """Get the total number of URLs added to frontier across all processes.
    
    Returns:
        Total URLs added, or 0 if metric not available
    """
    value = get_aggregated_counter_value('crawler_urls_added_total')
    return int(value) if value is not None else 0 