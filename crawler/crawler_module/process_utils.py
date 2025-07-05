"""Process management utilities for the crawler."""

import logging
import os
import psutil
from typing import List, Optional

logger = logging.getLogger(__name__)


def set_cpu_affinity(
    pod_start_core: int, 
    process_type: str, 
    process_id: int,
    fetchers_per_pod: int,
    parsers_per_pod: int,
    enabled: bool
) -> Optional[List[int]]:
    """Set CPU affinity for a process based on pod and process type.
    
    Args:
        pod_id: Pod identifier (0-15)
        process_type: Type of process ('orchestrator', 'fetcher', 'parser')
        process_id: Process ID within the pod (e.g., fetcher 0, 1, 2...)
        fetchers_per_pod: Number of fetcher processes per pod
        parsers_per_pod: Number of parser processes per pod
        enabled: Whether to actually set affinity
        
    Returns:
        List of CPU cores assigned (single core), or None if affinity not set
        
    CPU Allocation Strategy:
        - Each pod gets cores [pod_id * cores_per_pod : (pod_id + 1) * cores_per_pod]
        - Each process gets exactly one dedicated core
        - Pod 0: orchestrator (fetcher 0) gets core 0, fetchers 1-N get cores 1-N, parsers get remaining cores
        - Other pods: fetchers 0-N get cores 0-N, parsers get remaining cores
    """
    if not enabled:
        logger.debug(f"CPU affinity disabled for {process_type}-{process_id}")
        return None
    
    try:
        # Determine core based on process type and ID
        if process_type == 'orchestrator':
            # Orchestrator is fetcher 0 in pod 0, gets first core
            core_offset = 0
        elif process_type == 'fetcher':
            # Fetcher gets its own core based on ID
            if process_id >= fetchers_per_pod:
                logger.warning(f"Fetcher ID {process_id} exceeds fetchers_per_pod {fetchers_per_pod}")
                return None
            core_offset = process_id
        elif process_type == 'parser':
            # Parser gets core after all fetchers
            if process_id >= parsers_per_pod:
                logger.warning(f"Parser ID {process_id} exceeds parsers_per_pod {parsers_per_pod}")
                return None
            core_offset = fetchers_per_pod + process_id
        else:
            logger.warning(f"Unknown process type: {process_type}")
            return None
        
        # Calculate the single core for this process
        assigned_core = pod_start_core + core_offset
        cores = [assigned_core]
        
        # Check if cores are valid for this system
        cpu_count = psutil.cpu_count()
        if cpu_count is None:
            logger.warning("Unable to determine CPU count")
            return None
        
        available_cores = list(range(cpu_count))
        valid_cores = [c for c in cores if c in available_cores]
        
        if not valid_cores:
            logger.warning(
                f"No valid cores for {pod_start_core=} {process_type}-{process_id}. "
                f"Requested {cores}, available {available_cores}"
            )
            return None
        
        if len(valid_cores) < len(cores):
            logger.warning(
                f"Some requested cores not available for {pod_start_core=} {process_type}-{process_id}. "
                f"Using {valid_cores} instead of {cores}"
            )
        
        # Set CPU affinity
        process = psutil.Process()
        process.cpu_affinity(valid_cores)
        
        logger.info(
            f"Set CPU affinity for {pod_start_core=} {process_type}-{process_id} "
            f"(PID: {process.pid}) to cores: {valid_cores}"
        )
        
        return valid_cores
        
    except AttributeError:
        # cpu_affinity not supported on this platform
        logger.warning("CPU affinity not supported on this platform")
        return None
    except psutil.Error as e:
        logger.error(f"Failed to set CPU affinity: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error setting CPU affinity: {e}")
        return None


def get_current_cpu_affinity() -> Optional[List[int]]:
    """Get the current CPU affinity of this process.
    
    Returns:
        List of CPU cores this process is allowed to run on, or None if not supported
    """
    try:
        process = psutil.Process()
        return process.cpu_affinity()
    except AttributeError:
        # cpu_affinity not supported on this platform
        return None
    except psutil.Error:
        return None


def log_cpu_info():
    """Log information about the CPU configuration."""
    try:
        cpu_count = psutil.cpu_count(logical=True)
        cpu_count_physical = psutil.cpu_count(logical=False)
        
        logger.info(f"CPU Info: {cpu_count} logical cores, {cpu_count_physical} physical cores")
        
        # Log current CPU affinity if available
        current_affinity = get_current_cpu_affinity()
        if current_affinity:
            logger.info(f"Current process CPU affinity: {current_affinity}")
            
    except Exception as e:
        logger.error(f"Error getting CPU info: {e}") 