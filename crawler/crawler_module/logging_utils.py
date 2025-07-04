"""Logging utilities for pod-based crawler architecture."""

import logging
import logging.handlers
import os
from pathlib import Path
from typing import Optional, Union

def setup_pod_logging(
    config,
    pod_id: int,
    process_type: str,
    process_id: Union[int, str] = 0,
    to_file: bool = True
) -> logging.Logger:
    """Set up logging for a specific pod and process.
    
    Args:
        config: CrawlerConfig instance
        pod_id: Pod identifier (0-15)
        process_type: Type of process ('orchestrator', 'fetcher', 'parser')
        process_id: Process identifier within the pod
        to_file: Whether to log to file (default: True) or just console
        
    Returns:
        Configured logger instance
    """
    # Determine log level
    log_level = getattr(logging, config.log_level.upper(), logging.INFO)
    
    # Create process identifier string
    if process_type == 'orchestrator':
        process_label = f"Orchestrator"
    else:
        process_label = f"Pod-{pod_id}-{process_type.capitalize()}-{process_id}"
    
    # Configure root logger
    root_logger = logging.getLogger()
    
    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Set up format
    log_format = f'%(asctime)s - %(name)s - %(levelname)s - [PID:%(process)d] [{process_label}] %(message)s'
    formatter = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')
    
    # Console handler (always add)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    root_logger.addHandler(console_handler)
    
    # File handler (if requested and log_dir is configured)
    if to_file and hasattr(config, 'log_dir') and config.log_dir:
        try:
            # Create pod-specific log directory
            pod_log_dir = Path(config.log_dir) / f"pod_{pod_id}"
            pod_log_dir.mkdir(parents=True, exist_ok=True)
            
            # Determine log filename
            if process_type == 'orchestrator':
                log_filename = pod_log_dir / 'orchestrator.log'
            else:
                log_filename = pod_log_dir / f'{process_type}_{process_id}.log'
            
            # Create rotating file handler (100MB per file, keep 5 backups)
            file_handler = logging.handlers.RotatingFileHandler(
                str(log_filename),
                maxBytes=100 * 1024 * 1024,  # 100MB
                backupCount=5,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            file_handler.setLevel(log_level)
            root_logger.addHandler(file_handler)
            
            # Log that we're logging to file
            logger = logging.getLogger(__name__)
            logger.info(f"Logging to file: {log_filename}")
            
        except Exception as e:
            # If file logging fails, continue with console only
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to set up file logging: {e}")
    
    # Set root logger level
    root_logger.setLevel(log_level)
    
    return root_logger


def get_process_logger(name: str, pod_id: Optional[int] = None) -> logging.Logger:
    """Get a logger for a specific module with optional pod context.
    
    Args:
        name: Logger name (usually __name__)
        pod_id: Optional pod ID to include in logger name
        
    Returns:
        Logger instance
    """
    if pod_id is not None and pod_id > 0:
        # Include pod ID in logger name for better filtering
        logger_name = f"{name}.pod{pod_id}"
    else:
        logger_name = name
    
    return logging.getLogger(logger_name) 