# main.py - Entry point for the web crawler

import asyncio
import logging
import signal

from crawler_module.config import parse_args, CrawlerConfig
from crawler_module.orchestrator import CrawlerOrchestrator

# Configure basic logging until config is parsed
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__) # Get a logger for main

async def main():
    config: CrawlerConfig = parse_args()

    # Reconfigure logging based on parsed config
    # Get the root logger and remove existing handlers to avoid duplicate messages
    # if basicConfig was called before (e.g. by imports)
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    logging.basicConfig(
        level=config.log_level.upper(),
        format="%(asctime)s - %(levelname)s - %(name)s [%(process)d] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        # filename=config.data_dir / "crawler.log" # Optional: log to file
    )
    
    logger.info(f"Logging reconfigured to level: {config.log_level.upper()}")
    logger.info("Starting crawler...")

    orchestrator = CrawlerOrchestrator(config)

    # --- Graceful shutdown handling ---
    loop = asyncio.get_event_loop()
    stop_event_internal = asyncio.Event() # Internal event for main to signal orchestrator

    def _signal_handler():
        logger.info("Shutdown signal received. Initiating graceful shutdown...")
        # This will signal the orchestrator's internal shutdown_event
        # which the orchestrator's run_crawl loop checks.
        # For direct cancellation of orchestrator.run_crawl() task:
        # if orchestrator_task and not orchestrator_task.done():
        #    orchestrator_task.cancel()
        # However, a cooperative shutdown via its internal event is cleaner.
        if orchestrator and hasattr(orchestrator, '_shutdown_event'):
             orchestrator._shutdown_event.set()
        stop_event_internal.set() # Also signal our main loop to stop waiting if it is

    # For POSIX systems (Linux, macOS)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # add_signal_handler is not available on all platforms (e.g. Windows sometimes)
            logger.warning(f"Signal handler for {sig.name} could not be added. Graceful shutdown via Ctrl+C might not work as expected.")
            # Fallback or alternative mechanism might be needed for Windows if signals don't work well.

    orchestrator_task = None
    try:
        orchestrator_task = asyncio.create_task(orchestrator.run_crawl())
        await orchestrator_task 
    except asyncio.CancelledError:
        logger.info("Main orchestrator task was cancelled.")
    except Exception as e:
        logger.critical(f"Critical error in main execution: {e}", exc_info=True)
    finally:
        logger.info("Crawler shutting down main process.")
        # Remove signal handlers cleanly
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.remove_signal_handler(sig)
            except (NotImplementedError, RuntimeError): # RuntimeError if loop is closed
                pass

if __name__ == "__main__":
    asyncio.run(main()) 