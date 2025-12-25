import asyncio
import logging

from producers.event_poller import RSSEventFetcher
from engine.event_filter import EventFilter
from engine.ai_engine import AIEngine
from engine.technical_engine import TechnicalEngine
from producers.fcm_publisher import run as run_fcm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("STOCK_AGENT")


async def main():
    logger.info("STARTING TRADING SYSTEM CORE...")

    poller = RSSEventFetcher()
    ev_filter = EventFilter()
    ai_engine = AIEngine()
    tech_engine = TechnicalEngine()

    tasks = [
        asyncio.create_task(poller.run_loop(), name="Poller"),
        asyncio.create_task(ev_filter.run(), name="Filter"),
        asyncio.create_task(ai_engine.run(), name="AI_Engine"),
        asyncio.create_task(tech_engine.run(), name="Tech_Engine"),
        asyncio.create_task(run_fcm(), name="FCM_Publisher")
    ]

    logger.info(f"All {len(tasks)} Engines Running.")

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Main loop received stop signal. Shutting down engines...")
    except Exception as e:
        logger.critical(f"Critical System Failure: {e}")
    finally:
        logger.info("Cleaning up tasks...")
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await poller.close()
        logger.info("System Shutdown Complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass