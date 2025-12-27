import asyncio
import logging
import os
import threading
from flask import Flask

from config import redis_client
from producers.event_poller import RSSEventFetcher
from engine.event_filter import EventFilter
from engine.ai_engine import AIEngine
from engine.technical_engine import TechnicalEngine
from producers.fcm_publisher import run as run_fcm
from scheduler import start_scheduler
from jobs.ISIN_lookup import ISINLookupService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("STOCK_AGENT")

app = Flask(__name__)


@app.route('/')
def home():
    return "Stock Trading Agent is RUNNING (Async Core Active)"


def run_web_server():
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"Starting Web Server on port {port}...")
    app.run(host='0.0.0.0', port=port)


async def start_async_core():
    logger.info("STARTING TRADING SYSTEM CORE...")

    # 1. Startup Checks
    if not await redis_client.exists("CONFIG:ISIN:NSE"):
        logger.info("Redis empty. Performing Startup ISIN Lookup...")
        try:
            await ISINLookupService().run()
            logger.info("Startup ISIN Lookup Complete.")
        except Exception as e:
            logger.error(f"Startup ISIN Failed: {e}")
    else:
        logger.info("ISIN Data found in Redis. Skipping startup sync.")

    start_scheduler()

    # 2. Initialize Engines
    poller = RSSEventFetcher()
    ev_filter = EventFilter()
    ai_engine = AIEngine()
    tech_engine = TechnicalEngine()


    async def safe_run(name, func):
        logger.info(f"{name} Starting...")
        while True:
            try:
                await func()
            except asyncio.CancelledError:
                logger.info(f"{name} Stopped.")
                break
            except Exception as e:
                logger.error(f"CRASH in {name}: {e}. Restarting in 5s...")
                await asyncio.sleep(5)

    tasks = [
        asyncio.create_task(safe_run("Poller", poller.run_loop)), 
        asyncio.create_task(safe_run("Filter", ev_filter.run)), 
        asyncio.create_task(safe_run("AI_Engine", ai_engine.run)), 
        asyncio.create_task(safe_run("Tech_Engine", tech_engine.run)), 
        asyncio.create_task(safe_run("FCM_Publisher", run_fcm)) 
    ]

    logger.info(f"All {len(tasks)} Engines Running in Safe Mode.")

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

        # Wait for graceful shutdown
        await asyncio.gather(*tasks, return_exceptions=True)

        # Close connections
        if hasattr(poller, 'close'):
            await poller.close()

        logger.info("System Shutdown Complete.")


def run_async_loop_in_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(start_async_core())
    finally:
        loop.close()


if __name__ == "__main__":
    try:
        t = threading.Thread(target=run_async_loop_in_thread, daemon=True)
        t.start()
        run_web_server()

    except KeyboardInterrupt:
        logger.info("Manual Stop Triggered")