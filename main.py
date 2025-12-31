import asyncio
import logging
import os
import threading
import signal
from flask import Flask

from config import redis_client, raw_events, ai_audit, technical_audit, trade_signals
from producers.event_poller import RSSEventFetcher
from engine.event_filter import EventFilter
from engine.ai_engine import AIEngine
from engine.technical_engine import TechnicalEngine, SymbolStateMachine
from producers.fcm_publisher import run as run_fcm
from scheduler import start_scheduler
from jobs.ISIN_lookup import ISINLookupService
from utils.angel_one_bridge import angel_bridge

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("STOCK_AGENT")

app = Flask(__name__)

# USE THREADING EVENT
shutdown_flag = threading.Event()


@app.route("/")
def home():
    return "Stock Trading Agent is RUNNING", 200


@app.route("/health")
def health():
    if shutdown_flag.is_set():
        return "Shutting down", 503
    return "OK", 200


async def init_db_indexes():
    await raw_events.create_index([("status", 1), ("ingestion_ts", -1)])
    await raw_events.create_index("shadow_violation")
    await raw_events.create_index("event_id", unique=True)
    await ai_audit.create_index("event_id")
    await technical_audit.create_index("event_id")
    await trade_signals.create_index("trade_id")
    logger.info("MongoDB indexes initialized")


async def safe_run(name, coro):
    logger.info(f"{name} started")
    while not shutdown_flag.is_set():
        try:
            # Create a task for the coroutine
            task = asyncio.create_task(coro())

            # Wait for task OR shutdown flag
            while not task.done() and not shutdown_flag.is_set():
                await asyncio.sleep(1)

            if shutdown_flag.is_set():
                if not task.done():
                    task.cancel()
                break

            await task  # Re-raise exceptions if any

        except asyncio.CancelledError:
            logger.info(f"{name} stopped.")
            break
        except Exception as e:
            logger.error(f"{name} crashed: {e}. Restarting in 5s...")
            await asyncio.sleep(5)


async def start_async_core():
    logger.info("STARTING TRADING SYSTEM CORE")

    # 1. Dependency Check
    try:
        if not await redis_client.ping():
            raise RuntimeError("Redis unavailable")
    except Exception as e:
        logger.critical(f"Redis Connection Failed: {e}")
        os._exit(1)  # Force exit if Redis is dead

    # 2. Initialize Bridge & Data
    await angel_bridge.initialize()

    if not await redis_client.exists("CONFIG:ISIN:NSE"):
        logger.info("ISIN Map missing. Running Sync Job...")
        await ISINLookupService().run()

    start_scheduler()
    await init_db_indexes()
    await SymbolStateMachine.clear_all_locks()

    # 3. Start Engines
    poller = RSSEventFetcher()
    ev_filter = EventFilter()
    ai_engine = AIEngine()
    tech_engine = TechnicalEngine()

    # Create distinct tasks
    tasks = [
        asyncio.create_task(safe_run("Poller", poller.run)),
        asyncio.create_task(safe_run("Filter", ev_filter.run)),
        asyncio.create_task(safe_run("AI_Engine", ai_engine.run)),
        asyncio.create_task(safe_run("Tech_Engine", tech_engine.run)),
        asyncio.create_task(safe_run("FCM_Publisher", run_fcm)),
    ]

    logger.info("SYSTEM ONLINE: All engines running.")

    # 4. Keep Loop Alive until Shutdown
    while not shutdown_flag.is_set():
        await asyncio.sleep(1)

    # 5. Graceful Shutdown Sequence
    logger.info("Initiating Graceful Shutdown...")

    for t in tasks:
        t.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    if hasattr(poller, "close"):
        await ai_engine.close()

    logger.info("Async core shutdown complete.")


def run_async_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(start_async_core())
    finally:
        loop.close()
        logger.info("Event Loop Closed")


def handle_shutdown(signum, frame):
    logger.warning(f"Received Signal {signum}. Stopping System...")
    shutdown_flag.set()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Start Async Core in Background Thread
    bg_thread = threading.Thread(target=run_async_loop, daemon=True)
    bg_thread.start()

    # Run Flask in Main Thread
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)