import asyncio
import logging
import os
import threading
from flask import Flask

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
