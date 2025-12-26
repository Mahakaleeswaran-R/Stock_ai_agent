import asyncio
import logging
import signal as sgnl
from datetime import datetime, timedelta
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from analyser.performance_analyser import PerformanceAnalyzer
from config import redis_client, raw_events, ai_audit, technical_audit

from jobs.ISIN_lookup import ISINLookupService
from jobs.feedback_layer import run as run_feedback_job

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger("SCHEDULER")
IST = pytz.timezone('Asia/Kolkata')


async def job_data_cleanup():
    logger.info("STARTING NIGHTLY STORAGE PURGE...")
    try:
        retention_cutoff = datetime.now() - timedelta(hours=7)
        res_events = await raw_events.delete_many({
            "timestamp": {"$lt": retention_cutoff.isoformat()}
        })
        logger.info(f"Wiped {res_events.deleted_count} old Raw Events.")

        res_ai = await ai_audit.delete_many({
            "timestamp": {"$lt": retention_cutoff}
        })
        logger.info(f"Wiped {res_ai.deleted_count} old AI Audit logs.")

        res_tech = await technical_audit.delete_many({
            "timestamp": {"$lt": retention_cutoff}
        })
        logger.info(f"Wiped {res_tech.deleted_count} old Technical Audit logs.")
        queues = ["QUEUE:NORMALIZED_EVENTS", "QUEUE:FILTERED_EVENTS", "QUEUE:AI_SIGNALS", "QUEUE:TRADE_SIGNALS"]
        for q in queues:
            q_len = await redis_client.llen(q)
            if q_len > 2000:
                logger.warning(f"{q} overflowing ({q_len}). Trimming to last 2000.")
                await redis_client.ltrim(q, -2000, -1)
        logger.info("Daily Purge Complete. Storage reclaimed.")
    except Exception as e:
        logger.error(f"Cleanup Failed: {e}")


async def job_isin_update():
    logger.info("REFRESHING ISIN MAP (Market Prep)...")
    try:
        service = ISINLookupService()
        await service.run()
        logger.info("ISIN Sync Completed")
    except Exception as e:
        logger.error(f"ISIN Job Failed: {e}")

async def job_performance_analyser():
    logger.info("Analysing performance data...")
    try:
        service = PerformanceAnalyzer()
        await service.run()
        logger.info("performance analysis Job Completed")
    except Exception as e:
        logger.error(f"performance analysis Job: {e}")


def start_scheduler():
    scheduler = AsyncIOScheduler(timezone=IST)

    # 1. ISIN Sync: 08:30 AM
    scheduler.add_job(job_isin_update, CronTrigger(hour=8, minute=30))

    # 2. Performance Analyzer: 04:30 PM
    scheduler.add_job(job_performance_analyser, CronTrigger(hour=16, minute=30))

    # 3. Feedback Layer: 08:00 PM
    scheduler.add_job(run_feedback_job, CronTrigger(hour=20, minute=0))

    # 4. Cleanup: 11:30 PM
    scheduler.add_job(job_data_cleanup, CronTrigger(hour=23, minute=30))

    scheduler.start()
    logger.info("Scheduler Active")
    return scheduler