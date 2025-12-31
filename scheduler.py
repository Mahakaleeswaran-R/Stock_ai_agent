import logging
from datetime import datetime, timedelta
import pytz

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from analyser.performance_analyser import PerfromanceAnalyser
from config import redis_client, raw_events, ai_audit, technical_audit
from jobs.ISIN_lookup import ISINLookupService
from jobs.feedback_layer import run as run_feedback_job

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("SCHEDULER")

IST = pytz.timezone("Asia/Kolkata")
_scheduler: AsyncIOScheduler | None = None

async def job_data_cleanup():
    logger.info("STARTING NIGHTLY STORAGE PURGE")
    try:
        cutoff = datetime.now(IST) - timedelta(days=3)

        res_events = await raw_events.delete_many({
            "ingestion_ts": {"$lt": cutoff.isoformat()}
        })
        logger.info(f"Purged {res_events.deleted_count} raw events")

        res_ai = await ai_audit.delete_many({
            "timestamp": {"$lt": cutoff}
        })
        logger.info(f"Purged {res_ai.deleted_count} AI audit logs")

        res_tech = await technical_audit.delete_many({
            "timestamp": {"$lt": cutoff}
        })
        logger.info(f"Purged {res_tech.deleted_count} technical logs")

        # Nightly reset
        await redis_client.delete(
            "RISK:OPEN_TRADES_COUNT",
            "RISK:LOSS_STREAK"
        )

        queues = [
            "QUEUE:NORMALIZED_EVENTS",
            "QUEUE:FILTERED_EVENTS",
            "QUEUE:AI_SIGNALS",
            "QUEUE:TRADE_SIGNALS"
        ]

        for q in queues:
            q_len = await redis_client.llen(q)
            if q_len > 2000:
                logger.warning(f"{q} overflow ({q_len}) → trimming")
                await redis_client.ltrim(q, -2000, -1)

        logger.info("Nightly cleanup completed")

    except Exception as e:
        logger.error(f"Cleanup job failed: {e}", exc_info=True)


async def job_isin_update():
    logger.info("REFRESHING ISIN MAP")
    try:
        await ISINLookupService().run()
        logger.info("ISIN sync completed")
    except Exception as e:
        logger.error(f"ISIN job failed: {e}", exc_info=True)


async def job_performance_analyser():
    logger.info("Running performance analyzer")
    try:
        service = PerfromanceAnalyser()
        await service.run()
        logger.info("Performance analysis completed")
    except Exception as e:
        logger.error(f"Performance analyzer failed: {e}", exc_info=True)


def start_scheduler() -> AsyncIOScheduler:
    global _scheduler

    if _scheduler and _scheduler.running:
        logger.warning("Scheduler already running — skipping restart")
        return _scheduler

    scheduler = AsyncIOScheduler(timezone=IST)

    scheduler.add_job(
        job_isin_update,
        CronTrigger(hour=8, minute=30, timezone=IST),
        id="isin_sync",
        max_instances=1,
        replace_existing=True,
    )

    scheduler.add_job(
        job_performance_analyser,
        CronTrigger(hour=1, minute=00, timezone=IST),
        id="performance_analysis",
        max_instances=1,
        replace_existing=True,
    )

    scheduler.add_job(
        run_feedback_job,
        CronTrigger(hour=20, minute=0, timezone=IST),
        id="feedback_layer",
        max_instances=1,
        replace_existing=True,
    )

    scheduler.add_job(
        job_data_cleanup,
        CronTrigger(hour=2, minute=30, timezone=IST),
        id="cleanup",
        max_instances=1,
        replace_existing=True,
    )

    scheduler.start()
    _scheduler = scheduler

    logger.info("Scheduler started successfully")
    return scheduler
