import difflib
import json
import logging
import asyncio
import re
import pytz
from bson import ObjectId
from datetime import datetime, timezone, timedelta
from config import redis_client, raw_events

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("EVENT_FILTER")

IST = pytz.timezone("Asia/Kolkata")

CACHED_BAD_KEYWORDS = set()
CACHED_SHADOW_KEYWORDS = set()
LAST_KEYWORD_REFRESH = datetime.now(timezone.utc) - timedelta(days=1)

KEYWORD_REFRESH_SECONDS = 6000
SIMILARITY_CACHE_TTL = 6 * 3600  # 6 hours

SEQUEL_KEYWORDS = [
    "FURTHER", "ADDITIONAL", "CLARIFICATION", "DETAILS",
    "UPDATE", "CORRIGENDUM", "OUTCOME", "REVISED", "PRESS RELEASE"
]

CORP_ACTION_WHITELIST = [
    "RESIGNATION", "APPOINTMENT", "CHANGE IN DIRECTOR",
    "KEY MANAGERIAL PERSONNEL", "AUDITOR", "COMPLIANCE OFFICER"
]


def json_serial(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


async def _refresh_keywords_if_needed():
    global CACHED_BAD_KEYWORDS, CACHED_SHADOW_KEYWORDS, LAST_KEYWORD_REFRESH

    now = datetime.now(timezone.utc)
    if (now - LAST_KEYWORD_REFRESH).total_seconds() < KEYWORD_REFRESH_SECONDS:
        return

    try:
        bad = await redis_client.smembers("CONFIG:BAD_KEYWORDS")
        shadow = await redis_client.smembers("CONFIG:SHADOW_KEYWORDS")

        CACHED_BAD_KEYWORDS = {
            (k.decode() if isinstance(k, bytes) else k).upper() for k in bad
        }
        CACHED_SHADOW_KEYWORDS = {
            (k.decode() if isinstance(k, bytes) else k).upper() for k in shadow
        }
        LAST_KEYWORD_REFRESH = now
    except Exception as e:
        logger.warning(f"Keyword refresh failed: {e}")


async def _get_isin(join_key, source):
    try:
        key = f"CONFIG:ISIN:{source}"
        val = await redis_client.hget(key, join_key)
        if not val:
            return None
        return val.decode() if isinstance(val, bytes) else val
    except Exception:
        return None


def classify_event_type(title: str) -> str:
    t = title.upper()
    if "RESULT" in t or "FINANCIAL" in t:
        return "RESULT"
    if "ORDER" in t or "CONTRACT" in t:
        return "ORDER"
    if "RESIGNATION" in t or "AUDITOR" in t:
        return "GOVERNANCE"
    if "SEBI" in t or "RAID" in t or "REGULATOR" in t:
        return "REGULATORY"
    return "GENERAL"


async def _update_last_event_cache(key, event):
    payload = {
        "title": event["title"],
        "summary": event.get("summary", ""),
        "timestamp": event["event_ts"],
        "source": event["source"]
    }
    await redis_client.setex(key, SIMILARITY_CACHE_TTL, json.dumps(payload))

async def _check_smart_similarity(isin, event):
    # 1. Define Cache Key (Prefer ISIN, Fallback to Name)
    if isin:
        cache_key = f"CACHE:LAST_EVENT:{isin}"
    else:
        # Remove spaces for tighter matching (e.g. "Tata Motors" == "TataMotors")
        clean = event["clean_name"].replace(" ", "").upper()
        cache_key = f"CACHE:LAST_EVENT:NAME:{clean}"

    cached = await redis_client.get(cache_key)
    title = event["title"].upper()

    # 2. Allow "Sequels" to pass
    if any(title.startswith(k) for k in SEQUEL_KEYWORDS):
        await _update_last_event_cache(cache_key, event)
        return False

    if not cached:
        await _update_last_event_cache(cache_key, event)
        return False

    last = json.loads(cached)

    # 3. Time Window Check (> 20 mins = New Event)
    try:
        last_ts = datetime.fromisoformat(last["timestamp"])
        curr_ts = datetime.fromisoformat(event["event_ts"])
        if abs((curr_ts - last_ts).total_seconds()) > 1200:
            await _update_last_event_cache(cache_key, event)
            return False
    except Exception:
        pass

    # 4. Text Similarity
    def clean_text(t):
        return re.sub(r"[^A-Z0-9]", "", t.upper())

    a = clean_text(last["title"] + last.get("summary", ""))
    b = clean_text(event["title"] + event.get("summary", ""))

    ratio = difflib.SequenceMatcher(None, a, b).ratio()

    if last["source"] == event["source"]:
        is_dupe = ratio > 0.90
    else:
        is_dupe = ratio > 0.65

    if is_dupe:
        logger.info(f"Duplicate blocked [{ratio:.2f}] {event['clean_name']}")
        return True

    await _update_last_event_cache(cache_key, event)
    return False


async def _is_noise(event):
    await _refresh_keywords_if_needed()

    name = event["clean_name"].upper()
    title = event["title"].upper()
    summary = (event.get("summary") or "").upper()

    if any(x in name for x in ["MUTUAL FUND", "ETF", "BOND", "NIFTY", "SENSEX", "FMP ", "DEBENTURE"]):
        return True, "NON_EQUITY", None

    shadow_violation = None

    for kw in CACHED_BAD_KEYWORDS:
        if kw in title or kw in summary:
            if any(w in title for w in CORP_ACTION_WHITELIST):
                shadow_violation = kw
                break
            return True, f"BLOCKED:{kw}", None

    if not shadow_violation:
        for kw in CACHED_SHADOW_KEYWORDS:
            if kw in title or kw in summary:
                shadow_violation = kw
                break

    return False, None, shadow_violation


class EventFilter:
    def __init__(self):
        self.input_queue = "QUEUE:NORMALIZED_EVENTS"
        self.output_queue = "QUEUE:FILTERED_EVENTS"

    async def process_event(self, raw):
        try:
            event = json.loads(raw)

            # --- 1. FILTER CHECK ---
            junk, reason, shadow = await _is_noise(event)

            if junk:
                event["status"] = "REJECTED"
                event["rejection_reason"] = reason

                # CRITICAL FIX: Save to DB for Feedback Layer, then STOP
                await raw_events.insert_one(event)
                logger.info(f"Filtered {event['clean_name']} | {reason}")
                return  # <--- THIS RETURN WAS MISSING

            # --- 2. ENRICHMENT ---
            event["status"] = "ACCEPTED"
            if shadow:
                event["shadow_violation"] = shadow

            # Metadata
            event["event_ts"] = event["ingestion_ts"]
            latency = event.get("latency_seconds", 0)
            disclosure_phase = event.get("disclosure_market_phase")
            event["late_news_risk"] = (latency > 1800 and disclosure_phase == "LIVE")
            if latency == 0 and disclosure_phase == "LIVE":
                event["late_news_risk"] = True

            event["event_type"] = classify_event_type(event["title"])
            event["filtered_at"] = datetime.now(timezone.utc).isoformat()
            # ISIN Lookup
            event["isin"] = await _get_isin(event["join_key"], event["source"])

            event["timestamp"] = datetime.now(timezone.utc).isoformat()
            event["urgency"] = (
                "HIGH" if event.get("event_urgency") == "EXTREME" and not event.get("late_news_risk") else "LOW")

            # --- 3. DEDUPLICATION ---
            if await _check_smart_similarity(event["isin"], event):
                return

            # --- 4. SUCCESS: SAVE & PUSH ---
            await raw_events.insert_one(event)

            if "_id" in event:
                event["_id"] = str(event["_id"])

            await redis_client.rpush(
                self.output_queue,
                json.dumps(event, default=json_serial)
            )

            logger.info(f"Accepted {event['clean_name']}")

        except Exception as e:
            logger.error(f"Filter error: {e}", exc_info=True)

    async def run(self):
        logger.info("Event Filter Started")
        while True:
            item = await redis_client.blpop(self.input_queue, timeout=60)
            if item:
                await self.process_event(item[1])
            else:
                await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(EventFilter().run())