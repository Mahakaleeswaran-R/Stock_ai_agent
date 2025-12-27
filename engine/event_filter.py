import difflib
import json
import logging
import asyncio
import re
from datetime import datetime

from bson import ObjectId

from config import redis_client, raw_events

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("EVENT_FILTER")

CACHED_BAD_KEYWORDS = set()
CACHED_SHADOW_KEYWORDS = set()
LAST_KEYWORD_REFRESH = datetime.min


async def _refresh_keywords_if_needed():
    global CACHED_BAD_KEYWORDS, CACHED_SHADOW_KEYWORDS, LAST_KEYWORD_REFRESH
    now = datetime.now()
    if (now - LAST_KEYWORD_REFRESH).total_seconds() > 6000:  # 100 Mins
        try:
            bad = await redis_client.smembers("CONFIG:BAD_KEYWORDS")
            CACHED_BAD_KEYWORDS = {k.decode() if isinstance(k, bytes) else k for k in bad}

            shadow = await redis_client.smembers("CONFIG:SHADOW_KEYWORDS")
            CACHED_SHADOW_KEYWORDS = {k.decode() if isinstance(k, bytes) else k for k in shadow}

            LAST_KEYWORD_REFRESH = now
        except:
            pass


async def _get_isin(join_key, source):
    redis_key = f"CONFIG:ISIN:{source}"
    isin = await redis_client.hget(redis_key, join_key)
    if isin:
        return isin.decode('utf-8') if isinstance(isin, bytes) else isin
    return None

def json_serial(obj):
    if isinstance(obj, (datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


async def _check_smart_similarity(isin, current_event):
    last_event_key = f"CACHE:LAST_EVENT:{isin}"
    last_event_raw = await redis_client.get(last_event_key)

    if last_event_raw:
        last_event = json.loads(last_event_raw)
        last_ts = datetime.fromisoformat(last_event['timestamp'])
        curr_ts = datetime.fromisoformat(current_event['timestamp'])
        if abs((curr_ts - last_ts).total_seconds()) < 600:

            def get_numbers(text):
                return set(re.findall(r'\d+(?:\.\d+)?', text))


            def clean_text(text, company_name):
                t = text.upper()
                t = t.replace(company_name.upper(), "")
                fillers = [
                    "LIMITED", "LTD", "HAS INFORMED THE EXCHANGE", "REGARDING",
                    "SUB:", "SUBJECT:", "DISCLOSURE OF", "INTIMATION OF",
                    "PURSUANT TO", "REGULATION", "SEBI", "WE HEREBY SUBMIT"
                ]
                for f in fillers:
                    t = t.replace(f, "")
                return re.sub(r'[^A-Z0-9 ]', '', t).strip()
            clean_name = current_event['clean_name']
            str_a = f"{last_event['title']} {last_event.get('summary', '')}"
            str_b = f"{current_event['title']} {current_event.get('summary', '')}"
            norm_a = clean_text(str_a, clean_name)
            norm_b = clean_text(str_b, clean_name)

            nums_a = get_numbers(str_a)
            nums_b = get_numbers(str_b)
            if nums_a and nums_b and not nums_a.intersection(nums_b):
                return False

            ratio = difflib.SequenceMatcher(None, norm_a, norm_b).ratio()
            set_a = set(norm_a.split())
            set_b = set(norm_b.split())

            jaccard = 0.0
            if set_a or set_b:
                intersection = len(set_a.intersection(set_b))
                union = len(set_a.union(set_b))
                jaccard = intersection / union if union > 0 else 0

            is_dupe = False
            if last_event['source'] == current_event['source']:
                if ratio > 0.95 or jaccard > 0.85:
                    is_dupe = True
            else:
                if ratio > 0.75 or jaccard > 0.80:
                    is_dupe = True
            if is_dupe:
                logger.info(
                    f"Duplicate Blocked [R:{ratio:.2f}|J:{jaccard:.2f}]: {current_event['source']} vs {last_event['source']}")
                return True
    cache_data = {
        "title": current_event['title'],
        "summary": current_event.get('summary', ''),
        "timestamp": current_event['timestamp'],
        "source": current_event['source']
    }
    await redis_client.setex(last_event_key, 3600, json.dumps(cache_data))
    return False


async def _is_noise(event):
    await _refresh_keywords_if_needed()

    name = event['clean_name']
    title = (event.get('title') or "").upper()
    summary = (event.get('summary') or "").upper()

    # 1. Mutual Funds / ETFs check
    if any(x in name for x in ["MUTUAL FUND", "ETF", "BOND", "NIFTY 50", "SENSEX"]):
        return True, "NON_EQUITY", None

    # 2. BSE Debt Scrip Check (7, 8, 9 series)
    if event['source'] == 'BSE':
        scrip = str(event.get('scrip_code', ''))
        if scrip and (scrip.startswith('9') or scrip.startswith('8') or scrip.startswith('7')):
            return True, f"DEBT_INSTRUMENT ({scrip})", None

    # 3. Bad Keywords
    for kw in CACHED_BAD_KEYWORDS:
        keyword = kw.upper()
        if keyword in title or keyword in summary:
            return True, f"BLOCKED: {keyword}", None

    # SHADOW_KEYWORDS
    shadow_violation = None
    for kw in CACHED_SHADOW_KEYWORDS:
        keyword = kw.upper()
        if keyword in title or keyword in summary:
            shadow_violation = keyword
            break

    return False, None, shadow_violation


class EventFilter:
    def __init__(self):
        self.input_queue = "QUEUE:NORMALIZED_EVENTS"
        self.output_queue = "QUEUE:FILTERED_EVENTS"

    async def process_event(self, raw_event_json):
        try:
            event = json.loads(raw_event_json)

            is_junk, reason, shadow_violation = await _is_noise(event)
            if is_junk:
                logger.info(f"Filtered: {event['clean_name']} | {reason}")
                return

            if shadow_violation:
                event['shadow_violation'] = shadow_violation
                logger.info(f"Shadow Match: {event['clean_name']} (Allowed)")

            event['status'] = "ACCEPTED"
            event['filtered_at'] = datetime.now().isoformat()
            event['isin'] = await _get_isin(event['join_key'], event['source'])

            await raw_events.insert_one(event)

            if '_id' in event:
                event['_id'] = str(event['_id'])

            # Push to Redis
            await redis_client.rpush(self.output_queue, json.dumps(event, default=json_serial))
            logger.info(f"Passed: {event['clean_name']}")

        except Exception as e:
            logger.error(f"Filter Error: {e}")

    async def run(self):
        logger.info("Event Filter Active...")
        while True:
            item = await redis_client.blpop(self.input_queue, timeout=15)
            if item:
                await self.process_event(item[1])
            else:
                await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(EventFilter().run())