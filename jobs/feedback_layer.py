import asyncio
import logging
import json
import re
from datetime import datetime, timedelta
from google import genai
from google.genai.types import GenerateContentResponse

from config import redis_client, raw_events, GEMINI_API_KEY

API_KEY = GEMINI_API_KEY
if not API_KEY:
    raise ValueError("GEMINI api key environment variable not set!")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("FEEDBACK_LAYER")

client = genai.Client(api_key=API_KEY)

RATE_LIMIT_DELAY = 5
BATCH_SIZE = 50
MODEL_NAME = "gemma-3-27b-it"

async def fetch_events_generator(status, hours=24):
    cutoff = datetime.now() - timedelta(hours=hours)
    total_count = await raw_events.count_documents({
        "status": status,
        "timestamp": {"$gte": cutoff.isoformat()}
    })
    logger.info(f"Found {total_count} total '{status}' events to process.")
    if total_count == 0:
        return
    cursor = raw_events.find({
        "status": status,
        "timestamp": {"$gte": cutoff.isoformat()}
    })

    current_batch = []
    async for document in cursor:
        current_batch.append(document)
        if len(current_batch) == BATCH_SIZE:
            yield current_batch
            current_batch = []

    if current_batch:
        yield current_batch


def clean_and_parse_json(text):
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r'```json\s*(.*?)\s*```', text, re.DOTALL)
        if match:
            text = match.group(1)
        else:
            match = re.search(r'\[.*\]', text, re.DOTALL)
            if match:
                text = match.group(0)
        try:
            return json.loads(text)
        except:
            return []

async def analyze_shadow_performance():
    logger.info("Starting Shadow Validation")
    query = {"shadow_violation": {"$exists": True}}
    async for batch in fetch_events_generator(query):
        logger.info(f"Verifying {len(batch)} Shadow Hits...")
        event_text = "\n".join([f"- {e['clean_name']}: {e.get('title', '')}" for e in batch])
        prompt = f"""
        You are a Filter Validation Judge.
        We are testing new keywords in "Shadow Mode". These keywords FLAGGED the events below as "Junk".

        Your Job: Verify if these keywords were CORRECT.

        **CRITERIA:**
        - **CORRECT FLAG (Junk):** Debt, Bonds, MF, ETF, Routine Compliance, Credit Ratings.
        - **INCORRECT FLAG (Valid):** Earnings, Dividends, Mergers, Orders, Projects, CEO Changes.

        **EVENTS FLAGGED BY SHADOW KEYWORDS:**
        {event_text}

        **OUTPUT:**
        Return a JSON list of Event Titles that are **DEFINITELY JUNK**. 
        (If an event is NOT in your return list, we assume it was Valid News and the keyword failed).
        """
        try:
            response = await call_ai(prompt)
            confirmed_junk_titles = clean_and_parse_json(response.text)
            for event in batch:
                keyword = event['shadow_violation']
                is_actually_junk = event.get('title') in confirmed_junk_titles
                if is_actually_junk:
                    new_score = await redis_client.hincrby("CONFIG:SHADOW_SCORES", keyword, 1)
                    logger.info(f"Shadow '{keyword}' +1 Score (Total: {new_score}/3)")
                    if new_score >= 3:
                        logger.info(f"PROMOTING '{keyword}' to LIVE BLOCKLIST!")
                        await redis_client.smove("CONFIG:SHADOW_KEYWORDS", "CONFIG:BAD_KEYWORDS", keyword)
                        await redis_client.hdel("CONFIG:SHADOW_SCORES", keyword)
                else:
                    logger.warning(f"Shadow '{keyword}' flagged Valid News! DELETING.")
                    await redis_client.srem("CONFIG:SHADOW_KEYWORDS", keyword)
                    await redis_client.hdel("CONFIG:SHADOW_SCORES", keyword)
            await asyncio.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            logger.error(f"Shadow Validation Failed: {e}")
            await asyncio.sleep(RATE_LIMIT_DELAY)


async def call_ai(prompt: str):
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=prompt
            )
            return response
        except Exception as e:
            error_str = str(e)
            if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                logger.warning(f"Quota Exceeded. Sleeping 60s (Attempt {attempt + 1}/{max_attempts})...")
                await asyncio.sleep(60)
            else:
                logger.error(f"AI Error: {e}")
    logger.error("Failed after max retries.")
    return None


async def handle_false_negative_strikes(valid_titles, batch_events):
    for title in valid_titles:
        event = next((e for e in batch_events if e.get('title') == title), None)
        if event and 'rejection_reason' in event and event['rejection_reason'].startswith("BLOCKED:"):
            bad_keyword = event['rejection_reason'].split(":")[1].strip().upper()
            logger.critical(f"LIVE FAILURE: '{bad_keyword}' blocked valid news: '{title}'")
            await redis_client.srem("CONFIG:BAD_KEYWORDS", bad_keyword)
            await redis_client.sadd("CONFIG:KEYWORD_WHITELIST", bad_keyword)
            logger.info(f"Granted Immunity to '{bad_keyword}'")

async def analyze_leaks():
    logger.info("Starting Leak Analysis (Accepted Events)")
    async for batch in fetch_events_generator("ACCEPTED"):
        logger.info(f"Analyzing batch of {len(batch)} events")
        event_text = "\n".join([f"- {e['clean_name']}: {e.get('title', '')} | {e.get('summary', '')}" for e in batch])
        prompt = f"""
        You are a Senior Market Data Analyst for an Algorithmic Equity Trading Bot.
        Your goal is to identify "Noise" events that slipped through our filters.

        **STRICT GUIDELINES:**
        1. **Target:** Identify events related to **Debt Instruments**, **Mutual Funds**, **Government Securities**, or **Routine Clerical Compliance**.
        2. **Protection:** Do NOT flag events related to **Earnings, Board Meetings, Dividends, Buybacks, Mergers, Orders/Contracts, or Management Changes**. These are valid equity news.

        **CATEGORIES OF NOISE (Block These):**
        - Debt: "Non-Convertible Debentures", "NCD", "Bonds", "Commercial Paper", "Interest Payment".
        - Funds: "Mutual Fund", "ETF", "Fixed Maturity Plan", "NAV", "Scheme".
        - Compliance: "Trading Window Closure", "Loss of Share Certificate", "Duplicate Share Certificate", "Credit Rating".

        **INSTRUCTIONS:**
        Analyze the list below. If an event is NOISE, extract a **discriminative phrase** (2-4 words) that identifies it as junk. 
        - Bad Keyword: "SERIES" (Too broad).
        - Good Keyword: "BOND SERIES" (Specific).

        **EVENTS LIST:**
        {event_text}

        **OUTPUT:**
        Return ONLY a JSON list of unique uppercase strings. Example: ["LOSS OF SHARE", "DEBT SERIES"]
        """

        try:
            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=prompt
            )
            keywords = clean_and_parse_json(response.text)
            if keywords:
                logger.info(f"FOUND LEAKS: {keywords}")
                for kw in keywords:
                    clean_kw = kw.strip().upper()
                    if await redis_client.sismember("CONFIG:KEYWORD_WHITELIST", clean_kw):
                        logger.info(f"SKIPPING '{clean_kw}': It is Immune/Whitelisted.")
                        continue
                    if await redis_client.sismember("CONFIG:BAD_KEYWORDS", clean_kw):
                        continue
                    if len(clean_kw) > 3 and clean_kw not in ["PROFIT", "RESULTS", "DIVIDEND"]:
                        if await redis_client.sadd("CONFIG:SHADOW_KEYWORDS", clean_kw):
                            await redis_client.hset("CONFIG:SHADOW_SCORES", clean_kw, 0)
                            logger.info(f"Added '{clean_kw}' to Shadow Mode.")

            await asyncio.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            logger.error(f"Leak Batch Failed: {e}")
            await asyncio.sleep(RATE_LIMIT_DELAY)


async def analyze_filters():
    logger.info("Starting Filter Audit (Rejected Events)")
    async for batch in fetch_events_generator("REJECTED"):
        logger.info(f"Analyzing batch of {len(batch)} events...")
        event_text = "\n".join(
            [f"- {e['clean_name']} | Title: {e.get('title', '')} | Reason: {e.get('rejection_reason', '')}" for e in
             batch])

        prompt = f"""
        You are a Strategy Auditor.
        Your goal is to identify **High-Value Trading Opportunities** that were ACCIDENTALLY BLOCKED by our noise filter.

        **STRICT GUIDELINES:**
        1. **Target (The "Gold"):** We are looking for **Price Sensitive Information**.
           - Earnings / Financial Results (Q1, Q2, Q3, Q4)
           - Dividend / Bonus Issue / Stock Split / Buyback
           - Large Orders / Contracts / Project Wins
           - Mergers / Acquisitions
           - Resignation/Appointment of CEO/CFO/MD

        2. **Ignore (Correctly Blocked):** Do NOT flag these.
           - "Loss of Share Certificates" / "Duplicate Share Issue"
           - "Trading Window Closure" / "Investor Grievance"
           - "Credit Rating" (unless downgrade)
           - Debt/Bond related news

        **INSTRUCTIONS:**
        Review the BLOCKED events below. If an event looks like **"Target"** news, extract its Title.

        **BLOCKED EVENTS LIST:**
        {event_text}

        **OUTPUT:**
        Return ONLY a JSON list of strings (The Titles of the wrongly blocked events).
        """

        try:
            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=prompt
            )
            false_negatives = clean_and_parse_json(response.text)
            if false_negatives:
                await handle_false_negative_strikes(false_negatives, batch)
            await asyncio.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            logger.error(f"Filter Audit Batch Failed: {e}")
            await asyncio.sleep(RATE_LIMIT_DELAY)


async def run():
    logger.info("Nightly Feedback Job Starting")
    await analyze_leaks()
    await analyze_shadow_performance()
    await analyze_filters()
    logger.info("Job Complete.")