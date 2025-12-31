import asyncio
import aiohttp
import feedparser
import hashlib
import json
import logging
import time
from datetime import datetime, timezone, time as dtime
from collections import OrderedDict
import pytz
import re
import calendar
from config import redis_client
from utils.utility import normalize_company_name

IST = pytz.timezone("Asia/Kolkata")

POLL_INTERVAL_SECONDS = 10
REDIS_EXPIRY = 86400  # 1 day

RSS_SOURCES = {
    "BSE": "https://www.bseindia.com/data/xml/announcements.xml",
    "NSE": "https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/xml"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("EVENT_POLLER")

def classify_market_phase(ts_ist: datetime) -> str:
    t = ts_ist.time()
    if ts_ist.weekday() >= 5:
        return "WEEKEND"
    if t < dtime(9, 0):
        return "PRE_MARKET"
    if dtime(9, 0) <= t < dtime(9, 15):
        return "AUCTION"
    if dtime(9, 15) <= t <= dtime(15, 30):
        return "LIVE"
    return "POST_MARKET"


def trade_eligibility_from_phase(phase: str) -> str:
    if phase in ("PRE_MARKET", "AUCTION", "POST_MARKET", "WEEKEND"):
        return "NEXT_SESSION_ONLY"
    return "IMMEDIATE"


def generate_event_id(source, join_key, title, doc_urls):
    base = f"{source}|{join_key}|{title}"
    h = hashlib.md5(base.encode()).hexdigest()[:10]

    if doc_urls:
        doc_hash = hashlib.md5("".join(doc_urls).encode()).hexdigest()[:6]
        return f"{source}_{h}_{doc_hash}"

    return f"{source}_{h}"

def parse_entry(source, entry):
    try:
        ingestion_ts = datetime.now(timezone.utc)

        raw_title = entry.get("title", "").strip()
        summary = entry.get("summary", "") or entry.get("description", "")
        if not raw_title:
            return None

        text = f"{raw_title} {summary}".upper()
        noise_keywords = [
            # --- MUTUAL FUNDS & DEBT (Specific) ---
            "MUTUAL FUND", "NAV ", "NET ASSET VALUE",
            " MF ",
            "FIXED MATURITY PLAN", " FMP ",
            " IDCW ", "FUND OF FUNDS", "INDEX FUND",
            "EXCHANGE TRADED FUND", " ETF",  # Catches 'Gold ETF', 'Nifty ETF'
            "NON-CONVERTIBLE DEBENTURES", " NCD ", "COMMERCIAL PAPER",
            "INTEREST PAYMENT", "REDEMPTION OF",
            "PORTFOLIO MANAGEMENT",

            # --- ROUTINE COMPLIANCE ---
            "LOSS OF SHARE", "LOST OF SHARE", "DUPLICATE SHARE",
            "CLOSURE OF TRADING", "TRADING WINDOW", "WINDOW CLOSURE",
            "INVESTOR GRIEVANCE", "COMPLIANCE CERTIFICATE",
            "NEWSPAPER PUBLICATION", "POSTAL BALLOT", "E-VOTING",
            "REGULATION 74", "REGULATION 76", "REGULATION 40",
            "TRANSCRIPT OF", "AUDIO RECORDING",
            "CLARIFICATION ON SPURT", "CLARIFICATION ON PRICE",
            "MOVEMENT IN PRICE", "MOVEMENT IN VOLUME",

            # --- SAFE BLOCKS ---
            "DIRECT PLAN", "REGULAR PLAN", "BENEFIT PLAN",  # Fixed Comma Here

            # --- DEBT (Safe Filters Only) ---
            "ISSUANCE OF DEBT",  # Blocks raising generic debt (usually routine)
            "DEBT SECURITIES",  # Blocks Bond trading noise
            "DEBT INSTRUMENT",
            "DEBT_INSTRUMENT",# Blocks Bond market noise
            "SERVICING OF DEBT",  # Routine interest payments

            # --- MISSING MF VARIATIONS (From your logs) ---
            "MUTUAL F",
            "FIXED TERM PLAN",
            " FTP ",  # Short for Fixed Term Plan
            "INTERVAL INCOME FUND",  # Catches "INTERVAL INCOME FUND"
            "RESURGENT INDIA FUND",  # Catches the specific scheme in your logs
            "DUAL ADVANTAGE FUND",  # Catches the "Series 2" fund in your logs

            # --- GENERIC MF TERMS (Safe to block) ---
            "GROWTH OPTION",  # MF Terminology (Safe: Companies don't call growth "Growth Option")
            "DIVIDEND PAYOUT",  # MF Terminology
            "DIVIDEND SWEEP",  # MF Terminology
            "DIVIDEND REINVESTMENT",  # MF Terminology

            # --- DEBT/BONDS (Safe filters) ---
            "ISSUANCE OF DEBT",  # Blocks raising debt (Routine)
            "DEBT SECURITIES",  # Blocks Bond trading
            "DEBT INSTRUMENT",  # Blocks Bond market
            "SERVICING OF DEBT"
        ]
        if any(k in text for k in noise_keywords):
            return None

        clean_name = normalize_company_name(raw_title)
        join_key = clean_name

        published = entry.get("published_parsed")
        disclosure_ts = None

        # 1. Try standard RSS date
        if published:
            try:
                disclosure_ts = datetime.fromtimestamp(calendar.timegm(published), tz=timezone.utc)
            except Exception:
                pass

        if not disclosure_ts:
            try:
                text_search = f"{summary} {raw_title}"
                match_dt = re.search(r"(\d{1,2}-[A-Za-z]{3}-\d{4}\s+\d{1,2}:\d{1,2}:\d{1,2})", text_search)

                if match_dt:
                    dt_str = match_dt.group(1)
                    # Parse assuming IST
                    dt_obj = datetime.strptime(dt_str, "%d-%b-%Y %H:%M:%S")
                    disclosure_ts = IST.localize(dt_obj).astimezone(timezone.utc)
            except Exception:
                pass

        if not disclosure_ts:
            disclosure_ts = ingestion_ts


        disclosure_ts_ist = disclosure_ts.astimezone(IST)
        ingestion_ts_ist = ingestion_ts.astimezone(IST)

        latency = int((ingestion_ts - disclosure_ts).total_seconds())
        latency = max(0, min(latency, 86400))

        market_phase = classify_market_phase(ingestion_ts_ist)
        disclosure_phase = classify_market_phase(disclosure_ts_ist)

        urgency = "LOW"
        if market_phase == "LIVE":
            if ingestion_ts_ist.time() < dtime(11, 0):
                urgency = "HIGH"
            elif ingestion_ts_ist.time() < dtime(14, 30):
                urgency = "MEDIUM"
            if disclosure_phase == "LIVE" and latency < 300:
                urgency = "EXTREME"

        price_context = "IGNORE"
        if market_phase == "LIVE":
            price_context = "CHECK_REQUIRED"
        elif market_phase == "PRE_MARKET":
            price_context = "GAP_CHECK"

        doc_urls = []
        if entry.get("links"):
            for l in entry["links"]:
                href = l.get("href", "")
                if href.lower().endswith((".pdf", ".zip", ".xml")):
                    doc_urls.append(href)
        elif entry.get("link"):
            doc_urls.append(entry["link"])

        event_id = generate_event_id(
            source, join_key, raw_title, doc_urls
        )

        return {
            "source": source,
            "event_id": event_id,
            "raw_name": raw_title,
            "clean_name": clean_name,
            "join_key": join_key,
            "title": raw_title.upper(),
            "summary": summary.upper(),
            "pdf_url": doc_urls,
            "disclosure_ts": disclosure_ts.isoformat(),
            "ingestion_ts": ingestion_ts.isoformat(),
            "latency_seconds": latency,
            "market_phase": market_phase,
            "disclosure_market_phase": disclosure_phase,
            "trade_eligibility": trade_eligibility_from_phase(market_phase),
            "event_urgency": urgency,
            "price_context": price_context,
            "status": "RAW"
        }

    except Exception as e:
        logger.error(f"Parse error: {e}", exc_info=True)
        return None


class RSSEventFetcher:
    def __init__(self):
        self.output_queue = "QUEUE:NORMALIZED_EVENTS"
        self.local_seen = OrderedDict()
        self.MAX_CACHE = 8000

    async def fetch(self, session, source, url):
        try:
            headers = {}
            last_mod = await redis_client.get(f"POLLER:LAST_MOD:{source}")
            if last_mod:
                headers["If-Modified-Since"] = last_mod
            else:
                logger.info(f"[{source}] Polling (No previous Last-Mod found)")

            content = None

            for attempt in range(2):
                try:
                    async with session.get(url, headers=headers) as resp:
                        # LOGGING POINT 2: Verify the Server Response
                        if resp.status == 304:
                            logger.info(f"[{source}] HTTP 304: No new data (Skipping parse)")
                            return

                        if resp.status == 200:
                            logger.info(f"[{source}] HTTP 200: New data received. Downloading...")
                        else:
                            logger.warning(f"[{source}] Unexpected Status: {resp.status}")
                            if resp.status >= 500:
                                raise aiohttp.ClientError()
                            return

                        lm = resp.headers.get("Last-Modified")
                        if lm:
                            await redis_client.set(f"POLLER:LAST_MOD:{source}", lm)

                        content = await resp.read()
                        break
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    if attempt == 1:
                        logger.warning(f"[{source}] Fetch failed (Timeout/Connection).")
                        return
                    await asyncio.sleep(1)

            if not content:
                return

            if source == "BSE":
                hash_key = f"POLLER:HASH:{source}:{url}"
                content_hash = hashlib.md5(content).hexdigest()
                last_hash = await redis_client.get(hash_key)
                if last_hash and last_hash == content_hash:
                    logger.info(f"[{source}] Content Hash matches previous pull. (Skipping parse)")
                    return

                logger.info(f"[{source}] Content Hash changed. Processing feed...")
                await redis_client.set(hash_key, content_hash)

            feed = await asyncio.to_thread(feedparser.parse, content)

            new_count = 0
            for entry in reversed(feed.entries):
                evt = parse_entry(source, entry)
                if not evt:
                    continue

                key = evt["event_id"]

                if key in self.local_seen:
                    continue

                is_new = await redis_client.set(
                    f"POLLER:SEEN:{key}", "1", ex=REDIS_EXPIRY, nx=True
                )
                if not is_new:
                    continue

                self.local_seen[key] = time.time()
                await redis_client.rpush(self.output_queue, json.dumps(evt))

                logger.info(f"New Event [{source}]: {evt['clean_name']}")
                new_count += 1

                if len(self.local_seen) > self.MAX_CACHE:
                    self.local_seen.popitem(last=False)

            if new_count == 0 and source == "NSE":
                logger.info(f"[{source}] Parsed feed but found 0 new relevant events.")

        except Exception as e:
            logger.error(f"Fetch Loop Error {source}: {e}", exc_info=True)

    async def run(self):
        logger.info("Event Poller Started")
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout) as session:
            while True:
                tasks = [
                    self.fetch(session, src, url)
                    for src, url in RSS_SOURCES.items()
                ]
                await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        asyncio.run(RSSEventFetcher().run())
    except KeyboardInterrupt:
        logger.info("Poller stopped")
