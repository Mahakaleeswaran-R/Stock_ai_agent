import hashlib
import aiohttp
import feedparser
import logging
import asyncio
import re
import json
from collections import OrderedDict
from datetime import datetime, timezone
from dateutil import parser as date_parser
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import pytz

from config import redis_client
from utils.utility import normalize_company_name

IST = pytz.timezone('Asia/Kolkata')

logger = logging.getLogger("EVENT_POLLER")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

POLL_INTERVAL_SECONDS = 10
REDIS_EXPIRY = 86400 * 1

RSS_SOURCES = {
    "BSE": "https://www.bseindia.com/data/xml/announcements.xml",
    "NSE": "https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/xml, text/xml, */*",
    "Referer": "https://www.bseindia.com/",
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache",
    "Expires": "0"
}


def _normalize_text(text):
    if not text: return ""
    return " ".join(text.strip().upper().split())


def _normalize_bse_title(raw_title):
    match = re.search(r"^(.*?)\s*\((\d{6})\)$", raw_title)
    if match:
        return match.group(1).strip(), match.group(2)
    if re.match(r"^\d{6}$", raw_title):
        return "UNKNOWN_BSE_SCRIP", raw_title
    return raw_title.strip(), None


def _parse_nse_description(description):
    if "|SUBJECT:" in description:
        parts = description.split("|SUBJECT:")
        return parts[0].strip(), parts[1].strip()
    return description.strip(), "General"


def is_relevant_stock(title, summary=""):
    if not title: return False
    junk_keywords = [
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
        "RATING REAFFIRMED", "RATING WITHDRAWN", "REVIEW OF RATING",
        "DIRECT PLAN", "REGULAR PLAN", "BENEFIT PLAN",  # Fixed Comma Here

        # --- DEBT (Safe Filters Only) ---
        "ISSUANCE OF DEBT",  # Blocks raising generic debt (usually routine)
        "DEBT SECURITIES",  # Blocks Bond trading noise
        "DEBT INSTRUMENT",  # Blocks Bond market noise
        "SERVICING OF DEBT",  # Routine interest payments

        # --- MISSING MF VARIATIONS (From your logs) ---
        "MUTUAL F",  # Catches "ADITYA BIRLA SUN LIFE MUTUAL F" (Truncated title)
        "FIXED TERM PLAN",  # Catches "BIRLA SUN LIFE FIXED TERM PLAN"
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
        "SERVICING OF DEBT",  # Routine interest payments
    ]
    text_to_check = (title + " " + summary).upper()
    if any(keyword in text_to_check for keyword in junk_keywords): return False
    if "-ETF" in text_to_check or " ETF" in text_to_check: return False
    return True


def _parse_timestamp(time_str):
    try:
        if not time_str:
            return datetime.now(timezone.utc).isoformat()
        dt = date_parser.parse(time_str)
        if dt.tzinfo is None:
            dt = IST.localize(dt)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception as e:
        logger.debug(f"Date Parse Error: {e}")
        return datetime.now(timezone.utc).isoformat()


def _extract_doc_urls(entry) -> list:
    doc_links = []
    allowed_exts = (".pdf", ".xml", ".zip")
    main_link = entry.get("link", "")
    if main_link.lower().endswith(allowed_exts):
        doc_links.append(main_link)
    for link in entry.get("links", []):
        href = link.get("href", "")
        if href.lower().endswith(allowed_exts):
            if href not in doc_links:
                doc_links.append(href)
    return doc_links


def _generate_id(source, entry, scrip_code=None):
    doc_urls = _extract_doc_urls(entry)
    # Strategy 1: PDF Filename + Identifier
    if doc_urls:
        filename = doc_urls[0].split('/')[-1]
        if scrip_code:
            unique_identifier = scrip_code
        else:
            unique_identifier = hashlib.md5(entry.get('title', '').encode()).hexdigest()[:8]
        return f"{source}_{unique_identifier}_{filename}"

    # Strategy 2: GUID
    guid = entry.get('guid')
    if guid:
        clean_guid = re.sub(r'\W+', '', guid)[-20:]
        return f"{source}_GUID_{clean_guid}"

    # Strategy 3: Content Hash
    raw_string = f"{entry.get('title', '')}{entry.get('published', '')}"
    content_hash = hashlib.md5(raw_string.encode('utf-8')).hexdigest()[:15]
    return f"{source}_HASH_{content_hash}"


def parse_entry(source, entry):
    try:
        raw_title = entry.get("title", "")
        summary_raw = entry.get("summary", "") or entry.get("description", "")

        if not is_relevant_stock(raw_title,summary_raw):
            return None

        scrip_code = None
        category = "General"
        raw_company_name = raw_title
        clean_name = ""
        join_key = ""

        if source == "BSE":
            raw_company_name, scrip_code = _normalize_bse_title(raw_title)
            if not scrip_code and hasattr(entry, 'scripcode'):
                scrip_code = entry.scripcode
            clean_name = normalize_company_name(raw_company_name)
            join_key = scrip_code if scrip_code else clean_name

        elif source == "NSE":
            clean_name = normalize_company_name(raw_title)
            summary_raw, category = _parse_nse_description(summary_raw)
            join_key = clean_name

        ts_str = _parse_timestamp(entry.get("published", ""))

        return {
            "source": source,
            "event_id": _generate_id(source, entry, scrip_code),
            "raw_name": raw_company_name,
            "clean_name": clean_name,
            "join_key": join_key,
            "scrip_code": scrip_code,
            "title": _normalize_text(raw_title),
            "category": _normalize_text(category),
            "summary": _normalize_text(summary_raw),
            "timestamp": ts_str,
            "pdf_url": _extract_doc_urls(entry),
            "status": "RAW"
        }

    except Exception as e:
        logger.error(f"Parser logic failed for {source}: {e}", exc_info=True)
        return None


class RSSEventFetcher:
    def __init__(self):
        self.session = None
        self.output_queue = "QUEUE:NORMALIZED_EVENTS"
        self.is_running = True

        # --- STATE OPTIMIZATION ---
        self.last_modified_times = {}  # For NSE (Headers)
        self.last_content_hashes = {}  # For BSE (Content Fingerprint)
        self.local_seen_cache = OrderedDict()  # Rolling Window Cache
        self.CACHE_LIMIT = 10000

    async def _get_session(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=15, connect=5)
            connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300, ssl=False)
            self.session = aiohttp.ClientSession(connector=connector, headers=HEADERS, timeout=timeout)
        return self.session

    async def _dispatch_to_redis(self, event):
        if event['source'] == 'BSE':
            scrip = str(event.get('scrip_code', ''))
            if scrip and (scrip.startswith('9') or scrip.startswith('8') or scrip.startswith('7')):
                return

        evt_id = event['event_id']

        # 1. RAM CHECK
        if evt_id in self.local_seen_cache:
            self.local_seen_cache.move_to_end(evt_id)
            return

        dedupe_key = f"POLLER:SEEN:{event['event_id']}"

        try:
            # 2. REDIS CHECK
            is_new = await redis_client.set(dedupe_key, "1", ex=REDIS_EXPIRY, nx=True)

            if is_new:
                logger.info(f"NEW: {event['clean_name']} [{event['source']}]")
                self.local_seen_cache[evt_id] = True
                await redis_client.rpush(self.output_queue, json.dumps(event))
            else:
                self.local_seen_cache[evt_id] = True

            if len(self.local_seen_cache) > self.CACHE_LIMIT:
                self.local_seen_cache.popitem(last=False)

        except Exception as e:
            logger.error(f"Redis Dispatch Error: {e}")

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_fixed(1),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def fetch_one(self, source, url):
        session = await self._get_session()

        # --- STRATEGY 1: IF-MODIFIED-SINCE (Works for NSE) ---
        req_headers = {}
        if url in self.last_modified_times:
            req_headers['If-Modified-Since'] = self.last_modified_times[url]

        try:
            async with session.get(url, headers=req_headers) as response:

                # [NSE] Bandwidth Saver
                if response.status == 304:
                    # Log optimization success
                    logger.info(f"[{source}] 304 Not Modified | Skipping Parse")
                    return

                if response.status == 200:
                    # Update Timestamp for next time
                    last_mod = response.headers.get('Last-Modified')
                    if last_mod:
                        self.last_modified_times[url] = last_mod

                    content = await response.read()

                    # --- STRATEGY 2: CONTENT HASHING (Works for BSE) ---
                    # Even if BSE sends 200 OK, we calculate the MD5 hash of the file.
                    # If the hash is same as last time, the file hasn't changed.
                    # We SKIP parsing entirely. Zero CPU usage.
                    current_hash = hashlib.md5(content).hexdigest()
                    if self.last_content_hashes.get(url) == current_hash:
                        # Log optimization success
                        logger.info(f"[{source}] MD5 Hash Match | Skipping Parse")
                        return

                    # New content detected! Update hash and parse.
                    self.last_content_hashes[url] = current_hash
                    # logger.info(f"[{source}] New Content Detected (Hash: {current_hash[:8]}...)")

                    feed = await asyncio.to_thread(feedparser.parse, content)
                    if not feed.entries:
                        return

                    for entry in reversed(feed.entries):
                        parsed = parse_entry(source, entry)
                        if parsed:
                            await self._dispatch_to_redis(parsed)
                    return

                logger.warning(f"{source} Down: {response.status}")

        except Exception as e:
            logger.error(f"Fetch Error ({source}): {str(e)}")

    async def run_loop(self):
        logger.info(f"Event Poller Started. Interval: {POLL_INTERVAL_SECONDS}s")

        while self.is_running:
            start_time = datetime.now()
            try:
                tasks = [self.fetch_one(src, url) for src, url in RSS_SOURCES.items()]
                await asyncio.gather(*tasks)
            except Exception as e:
                logger.critical(f"Global Poller Loop Error: {e}", exc_info=True)

            elapsed = (datetime.now() - start_time).total_seconds()
            sleep_time = max(0.5, POLL_INTERVAL_SECONDS - elapsed)
            await asyncio.sleep(sleep_time)

    async def close(self):
        logger.info("Shutting down Poller...")
        self.is_running = False
        if self.session:
            await self.session.close()


if __name__ == "__main__":
    fetcher = RSSEventFetcher()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(fetcher.run_loop())
    except KeyboardInterrupt:
        loop.run_until_complete(fetcher.close())
    finally:
        loop.close()