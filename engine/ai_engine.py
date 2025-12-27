import asyncio
import json
import logging
import io
import zipfile
import re

import aiohttp
from datetime import datetime
from google.genai import types
from pypdf import PdfReader
from google import genai
import urllib.parse

from config import redis_client, GEMINI_API_KEY, ai_audit

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AI_ENGINE")

MODEL_NAME = "gemma-3-27b-it"
RATE_LIMIT_COOLDOWN = 5.0
CONTEXT_CHAR_LIMIT = 30000

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Connection": "keep-alive"
}

async def log_audit(event, analysis):
    try:
        doc = {
            "_id": event['event_id'],
            "symbol": event['clean_name'],
            "timestamp": datetime.now(),
            "title": event.get('title'),
            "ai_signal": analysis.get('signal') if analysis else "ERROR",
            "ai_tier": analysis.get('tier') if analysis else None,
            "ai_confidence": analysis.get('confidence') if analysis else 0,
            "ai_catalyst": analysis.get('catalyst') if analysis else str(analysis),
            "news_source": event['source']
        }
        await ai_audit.update_one({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
    except Exception as e:
        logger.error(f"Audit Log Error: {e}")


def extract_json(text):
    try:
        match = re.search(r"```json\s*(\{.*?\})\s*```", text, re.DOTALL)
        if match: return json.loads(match.group(1))
        match = re.search(r"(\{.*?\})", text, re.DOTALL)
        if match: return json.loads(match.group(1))
        return None
    except:
        return None


def _parse_bytes(data):
    try:
        if data.startswith(b'%PDF'):
            reader = PdfReader(io.BytesIO(data))
            text = []
            pages = len(reader.pages)
            indices = list(range(min(2, pages))) + list(range(max(0, pages - 2), pages))
            indices = sorted(list(set(indices)))
            for i in indices:
                page_text = reader.pages[i].extract_text()
                if page_text: text.append(page_text)
            return "\n".join(text)
        if data.startswith(b'PK'):
            text_content = []
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                pdf_files = [f for f in z.namelist() if f.lower().endswith('.pdf')]
                if not pdf_files:
                    return "ZIP contained no PDFs."
                for filename in pdf_files:
                    try:
                        with z.open(filename) as pdf_file:
                            pdf_data = pdf_file.read()
                            reader = PdfReader(io.BytesIO(pdf_data))
                            file_text = "\n".join(
                                [page.extract_text() for page in reader.pages[:2] if page.extract_text()])
                            text_content.append(f"\nFILE: {filename}\n{file_text}")
                    except:
                        continue
            return "\n".join(text_content)
        return smart_truncate(data.decode('utf-8', errors='ignore'))
    except Exception as e:
        return f"Error parsing file: {str(e)}"


async def fetch_single_url(session, url):
    text_content = []
    if not url or not url.startswith("http"):
        return None

    # URL Encoding fix
    url = url.strip()
    if " " in url:
        url = urllib.parse.quote(url, safe=":/?&=")

    for attempt in range(3):
        try:
            async with session.get(url, timeout=30) as resp:
                if resp.status == 200:
                    data = io.BytesIO()
                    bytes_downloaded = 0
                    max_size = 10 * 1024 * 1024  # 10 MB Limit

                    # 2. Read in chunks (Safe for RAM)
                    async for chunk in resp.content.iter_chunked(1024 * 1024):  # 1MB chunks
                        data.write(chunk)
                        bytes_downloaded += len(chunk)
                        if bytes_downloaded > max_size:
                            logger.warning(f"Truncated large PDF at 10MB: {url}")
                            break
                    data.seek(0)
                    text = await asyncio.to_thread(_parse_bytes, data.read())

                    if text:
                        label = "XBRL_DATA" if url.lower().endswith(".xml") else "DOC"
                        text_content.append(f"{label} ({url.split('/')[-1]}):\n{text}")
                    break
                else:
                    logger.warning(f"Fetch Failed {resp.status}: {url}")
                    break
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(2)
            else:
                logger.warning(f"Fetch Error {url}: {e}")

    return "\n\n".join(text_content)

def smart_truncate(text, limit=CONTEXT_CHAR_LIMIT):
    if len(text) <= limit:
        return text
    head_size = int(limit * 0.6)
    tail_size = int(limit * 0.4)
    return text[:head_size] + "\n...[SNIPPED]...\n" + text[-tail_size:]


async def get_dynamic_confidence():
    try:
        val = await redis_client.get("CONFIG:DYNAMIC:AI_CONFIDENCE_THRESHOLD")
        if val:
            logger.info(f"Using Dynamic Confidence: {val}")
            return float(val)
    except Exception:
        pass
    return 0.65


class AIEngine:
    def __init__(self):
        self.input_queue = "QUEUE:FILTERED_EVENTS"
        self.output_queue = "QUEUE:AI_SIGNALS"
        if not GEMINI_API_KEY:
            raise ValueError("Missing GEMINI_API_KEY")
        self.ai_client = genai.Client(api_key=GEMINI_API_KEY)
        self.session = None

    async def get_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(headers=HEADERS)
        return self.session

    async def fetch_pdf_text(self, doc_urls):
        if not doc_urls: return ""
        if isinstance(doc_urls, str): doc_urls = [doc_urls]

        session = await self.get_session()
        tasks = [fetch_single_url(session, url) for url in doc_urls]
        results = await asyncio.gather(*tasks)

        return "\n\n".join([r for r in results if r])

    async def producer_step(self):
        try:
            while True:
                item = await redis_client.blpop(self.input_queue, timeout=60)
                if item:
                    event = json.loads(item[1])
                    pdf_text = await self.fetch_pdf_text(event.get('pdf_url'))
                    if not pdf_text:
                        pdf_text = "[WARNING: PDF DOWNLOAD FAILED]"
                    else:
                        pdf_text = smart_truncate(pdf_text)
                    return event, pdf_text
                await asyncio.sleep(0.1)

        except Exception as e:
            logger.error(f"Producer Error: {e}")
            await asyncio.sleep(1)
            return None

    async def consumer_step(self, event, pdf_text):
        try:
            logger.info(f"Analyzing: {event['clean_name']}")
            conf_threshold = await get_dynamic_confidence()
            logger.info("current ai threshold: " + str(conf_threshold))
            start_time = datetime.now()
            analysis = await self.analyze_sentiment(event, pdf_text)
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed < RATE_LIMIT_COOLDOWN:
                sleep_time = RATE_LIMIT_COOLDOWN - elapsed
                logger.info(f"Cooldown: Sleeping {sleep_time:.1f}s...")
                await asyncio.sleep(sleep_time)
            await log_audit(event, analysis)

            if analysis and analysis['signal'] != "HOLD" and analysis['confidence'] >= conf_threshold:
                event['ai_analysis'] = analysis
                await redis_client.rpush(self.output_queue, json.dumps(event))
                logger.info(
                    f"SIGNAL: {event['clean_name']} [{analysis['signal']} | {analysis['confidence']} >= {conf_threshold}]")
            else:
                reason = analysis.get('signal', 'N/A') if analysis else "AI_ERR"
                conf = analysis.get('confidence', 0) if analysis else 0
                logger.info(f"Dropped: {event['clean_name']} ({reason} | Conf {conf} < {conf_threshold})")

        except Exception as e:
            logger.error(f"Consumer Error: {e}")

    async def analyze_sentiment(self, event, doc_text):
        full_context = f"TITLE: {event.get('title')}\nSUMMARY: {event.get('summary')}\n{doc_text[:CONTEXT_CHAR_LIMIT]}"
        prompt = """
        You are a **Cynical Hedge Fund Algo**. Your goal is **Alpha**. 
        Most news is noise. Default to **HOLD**. Only trigger if price impact is IMMEDIATE and GUARANTEED.
        **CRITICAL INSTRUCTION:**
        If DOC_CONTENT contains "[WARNING: PDF DOWNLOAD FAILED]", you MUST LOWER your confidence. 
        Only trigger BUY/SELL if the TITLE/SUMMARY provides irrefutable proof (e.g., "Declared Bonus 1:1").
        Otherwise, default to **HOLD** due to missing data.
        **STRATEGY GUIDELINES:**
        1. **Ignore "Fluff":** MoUs without values, "Strategic Partnerships" with no money, and "Future Plans" are NOISE.
        2. **Focus on Cash Flow:** Hard numbers (₹ Cr), Order Book wins, and Debt Reduction are GOLD.
        3. **Panic is Profit:** Corporate Fraud, Raids, and Resignations are immediate SHORTS.
        **CLASSIFICATION TIERS:**        
        **SIGNAL: SELL (Panic & crash)**
        * **EXTREME (Conf > 0.90)**: Forensic Audit, ED/CBI/IT Raids, Default on Loan, Auditor Resignation.
        * **VERY (Conf 0.80)**: CFO/CEO Resignation (Unexpected), Major Fire/Strike, License Cancellation.

        **SIGNAL: BUY (Momentum & Gap Up)**
        * **EXTREME (Conf > 0.85)**: 
            - **Mega Order:** Value > 20% of Company's Market Cap (or >₹2000Cr for midcaps).
            - **Corporate Action:** Bonus/Split/Buyback **Record Date** Announced (NOT just meeting intimation).
            - **Strategic Stake:** Investment by Global Giant (Google, Reliance, Tata, Tesla).
        * **VERY (Conf 0.75-0.84)**:
            - **Strong Order:** Value 5-20% of Market Cap.
            - **Turnaround:** Company declares "Net Debt Free" or exits NCLT/IBC.
            - **Expansion:** New Plant **Commissioned** (Commercial production started).
        * **MODERATE (Conf 0.65-0.74)**:
            - **Upgrade:** Credit Rating **UPGRADE** (e.g., BBB -> A). *NOTE: "Reaffirmed" is NEUTRAL.*
            - **Alliance:** MoU / JVs (Non-binding, speculative).
            - **Small Order:** < 5% Market Cap.
        
        **SIGNAL: HOLD / NEUTRAL (The Filter)**
        * **Routine:** Trading Window Closure, Earnings Call, AGM/EGM Notice, Postal Ballot.
        * **Non-Event:** "Rating Reaffirmed", "Review of Rating", "Clarification on Spurt in Volume".
        * **Vague:** Press Release without financial numbers.

        **OUTPUT FORMAT (JSON ONLY):**
        {"signal": "BUY"|"SELL"|"HOLD", "tier": "EXTREME"|"VERY"|"MODERATE"|"NEUTRAL", "confidence": <float 0.0-1.0>, "catalyst": "<Max 10 words, punchy reason>"}
        """
        retries = 5
        for attempt in range(retries):
            try:
                response = await asyncio.to_thread(
                    self.ai_client.models.generate_content,
                    model=MODEL_NAME,
                    contents=f"{prompt}\n\nDATA:\n{full_context}",
                    config = types.GenerateContentConfig(temperature=0.1)
                )
                return extract_json(response.text)
            except Exception as e:
                error_msg = str(e)
                if "429" in error_msg or "RESOURCE_EXHAUSTED" in error_msg or "503" in error_msg:
                    sleep_time = 15 * (attempt + 1)
                    logger.warning(f"Quota Exceeded. Sleeping {sleep_time}s (Attempt {attempt + 1}/{retries})...")
                    await asyncio.sleep(sleep_time)
                else:
                    logger.error(f"AI Analysis Error: {e}")
                    return None
        return None

    async def run(self):
        while True:
            try:
                data = await self.producer_step()
                if not data:
                    await asyncio.sleep(1)
                    continue
                event, pdf_text = data
                await self.consumer_step(event, pdf_text)
            except Exception as e:
                logger.error(f"Engine Loop Error: {e}")
                await asyncio.sleep(5)

    async def shutdown(self):
        if self.session: await self.session.close()


if __name__ == "__main__":
    aiEngine = AIEngine()
    try:
        asyncio.run(aiEngine.run())
    except KeyboardInterrupt:
        asyncio.run(aiEngine.shutdown())
