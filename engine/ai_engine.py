import asyncio
import json
import logging
import io
import urllib.parse
import zipfile
import re
import gc
from datetime import datetime, timezone
from typing import Union

import aiohttp
from pypdf import PdfReader
from google import genai
from google.genai import types
from bot_config import BotConfig
from config import redis_client, GEMINI_API_KEY, ai_audit

MODEL_NAME = "gemma-3-27b-it"
RATE_LIMIT_SECONDS = 5.0
CONTEXT_CHAR_LIMIT = 20_000
MAX_PDF_SIZE = 7 * 1024 * 1024

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Connection": "keep-alive"
}

# Configure logging to show everything
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AI_ENGINE")


def extract_json(text: str):
    try:
        fenced = re.search(r"```json\s*(\{.*?\})\s*```", text, re.DOTALL)
        if fenced:
            return json.loads(fenced.group(1))
        block = re.search(r"(\{[\s\S]*?\})", text)
        if block:
            return json.loads(block.group(1))
    except:
        return None
    return None


def repair_json(text: str):
    try:
        text = re.sub(r"```json|```", "", text).strip()
        text = re.sub(r",\s*}", "}", text)
        text = re.sub(r",\s*]", "]", text)
        return json.loads(text)
    except:
        return None


def validate_analysis(obj):
    if not isinstance(obj, dict):
        return None
    required = {"signal", "tier", "confidence", "catalyst"}
    if not required.issubset(obj):
        return None
    if obj["signal"] not in ("BUY", "SELL", "HOLD"):
        return None
    try:
        obj["confidence"] = float(obj["confidence"])
    except:
        return None

    if "event_summary" in obj and isinstance(obj["event_summary"], list):
        obj["event_summary"] = [
            " ".join(p.split()[:15]) for p in obj["event_summary"]
        ]
    return obj


def calibrate_confidence(raw, tier, market_phase, late_news, shadow):
    conf = float(raw)
    if tier == "EXTREME":
        conf -= 0.05
    elif tier == "VERY":
        conf -= 0.03
    if late_news:
        conf -= 0.10
    if shadow:
        conf -= 0.07
    if market_phase and market_phase != "LIVE":
        conf -= 0.05
    return round(max(0.0, min(conf, 1.0)), 2)


def smart_truncate(text: str) -> str:
    if len(text) <= CONTEXT_CHAR_LIMIT:
        return text
    h = int(CONTEXT_CHAR_LIMIT * 0.6)
    t = int(CONTEXT_CHAR_LIMIT * 0.4)
    return text[:h] + "\n...[TRUNCATED]...\n" + text[-t:]


def parse_binary(data: Union[bytes, memoryview]) -> str:
    try:
        text_content = []

        # 1. Use slicing instead of startswith()
        # This works for both 'bytes' and 'memoryview'
        if data[0:4] == b"%PDF":
            reader = PdfReader(io.BytesIO(data))
            pages = reader.pages
            # Limit pages to save time/memory
            total_pages = len(pages)
            if total_pages > 4:
                indices = [0, 1, 2, total_pages - 1]
            else:
                indices = range(total_pages)

            for i in indices:
                t = pages[i].extract_text()
                if t: text_content.append(t)
            return "\n".join(text_content)

        # 2. Use slicing for ZIP check
        if data[0:2] == b"PK":
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                for f in z.namelist():
                    if f.lower().endswith(".pdf"):
                        with z.open(f) as pdf_file:
                            pdf_data = pdf_file.read(MAX_PDF_SIZE)
                            if len(pdf_data) < MAX_PDF_SIZE:
                                text_content.append(parse_binary(pdf_data))
            return "\n".join(text_content)

        # 3. Handle string decoding for memoryview
        if isinstance(data, memoryview):
            return bytes(data).decode("utf-8", errors="ignore")
        return data.decode("utf-8", errors="ignore")

    except Exception as e:
        logger.error(f"Binary parsing error: {e}")
        return ""

async def fetch_doc(session, url: str) -> str:
    if not url or not url.startswith("http"):
        return ""

    logger.info(f"Downloading document: {url}")
    url_enc = urllib.parse.quote(url.strip(), safe=":/?&=%")

    for attempt in range(2):
        try:
            async with session.get(url_enc, timeout=20) as r:  # Reduced timeout
                if r.status != 200:
                    logger.warning(f"Download failed {url}: Status {r.status}")
                    return ""

                buf = io.BytesIO()
                size = 0

                async for chunk in r.content.iter_chunked(1024 * 1024):
                    buf.write(chunk)
                    size += len(chunk)
                    if size > MAX_PDF_SIZE:
                        logger.warning(f"PDF truncated (7MB cap): {url}")
                        break

                logger.info(f"Parsing PDF ({size} bytes)...")
                text = await asyncio.to_thread(parse_binary, buf.getbuffer())
                logger.info("PDF parsing complete.")
                return text

        except Exception as e:
            logger.warning(f"Download attempt {attempt + 1} failed: {e}")
            if attempt < 1:
                await asyncio.sleep(1)
            else:
                return ""
    return ""


def analyst_prompt(event, doc):
    return f"""
            You are a **professional hedge fund risk-first trading engine**.
            Your PRIMARY OBJECTIVE is NOT to predict.
            Your PRIMARY OBJECTIVE is to AVOID BAD TRADES.
            Default action is **HOLD**.
            ━━━━━━━━━━━━━━━━━━
            EVENT DATA
            ━━━━━━━━━━━━━━━━━━
            TITLE:
            {event.get("title")}
            SUMMARY:
            {event.get("summary")}
            MARKET PHASE:
            {event.get("market_phase")}
            URGENCY:
            {event.get("urgency")}
            LATE NEWS RISK:
            {event.get("late_news_risk")}
            DOCUMENT CONTENT (may be incomplete or delayed):
            {doc}
            ━━━━━━━━━━━━━━━━━━
            MANDATORY THINKING RULES
            ━━━━━━━━━━━━━━━━━━
            ASSUME MARKET EFFICIENCY  
            If this information is obvious, expected, repetitive, or narrative → HOLD.
            INFORMATION ≠ TRADE  
            The following are **NOT trade catalysts by default**:
            - MoUs
            - Strategic partnerships
            - Vision statements
            - Capacity plans
            - Generic press releases
            - Routine disclosures
            HASH > WORDS  
            Only consider BUY/SELL if there is:
            - Signed order with size + timeline
            - Financial impact (revenue, margin, debt, cash flow)
            - Regulatory shock, fraud, or enforcement action
            TIME SENSITIVITY  
            - If market is LIVE and reaction likely happened → HOLD
            - If disclosure is late → LOWER confidence
            - If impact is long-term → HOLD
            NEGATIVE BIAS  
            Bad news moves faster than good news.
            Good news must be **exceptionally strong** to justify BUY.
            WHEN IN DOUBT → HOLD  
            If certainty is not very high, DO NOT trade.
            EVENT SUMMARY RULES
            - Extract ONLY factual information from the document
            - NO opinions
            - NO trading language
            - NO speculation
            - NO inference
            - If disclosure is routine, explicitly say "Routine disclosure"
            - Max 5 bullets
            - Each bullet ≤ 15 words
            ━━━━━━━━━━━━━━━━━━
            DECISION STANDARD
            ━━━━━━━━━━━━━━━━━━
            BUY or SELL is allowed ONLY IF ALL conditions are true:
            1. Financial impact is explicitly stated (numbers, size, %, ₹, timeline)
            2. Impact affects P&L or balance sheet within 1–3 quarters
            3. Catalyst is non-narrative and verifiable
            4. Information is NOT routine or expected
            5. Confidence must be ≥ 0.65 for BUY or SELL
            
            If ANY condition fails → signal MUST be HOLD.
            ━━━━━━━━━━━━━━━━━━
            OUTPUT FORMAT (JSON ONLY)
            ━━━━━━━━━━━━━━━━━━
            {{
              "signal": "BUY" | "SELL" | "HOLD",
              "tier": "EXTREME" | "VERY" | "MODERATE" | "NEUTRAL",
              "confidence": <float between 0.0 and 1.0>,
              "catalyst": "<max 10 factual words>",
              "event_summary": [
                "<short factual point 1>",
                "<short factual point 2>"
              ]
            }}
            ⚠️ DO NOT explain your reasoning.
            ⚠️ DO NOT add extra text.
            ⚠️ OUTPUT JSON ONLY.
            """


def critic_prompt(event, analyst):
    return f"""
            You are a **senior risk manager whose sole job is to PREVENT LOSSES**.
            You do NOT care about opportunities.
            You ONLY care about downside risk, false positives, and overconfidence.
            ━━━━━━━━━━━━━━━━━━
            EVENT
            ━━━━━━━━━━━━━━━━━━
            {event.get("title")}
            ━━━━━━━━━━━━━━━━━━
            ANALYST DECISION
            ━━━━━━━━━━━━━━━━━━
            {analyst}
            ━━━━━━━━━━━━━━━━━━
            CRITICAL QUESTIONS
            ━━━━━━━━━━━━━━━━━━
            Ask yourself:
            - Is this already expected or routine?
            - Could this be priced-in?
            - Is this informational but not actionable?
            - Is the impact long-term but not tradable?
            - Is confidence unjustifiably high?
            - Is the catalyst narrative-based?
            - Would a professional trader hesitate here?
            If **ANY answer suggests risk**, you MUST veto.
            ━━━━━━━━━━━━━━━━━━
            OUTPUT FORMAT (JSON ONLY)
            ━━━━━━━━━━━━━━━━━━
            {{
              "veto": true | false,
              "reason": "<short factual reason>"
            }}
            ⚠️ Be aggressive.
            ⚠️ It is BETTER to veto a good trade than allow a bad one.
            ⚠️ No explanations outside JSON.
            """


def normalize_ai_decision(analysis, config):
    if analysis["signal"] in ("BUY", "SELL"):
        if analysis["confidence"] < config.AI_MIN_CONFIDENCE:
            analysis["signal"] = "HOLD"
            analysis["tier"] = "NEUTRAL"
            analysis["catalyst"] = "Insufficient confidence"

    if analysis["tier"] == "EXTREME" and analysis["confidence"] < config.AI_MIN_CONFIDENCE:
        analysis["tier"] = "VERY"

    return analysis


def build_ai_summary(event, analysis):
    flags = []
    if event.get("late_news_risk"):
        flags.append("LATE_NEWS")
    if "shadow_violation" in event:
        flags.append("REVIEW")

    bias = (
        "BULLISH" if analysis["signal"] == "BUY"
        else "BEARISH" if analysis["signal"] == "SELL"
        else "NEUTRAL"
    )

    return {
        "bias": bias,
        "risk_flags": flags,
        "headline": analysis.get("catalyst"),
        "pdf_summary": analysis.get("event_summary", []),
        "note": "Derived from official disclosure"
    }


class AIEngine:

    def __init__(self):
        if not GEMINI_API_KEY:
            raise RuntimeError("Missing GEMINI_API_KEY")

        self.input_queue = "QUEUE:FILTERED_EVENTS"
        self.output_queue = "QUEUE:AI_SIGNALS"
        self.client = genai.Client(api_key=GEMINI_API_KEY)
        self.session = None

    async def get_session(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(headers=HEADERS)
        return self.session

    async def _call_gemini_with_retry(self, prompt: str, temperature: float):
        max_attempts = 4
        delay = 20

        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"Calling ai (Attempt {attempt})...")
                return await asyncio.to_thread(
                    self.client.models.generate_content,
                    model=MODEL_NAME,
                    contents=prompt,
                    config=types.GenerateContentConfig(temperature=temperature)
                )

            except Exception as e:
                err = str(e).upper()
                retriable = ("429" in err or "RESOURCE_EXHAUSTED" in err or "503" in err)

                if not retriable:
                    logger.error(f"NON-RETRIABLE AI ERROR: {e}")
                    return None

                logger.warning(f"AI RATE LIMIT (Attempt {attempt}). Sleeping {delay}s")
                await asyncio.sleep(delay)
                delay *= 2

        return None

    async def analyze(self, event):
        session = await self.get_session()
        system_config = await BotConfig.load()

        docs = ""
        pdf_urls = event.get("pdf_url", [])
        # Log if we are about to fetch docs
        if pdf_urls:
            if isinstance(pdf_urls, str): pdf_urls = [pdf_urls]

            logger.info(f"Fetching {len(pdf_urls[:2])} documents in parallel...")
            tasks = [fetch_doc(session, url) for url in pdf_urls[:2]]
            results = await asyncio.gather(*tasks)
            docs = "\n".join(results)
        else:
            logger.info("No PDF URLs found.")

        docs = smart_truncate(docs)

        # --- ANALYST ---
        logger.info("Running Analyst...")
        analyst_resp = await self._call_gemini_with_retry(
            analyst_prompt(event, docs),
            temperature=0.1
        )

        if not analyst_resp:
            logger.error("Analyst returned None")
            return None

        analyst = extract_json(analyst_resp.text)
        if not analyst:
            analyst = repair_json(analyst_resp.text)

        analyst = validate_analysis(analyst)

        # HARD SAFETY VALIDATION
        if analyst["signal"] in ("BUY", "SELL"):
            if not (0.0 <= analyst["confidence"] <= 1.0):
                return None

            if len(analyst.get("catalyst", "")) < 3:
                analyst["signal"] = "HOLD"
                analyst["confidence"] = 0.0


        if not analyst:
            logger.error(f"Analyst JSON validation failed: {analyst_resp.text[:100]}...")
            return None

        analyst = normalize_ai_decision(analyst,system_config)
        logger.info(f"Analyst Decision: {analyst['signal']} ({analyst['confidence']})")

        if analyst["signal"] == "HOLD":
            logger.info("-> Skipping Critic (Analyst voted HOLD)")
            analyst["critic_vetted"] = False
        else:
            # --- CRITIC ---
            logger.info("Running Critic...")
            critic_resp = await self._call_gemini_with_retry(
                critic_prompt(event, analyst),
                temperature=0.0
            )

            if not critic_resp:
                logger.warning("Critic unavailable, defaulting to Analyst")
            else:
                critic = extract_json(critic_resp.text) or {"veto": False}
                if critic.get("veto"):
                    logger.info(f"Critic VETO: {critic.get('reason')}")
                    analyst["signal"] = "HOLD"
                    analyst["confidence"] = 0.0
                    analyst["catalyst"] = critic.get("reason", "Risk veto")

            analyst = normalize_ai_decision(analyst, system_config)

        # --- FINAL CONFIDENCE ---
        analyst["confidence_raw"] = analyst["confidence"]
        analyst["confidence_adjusted"] = calibrate_confidence(
            analyst["confidence"],
            analyst["tier"],
            event.get("market_phase"),
            event.get("late_news_risk", False),
            "shadow_violation" in event,
        )

        analyst["confidence_label"] = (
            "VERY_HIGH" if analyst["confidence_adjusted"] >= 0.75
            else "HIGH" if analyst["confidence_adjusted"] >= 0.65
            else "MEDIUM" if analyst["confidence_adjusted"] >= 0.50
            else "LOW"
        )

        return analyst

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def run(self):
        logger.info("AI Engine loop started.")
        while True:
            try:
                gc.collect()

                # 1. Backpressure Check
                queue_len = await redis_client.llen(self.input_queue)
                if queue_len > 0:
                    logger.info(f"Queue Length: {queue_len}")

                item = await redis_client.blpop(self.input_queue, timeout=60)
                if not item:
                    continue

                # 3. Parse Event
                event = json.loads(item[1])
                logger.info(f"Processing: {event.get('clean_name', 'Unknown')} | {event.get('title')}")

                start = datetime.now()

                # 4. Run Analysis
                result = await self.analyze(event)

                # 5. Handle Failure
                if not result:
                    logger.warning(f"AI Analysis Failed for {event['clean_name']}")
                    await ai_audit.insert_one({
                        "event_id": event["event_id"],
                        "signal": "HOLD",
                        "catalyst": "AI_FAILURE",
                        "timestamp": datetime.now(timezone.utc)
                    })
                    continue

                # 6. Save Audit
                logger.info(f"Result: {result['signal']} | Conf: {result.get('confidence_adjusted')}")
                await ai_audit.insert_one({
                    "event_id": event["event_id"],
                    "signal": result["signal"],
                    "tier": result["tier"],
                    "confidence_raw": result.get("confidence_raw"),
                    "confidence_adjusted": result.get("confidence_adjusted"),
                    "confidence_label": result.get("confidence_label"),
                    "catalyst": result["catalyst"],
                    "event_summary": result.get("event_summary"),
                    "analysis": result.copy(),
                    "schema_version": 2,
                    "timestamp": datetime.now(timezone.utc)
                })

                # 7. Push Signal (if valid)
                if result["signal"] != "HOLD":
                    try:
                        summary_data = build_ai_summary(event, result)
                        event["ai_analysis"] = result
                        event["ai_summary"] = summary_data

                        await redis_client.rpush(self.output_queue, json.dumps(event))
                        logger.info(f" *** SIGNAL PUSHED: {event['clean_name']} [{result['signal']}] ***")
                    except Exception as e:
                        logger.error(f"Summary Build Failed: {e}")

                # Rate Limiting
                elapsed = (datetime.now() - start).total_seconds()
                logger.info(f"Processed in {elapsed:.2f}s")

                if elapsed < RATE_LIMIT_SECONDS:
                    await asyncio.sleep(RATE_LIMIT_SECONDS - elapsed)

            except Exception as e:
                logger.error(f"CRITICAL LOOP ERROR: {e}", exc_info=True)
                await asyncio.sleep(1)


if __name__ == "__main__":
    engine = AIEngine()
    try:
        asyncio.run(engine.run())
    finally:
        try:
            asyncio.run(engine.close())
        except RuntimeError:
            pass

