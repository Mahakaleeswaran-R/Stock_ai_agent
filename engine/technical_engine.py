import asyncio
import json
import logging
import math
import uuid
from datetime import datetime, time, timezone
from typing import Dict, Tuple, Optional
from redis import WatchError
import pandas as pd
import pandas_ta as ta
import pytz
from bot_config import BotConfig
from utils.angel_one_bridge import angel_bridge
from config import redis_client, trade_signals, technical_audit


IST = pytz.timezone("Asia/Kolkata")

MAX_CAPITAL_ALLOCATION = 500_000
MAX_LOSS_STREAK = 3

REGIME_CACHE_SECONDS = 300

MARKET_START = time(9, 14)
MARKET_LAST_ENTRY = time(15, 0)
FORCE_EXIT_TIME = "15:15 IST"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TECHNICAL_ENGINE")


def json_serial(obj):
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

async def audit(event, ticker, decision, reason, stats=None):
    await technical_audit.insert_one({
        "event_id": event.get("event_id"),
        "ticker": ticker,
        "decision": decision,
        "reason": reason,
        "stats": stats,
        "timestamp": datetime.now(IST).isoformat()
    })

class GlobalRiskGovernor:

    @staticmethod
    async def try_reserve_capacity(config):
        if await redis_client.exists("MARKET:HALT"):
            return False, "MARKET_HALTED"

        # We use a Redis Pipeline to ensure atomicity
        async with redis_client.pipeline() as pipe:
            while True:
                try:
                    await pipe.watch(
                        "RISK:DAILY_PNL_PCT",
                        "RISK:LOSS_STREAK",
                        "RISK:OPEN_TRADES_COUNT",
                    )

                    # 2. READ current values
                    daily_pnl = float(await redis_client.get("RISK:DAILY_PNL_PCT") or 0)
                    loss_streak = int(await redis_client.get("RISK:LOSS_STREAK") or 0)
                    open_trades = int(await redis_client.get("RISK:OPEN_TRADES_COUNT") or 0)

                    # 3. CHECK Limits (The Logic)
                    if daily_pnl <= config.MAX_DAILY_LOSS_PCT:
                        await pipe.unwatch()
                        return False, "DAILY_DRAWDOWN_LIMIT"
                    if loss_streak >= MAX_LOSS_STREAK:
                        await pipe.unwatch()
                        return False, "LOSS_STREAK_LIMIT"
                    if open_trades >= config.MAX_CONCURRENT_TRADES:
                        await pipe.unwatch()
                        return False, "MAX_CONCURRENT_TRADES"


                    # 4. ACT (Increment Counters)
                    pipe.multi()  # Start transaction block
                    pipe.incr("RISK:OPEN_TRADES_COUNT")
                    await pipe.execute()  # Commit transaction
                    return True, "OK"

                except WatchError:
                    continue


class SymbolStateMachine:

    @staticmethod
    async def clear_all_locks():
        keys = await redis_client.keys("STATE:*")
        if keys:
            await redis_client.delete(*keys)
            logger.info(f"Cleared {len(keys)} stale symbol locks")

    @staticmethod
    async def validate(ticker: str, signal: str) -> Tuple[bool, str]:

        if await redis_client.exists(f"DAILY_TRADED:{ticker}"):
            return False, "ALREADY_TRADED_TODAY"

        state = await redis_client.get(f"STATE:{ticker}")
        if state:
            s = state
            if "LONG" in s and signal == "SELL":
                return False, "REVERSAL_BLOCKED"
            if "SHORT" in s and signal == "BUY":
                return False, "REVERSAL_BLOCKED"

        return True, "OK"

    @staticmethod
    async def lock(ticker: str, signal: str):
        await redis_client.setex(f"STATE:{ticker}", 64800, f"ACTIVE_{signal}")
        await redis_client.setex(f"DAILY_TRADED:{ticker}", 64800, "TRUE")


class MarketRegimeDetector:

    @staticmethod
    async def get() -> str:
        cached = await redis_client.get("MARKET:REGIME")
        if cached:
            return cached

        df = await asyncio.to_thread(
            angel_bridge.get_historical_candles,
            "NIFTY",
            "FIVE_MINUTE",
            5
        )
        if df is None or df.empty:
            return "RANGING"

        df["ATR"] = ta.atr(df.High, df.Low, df.Close, 14)
        atr, atr_ma = df.ATR.iloc[-1], df.ATR.rolling(20).mean().iloc[-1]
        close, sma50 = df.Close.iloc[-1], df.Close.rolling(50).mean().iloc[-1]

        if atr > atr_ma * 2:
            regime = "PANIC"
        elif abs(close - sma50) > sma50 * 0.005:
            regime = "TRENDING"
        else:
            regime = "RANGING"

        await redis_client.setex("MARKET:REGIME", REGIME_CACHE_SECONDS, regime)
        return regime


class MarketDataService:

    @staticmethod
    async def get_df(symbol: str) -> Optional[pd.DataFrame]:
        df = await asyncio.to_thread(
            angel_bridge.get_historical_candles,
            symbol,
            "FIVE_MINUTE",
            5
        )
        if df is None or df.empty:
            return None
        df.index = df.index.tz_localize(IST) if df.index.tz is None else df.index
        return df

    @staticmethod
    def indicators(df: pd.DataFrame) -> Dict:
        df = df.copy()

        tp = (df.High + df.Low + df.Close) / 3
        df["VWAP"] = (tp * df.Volume).groupby(df.index.date).cumsum() / df.Volume.groupby(df.index.date).cumsum()

        df["RSI"] = ta.rsi(df.Close, 14)
        df["ATR"] = ta.atr(df.High, df.Low, df.Close, 14)

        avg_vol = df.Volume.rolling(20).mean().iloc[-1]
        rvol = df.Volume.iloc[-1] / avg_vol if avg_vol and avg_vol > 0 else 1.0

        return {
            "price": float(df.Close.iloc[-1]),
            "vwap": float(df.VWAP.iloc[-1]),
            "rsi": float(df.RSI.iloc[-1]),
            "atr": float(df.ATR.iloc[-1]),
            "rvol": float(rvol)
        }


class TechnicalEngine:

    async def process(self, event: dict):
        config = await BotConfig.load()
        # 1. Symbol Lookup
        symbols = await redis_client.hget("CONFIG:ISIN:SYMBOL", event["isin"])
        if not symbols:
            await audit(event, "UNKNOWN", "REJECTED", "SYMBOL_NOT_FOUND")
            return
        ticker = json.loads(symbols)[0]

        # 2. Extract AI Data (Fixing the Missing Data Issue)
        ai = event["ai_analysis"]
        summary_data = event.get("ai_summary", {})

        signal = ai["signal"]
        tier = ai.get("tier", "MODERATE")
        catalyst = ai.get("catalyst", "Unknown")

        # 3. Urgency Check
        urgency = event.get("urgency", "LOW")
        if urgency == "LOW" and tier != "EXTREME":
            await audit(event, ticker, "REJECTED", "LOW_URGENCY")
            return

        now = datetime.now(IST).time()
        regime = await MarketRegimeDetector.get()

        # 4. Time Filters (Refined for Real Market)

        # Opening Volatility: Skip unless Extreme
        if time(9, 15) <= now <= time(9, 35):
            if tier != "EXTREME":
                await audit(event, ticker, "REJECTED", "OPEN_VOLATILITY_FILTER")
                return

        # Noon Chop: Allow Extreme news even during chop
        if time(11, 30) <= now <= time(13, 0):
            if regime != "TRENDING" and tier != "EXTREME":
                await audit(event, ticker, "REJECTED", "NOON_CHOP_FILTER")
                return

        if not (MARKET_START <= now <= MARKET_LAST_ENTRY):
            await audit(event, ticker, "REJECTED", "OUTSIDE_MARKET_HOURS")
            return

        # 5. State & Data
        ok, msg = await SymbolStateMachine.validate(ticker, signal)
        if not ok:
            await audit(event, ticker, "REJECTED", msg)
            return

        df = await MarketDataService.get_df(ticker)
        if df is None:
            await audit(event, ticker, "REJECTED", "NO_DATA")
            return

        stats = MarketDataService.indicators(df)

        try:
            ltp_data = await asyncio.to_thread(angel_bridge.get_ltp, ticker)

            if not ltp_data:
                await audit(event, ticker, "REJECTED", "LTP_FETCH_FAILED_NULL", stats)
                return

            current_price = float(ltp_data)

        except Exception as e:
            logger.error(f"LTP Fetch Error for {ticker}: {e}")
            await audit(event, ticker, "REJECTED", "LTP_API_ERROR", stats)
            return
            # Update stats with the REAL verified price
        stats["price"] = current_price

        price, vwap, atr, rvol = stats["price"], stats["vwap"], stats["atr"], stats["rvol"]

        # 6. Technical Checks
        if regime == "PANIC":
            if signal == "BUY":
                await audit(event, ticker, "REJECTED", "PANIC_NO_BUY", stats)
            if tier != "EXTREME":
                await audit(event, ticker, "REJECTED", "PANIC_TIER", stats)
            return


        if rvol < 1.2 and tier != "EXTREME":
            await audit(event, ticker, "REJECTED", "LOW_RVOL", stats)
            return

        # Gap Risk (Real Market Scenario: Don't buy if it already jumped 6%)
        gap_limit = 0.06 if tier != "EXTREME" else 0.12

        if abs(price - vwap) / vwap > gap_limit:
            await audit(event, ticker, "REJECTED", "GAP_RISK", stats)
            return

        # Momentum Alignment
        if signal == "BUY" and price < vwap and tier != "EXTREME":
            await audit(event, ticker, "REJECTED", "BELOW_VWAP", stats)
            return

        # 7. Sizing & Targets
        atr_mult = 2.2 if tier == "EXTREME" else 2.0
        prev_low = df.Low.iloc[-2]
        prev_high = df.High.iloc[-2]

        if signal == "BUY":
            sl = min(prev_low, vwap - atr)
        else:
            sl = max(prev_high, vwap + atr)

        risk_per_share = abs(price - sl)

        if risk_per_share < (price * 0.003):  # Minimum 0.3% risk
            await audit(event, ticker, "REJECTED", "STOP_TOO_TIGHT", stats)
            return

        # Calculate Quantity
        effective_conf = ai.get("confidence_adjusted", 0.6)
        risk_multiplier = 1.3 if effective_conf >= 0.80 else (0.7 if effective_conf < 0.60 else 1.0)

        qty = math.floor((MAX_CAPITAL_ALLOCATION * config.RISK_PER_TRADE_PCT * risk_multiplier) / risk_per_share)
        max_liq_qty = int(df.Volume.iloc[-1] * 0.05)
        qty = min(qty, max_liq_qty)
        cap_limit = math.floor((MAX_CAPITAL_ALLOCATION * config.MAX_POSITION_SIZE_PCT) / price)
        qty = min(qty, cap_limit)

        if qty <= 0:
            await audit(event, ticker, "REJECTED", "SIZE_TOO_SMALL", stats)
            return

        # 8. Capacity Reservation
        reserved, reason = await GlobalRiskGovernor.try_reserve_capacity(config)
        if not reserved:
            await audit(event, ticker, "REJECTED", reason, stats)
            return

        # 9. Construct Final Payload (Enriched)
        trade_id = str(uuid.uuid4())

        # Calculate Technical Confidence Score
        tech_confidence = 0.6
        if tier == "EXTREME": tech_confidence += 0.15
        if rvol > 2.0: tech_confidence += 0.1
        if regime == "TRENDING": tech_confidence += 0.05
        tech_confidence = max(0.4, min(tech_confidence, 0.95))

        payload = {
            "trade_id": trade_id,
            "symbol": ticker,
            "signal": signal,  # Standardized name
            "order_type": "MARKET",  # Usually safer for news trading than LIMIT
            "quantity": qty,

            # --- THE MISSING AI DATA ---
            "reason": catalyst,  # Passed to execution for logging
            "ai_confidence": ai.get("confidence_adjusted"),
            "tech_confidence": round(tech_confidence, 2),
            "event_summary": summary_data.get("pdf_summary", []),
            "event_title": event.get("title"),

            "trade_params": {
                "entry_ref": round(price, 2),
                "stop_loss": round(sl, 2),
                "target": round(price + (risk_per_share * 3) if signal == "BUY" else price - (risk_per_share * 3), 2)
            },
            "force_exit_time": FORCE_EXIT_TIME,
            "timestamp": datetime.now(IST).isoformat()
        }

        try:
            await SymbolStateMachine.lock(ticker, signal)
            await redis_client.rpush("QUEUE:TRADE_SIGNALS", json.dumps(payload, default=json_serial))
            await trade_signals.insert_one(payload)

            await audit(event, ticker, "PUBLISHED", "VALID_SIGNAL", stats)
            logger.info(f"EXECUTED {ticker} {signal} QTY {qty} | Reason: {catalyst}")

        except Exception as e:
            logger.critical(f"EXECUTION FAILED: {e}")
            # Rollback limits
            await redis_client.decr("RISK:OPEN_TRADES_COUNT")
            await redis_client.delete(f"TRADE:{trade_id}")
            await redis_client.delete(f"STATE:{ticker}")  # Unlock symbol
            return

    async def run(self):
        await angel_bridge.initialize()
        while True:
            item = await redis_client.blpop("QUEUE:AI_SIGNALS", timeout=60)
            if item:
                await self.process(json.loads(item[1]))
            else:
                await asyncio.sleep(0.05)

if __name__ == "__main__":
    engine = TechnicalEngine()
    asyncio.run(engine.run())
