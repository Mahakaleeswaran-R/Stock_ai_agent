import asyncio
import gc
import logging
from dataclasses import asdict
from datetime import datetime, timedelta, timezone, time
from typing import Dict, AsyncGenerator

import pandas as pd
import pytz

from config import ai_audit, technical_audit, trade_signals, system_performance
from bot_config import BotConfig
from utils.angel_one_bridge import angel_bridge

IST = pytz.timezone("Asia/Kolkata")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("PERFORMANCE_HEALER")


# The Analyzer is NOT allowed to set values outside these limits
BOUNDS = {
    "AI_MIN_CONFIDENCE": (0.55, 0.90),  # Never go below 55%, never require >90%
    "RISK_PER_TRADE_PCT": (0.001, 0.01),  # Min 0.1%, Max 1% risk
    "MAX_CONCURRENT_TRADES": (1, 10)
}

MIN_TRADES_FOR_ADJUSTMENT = 10
CANDLE_LOOKBACK_DAYS = 2


async def fetch_intraday_truth(ticker: str, trade_date: datetime) -> pd.DataFrame:
    try:
        if not angel_bridge.api:
            await angel_bridge.initialize()

        df = await angel_bridge.get_historical_candles(
            symbol=ticker,
            interval="FIVE_MINUTE",
            days=CANDLE_LOOKBACK_DAYS
        )
        if df is None or df.empty: return pd.DataFrame()
        if df.index.tz is None: df.index = df.index.tz_localize(IST)

        if trade_date.tzinfo is None: trade_date = pytz.utc.localize(trade_date)
        trade_date_ist = trade_date.astimezone(IST)

        day_df = df[df.index >= trade_date_ist].copy()
        if day_df.empty: return pd.DataFrame()
        del df
        day_df.sort_index(inplace=True)
        return day_df
    except Exception as e:
        logger.warning(f"Data fetch failed {ticker}: {e}")
        return pd.DataFrame()


async def stream_events() -> AsyncGenerator[Dict, None]:
    now_ist = datetime.now(IST)
    target_date = now_ist.date() - timedelta(days=1)
    end_window_ist = IST.localize(datetime.combine(target_date, time(16, 30, 0)))

    # Start of Window: Day Before Yesterday @ 16:30 IST
    start_date = now_ist.date() - timedelta(days=2)
    start_window_ist = IST.localize(datetime.combine(start_date, time(16, 30, 0)))

    # Convert to UTC for MongoDB Query
    start_utc = start_window_ist.astimezone(timezone.utc)
    end_utc = end_window_ist.astimezone(timezone.utc)

    logger.info(f"Analysis Window (IST): {start_window_ist} -> {end_window_ist}")

    cursor = ai_audit.find({
        "timestamp": {
            "$gte": start_utc,
            "$lte": end_utc
        }
    }).sort("timestamp", 1)

    async for doc in cursor:
        tech = await technical_audit.find_one({"event_id": doc["event_id"]})
        trade = await trade_signals.find_one({"event_id": doc["event_id"]})

        if not tech or not tech.get("ticker"): continue

        yield {
            "ticker": tech["ticker"],
            "signal": doc.get("analysis", {}).get("signal"),
            "ai_confidence": doc.get("analysis", {}).get("confidence_adjusted", 0),
            "timestamp": doc["timestamp"],
            "trade": trade,
            "tech_status": tech.get("decision")
        }


class PerfromanceAnalyser:
    def __init__(self):
        self.stats = {
            "ai_correct": 0, "ai_total": 0,
            "wins": 0, "losses": 0,
            "fomo_wins": 0,
            "total_pnl_r": 0.0
        }
        self.report = {
            "date": datetime.now(IST).strftime("%Y-%m-%d"),
            "healing_actions": [],
            "metrics": {}
        }

    async def process_event(self, ev: Dict):
        if ev.get("signal") not in ("BUY", "SELL"): return

        df = await fetch_intraday_truth(ev["ticker"], ev["timestamp"])
        if df.empty: return

        # "Truth" determination
        start_price = df.iloc[0].Close

        # Simple AI Accuracy (Did it move 1% in direction?)
        stock_ret = (df.Close.max() - start_price) / start_price
        nifty_df = await fetch_intraday_truth("NIFTY", ev["timestamp"])
        nifty_ret = (nifty_df.Close.max() - nifty_df.Close.iloc[0]) / nifty_df.Close.iloc[0]

        ai_won = (stock_ret - nifty_ret) > 0.007

        self.stats["ai_total"] += 1
        if ai_won: self.stats["ai_correct"] += 1

        # Trade PnL Simulation (R-Multiples)
        if ev.get("trade"):
            trade = ev["trade"]["trade_idea"]
            entry = float(trade["entry"])
            sl = float(trade["stop_loss"])

            # Simple simulation: Did it hit SL?
            # In a real engine, you'd check targets too, but for safety analysis, SL is key.
            hit_sl = False
            if ev["signal"] == "BUY":
                if df[df.Low <= sl].shape[0] > 0: hit_sl = True
            else:
                if df[df.High >= sl].shape[0] > 0: hit_sl = True

            if hit_sl:
                self.stats["losses"] += 1
                self.stats["total_pnl_r"] -= 1.0  # Lost 1R
            elif ai_won:
                self.stats["wins"] += 1
                self.stats["total_pnl_r"] += 1.5  # Assume avg win 1.5R

        elif ev["tech_status"] == "REJECTED" and ai_won:
            self.stats["fomo_wins"] += 1

        del df
        gc.collect()

    def determine_adjustments(self, current_config: BotConfig) -> BotConfig:
        new_config = BotConfig(**asdict(current_config))  # Clone
        actions = []

        total_trades = self.stats["wins"] + self.stats["losses"]
        if total_trades < MIN_TRADES_FOR_ADJUSTMENT:
            logger.info("Not enough trade data to adjust settings safely.")
            return current_config

        win_rate = (self.stats["wins"] / total_trades) if total_trades > 0 else 0
        ai_acc = (self.stats["ai_correct"] / self.stats["ai_total"]) if self.stats["ai_total"] > 0 else 0

        # --- SCENARIO 1: DEFENSE MODE (Low Win Rate) ---
        if win_rate < 0.35:
            # Slash risk, demand higher certainty
            proposed_risk = current_config.RISK_PER_TRADE_PCT * 0.75
            proposed_conf = current_config.AI_MIN_CONFIDENCE + 0.05

            # Apply Bounds
            new_config.RISK_PER_TRADE_PCT = max(BOUNDS["RISK_PER_TRADE_PCT"][0], proposed_risk)
            new_config.AI_MIN_CONFIDENCE = min(BOUNDS["AI_MIN_CONFIDENCE"][1], proposed_conf)
            actions.append(
                f"DEFENSE: Lowered Risk to {new_config.RISK_PER_TRADE_PCT:.4f}, Raised Conf to {new_config.AI_MIN_CONFIDENCE}")

        # --- SCENARIO 2: OFFENSE MODE (High Win Rate) ---
        elif win_rate > 0.60:
            # Incrementally increase risk
            proposed_risk = current_config.RISK_PER_TRADE_PCT * 1.10
            new_config.RISK_PER_TRADE_PCT = min(BOUNDS["RISK_PER_TRADE_PCT"][1], proposed_risk)
            actions.append(f"OFFENSE: Increased Risk to {new_config.RISK_PER_TRADE_PCT:.4f}")

        # --- SCENARIO 3: GROWTH MODE (High AI Accuracy, High FOMO) ---
        elif ai_acc > 0.70 and self.stats["fomo_wins"] > 5:
            # AI is right, but Tech Engine is rejecting too much. Lower barrier.
            proposed_conf = current_config.AI_MIN_CONFIDENCE - 0.05
            new_config.AI_MIN_CONFIDENCE = max(BOUNDS["AI_MIN_CONFIDENCE"][0], proposed_conf)
            actions.append(f"GROWTH: High AI Accuracy ({ai_acc:.2%}). Lowered Conf to {new_config.AI_MIN_CONFIDENCE}")

        self.report["healing_actions"] = actions
        return new_config

    async def run(self):
        logger.info("Performance Healer Running...")

        # 1. Load Current Config
        current_config = await BotConfig.load()

        # 2. Process Data
        async for ev in stream_events():
            await self.process_event(ev)

        # 3. Calculate & Validate New Config
        new_config = self.determine_adjustments(current_config)

        # 4. Save ONLY if changed
        if new_config != current_config:
            await BotConfig.save(new_config)
            logger.info(f"Healing Applied: {self.report['healing_actions']}")
        else:
            logger.info("System healthy. No config changes needed.")

        # 5. Save Report
        self.report["metrics"] = self.stats
        self.report["timestamp"] = datetime.now(timezone.utc)
        await system_performance.insert_one(self.report)
        logger.info("Healer Finished.")


if __name__ == "__main__":
    healer = PerformanceHealer()
    asyncio.run(healer.run())