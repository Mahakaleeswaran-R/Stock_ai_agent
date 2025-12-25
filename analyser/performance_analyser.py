import asyncio
import json
import logging
import pandas as pd
from datetime import datetime, timedelta, time
import pytz
from typing import Dict, AsyncGenerator, Any

from config import redis_client, ai_audit, technical_audit, trade_signals, system_performance
from engine.technical_engine import SymbolResolver
from utils.angel_one_bridge import angel_bridge

IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("PERFORMANCE_ANALYZER")


SAFETY_LIMITS = {
    "MIN_LIQUIDITY_CAP": 500_000,
    "MAX_ATR_MULTIPLIER": 3.5,
    "MIN_ATR_MULTIPLIER": 1.0,
    "MIN_AI_CONFIDENCE": 0.55,
    "MAX_AI_CONFIDENCE": 0.95,
    "MIN_RVOL": 1.5,
    "MAX_RVOL": 5.0,
    "MAX_CHASE_PCT": 0.10,
    "MIN_CHASE_PCT": 0.02
}


class PerformanceAnalyzer:
    def __init__(self):
        self.market_data_cache = {}
        self.resolver = SymbolResolver()

        # Performance Counters
        self.stats = {
            "ai_correct": 0, "ai_total": 0,
            "fomo_count": 0, "wins": 0, "losses": 0,
            "total_pnl": 0.0
        }

        self.report: Dict[str, Any] = {
            "date": datetime.now(IST).strftime("%Y-%m-%d"),
            "metrics": {},
            "proposed_changes": {},
            "timestamp": None
        }

    async def initialize(self):
        await self.resolver.initialize()
        await angel_bridge.initialize()

    async def fetch_intraday_truth(self, ticker: str, trade_date: datetime) -> pd.DataFrame:
        date_key = trade_date.strftime("%Y-%m-%d")
        cache_key = f"{ticker}_{date_key}_5m"

        if cache_key in self.market_data_cache:
            return self.market_data_cache[cache_key]

        try:
            df = await asyncio.to_thread(
                angel_bridge.get_historical_candles,
                symbol=ticker,
                interval="FIVE_MINUTE",
                days=5
            )

            if df is None or df.empty: return pd.DataFrame()

            if df.index.tz is None:
                df.index = df.index.tz_localize(IST)
            else:
                df.index = df.index.tz_convert(IST)

            target_date = trade_date.date()
            day_df = df[df.index.date == target_date].copy()
            day_df.sort_index(inplace=True)

            self.market_data_cache[cache_key] = day_df
            return day_df
        except Exception as e:
            logger.warning(f"Data Fetch Error {ticker}: {e}")
            return pd.DataFrame()


    async def stream_events(self, lookback_days=1) -> AsyncGenerator[Dict, None]:
        start_time = datetime.now(IST) - timedelta(days=lookback_days)
        logger.info(f"Streaming events from {start_time.strftime('%Y-%m-%d %H:%M')}...")

        cursor = ai_audit.find({"timestamp": {"$gte": start_time}}).sort("timestamp", -1)

        async for ai_doc in cursor:
            event_id = ai_doc.get("_id")
            tech_doc = await technical_audit.find_one({"event_id": event_id})
            trade_doc = await trade_signals.find_one({"event_id": event_id})

            ticker = tech_doc.get("ticker") if tech_doc else None
            if not ticker:
                dummy = {"clean_name": ai_doc.get("symbol", ""), "isin": ai_doc.get("isin", "")}
                ticker, _ = await self.resolver.resolve(dummy)

            ts = ai_doc.get("timestamp").replace(tzinfo=IST)
            if ts.time() > time(15, 30):
                trade_date = ts + timedelta(days=1)
            else:
                trade_date = ts

            yield {
                "id": event_id,
                "timestamp": ts,
                "trade_date": trade_date,
                "ticker": ticker,
                "ai": {"signal": ai_doc.get("ai_signal"), "confidence": ai_doc.get("ai_confidence", 0)},
                "tech": {
                    "decision": tech_doc.get("decision") if tech_doc else "SKIPPED",
                    "reason": tech_doc.get("reason") if tech_doc else None
                },
                "trade": {
                    "status": "EXECUTED" if trade_doc else "NONE",
                    "entry": float(trade_doc.get("entry_ref", 0)) if trade_doc else 0.0,
                    "tp": float(trade_doc.get("take_profit", 0)) if trade_doc else 0.0,
                    "sl": float(trade_doc.get("stop_loss", 0)) if trade_doc else 0.0,
                    "signal": trade_doc.get("signal") if trade_doc else None,
                    "db_id": trade_doc.get("_id") if trade_doc else None
                }
            }

    async def process_single_event(self, ev: Dict):
        if ev['ai']['signal'] in ["HOLD", "ERROR"] or not ev['ticker']: return

        # 1. Fetch 5-Minute Candles (The Truth)
        df = await self.fetch_intraday_truth(ev['ticker'], ev['trade_date'])

        if df.empty:
            logger.warning(f"No intraday data for {ev['ticker']}")
            return

        try:
            # --- AI Accuracy Check (Directional Correctness) ---
            # We compare the first candle's Open to the last candle's Close
            first_candle = df.iloc[0]
            last_candle = df.iloc[-1]
            day_change = (last_candle['Close'] - first_candle['Open']) / first_candle['Open']

            is_correct = (ev['ai']['signal'] == "BUY" and day_change > 0.005) or \
                         (ev['ai']['signal'] == "SELL" and day_change < -0.005)

            if is_correct: self.stats["ai_correct"] += 1
            self.stats["ai_total"] += 1

            # --- FOMO Check ---
            # If Tech blocked it, but it moved > 4% in the correct direction
            if ev['tech']['decision'] == "BLOCKED" and is_correct and abs(day_change) > 0.04:
                self.stats["fomo_count"] += 1
                logger.warning(f"FOMO: {ev['ticker']} blocked but moved {day_change:.2%}")

            # --- Trade Grading (Replay the Tape) ---
            if ev['trade']['status'] == "EXECUTED":
                signal = ev['trade']['signal']
                entry = ev['trade']['entry']
                sl, tp = ev['trade']['sl'], ev['trade']['tp']

                outcome = "OPEN"
                realized_pnl = 0.0

                # REPLAY THE DAY CANDLE BY CANDLE
                for index, candle in df.iterrows():
                    hi, lo = float(candle['High']), float(candle['Low'])
                    op, cl = float(candle['Open']), float(candle['Close'])

                    if signal == "BUY":
                        # AMBIGUITY CHECK: Both SL and TP in range of this candle
                        if lo <= sl and hi >= tp:
                            # Heuristic: If candle is Green (Close > Open), assume TP hit first.
                            if cl > op:
                                outcome = "WIN"
                                realized_pnl = (tp - entry) / entry
                            else:
                                outcome = "LOSS"
                                realized_pnl = (sl - entry) / entry
                            break

                        # Standard Checks
                        elif lo <= sl:
                            outcome = "LOSS"
                            realized_pnl = (sl - entry) / entry
                            break
                        elif hi >= tp:
                            outcome = "WIN"
                            realized_pnl = (tp - entry) / entry
                            break

                    elif signal == "SELL":
                        # AMBIGUITY CHECK
                        if hi >= sl and lo <= tp:
                            # Heuristic: If Red (Close < Open), assume TP hit first.
                            if cl < op:
                                outcome = "WIN"
                                realized_pnl = (entry - tp) / entry
                            else:
                                outcome = "LOSS"
                                realized_pnl = (entry - sl) / entry
                            break

                        # Standard Checks
                        elif hi >= sl:
                            outcome = "LOSS"
                            realized_pnl = (entry - sl) / entry
                            break
                        elif lo <= tp:
                            outcome = "WIN"
                            realized_pnl = (entry - tp) / entry
                            break

                # If loop finishes without SL or TP, mark based on Last Close
                if outcome == "OPEN":
                    last_price = df.iloc[-1]['Close']
                    if signal == "BUY":
                        realized_pnl = (last_price - entry) / entry
                    else:
                        realized_pnl = (entry - last_price) / entry
                    outcome = "WIN" if realized_pnl > 0 else "LOSS"

                # Stats Update
                if outcome == "WIN":
                    self.stats["wins"] += 1
                elif outcome == "LOSS":
                    self.stats["losses"] += 1
                self.stats["total_pnl"] += realized_pnl

                # Feedback Update
                if ev['trade']['db_id']:
                    await trade_signals.update_one(
                        {"_id": ev['trade']['db_id']},
                        {"$set": {
                            "performance_grade": outcome,
                            "realized_pnl_pct": round(realized_pnl * 100, 2),
                            "analyzed_at": datetime.now(IST)
                        }}
                    )

        except Exception as e:
            logger.error(f"Err {ev['ticker']}: {e}")

    async def calculate_tuning(self):
        # 1. Fetch Current Configs
        c = {
            "AI_CONF": float(await redis_client.get("CONFIG:DYNAMIC:AI_CONFIDENCE_THRESHOLD") or 0.65),
            "LIQ": int(await redis_client.get("CONFIG:DYNAMIC:LIQUIDITY_SMALL_CAP") or 1_000_000),
            "ATR": float(await redis_client.get("CONFIG:DYNAMIC:ATR_MULTIPLIER") or 1.5),
            "RVOL": float(await redis_client.get("CONFIG:DYNAMIC:RVOL_THRESHOLD") or 2.0),
            "CHASE": float(await redis_client.get("CONFIG:DYNAMIC:CHASE_PROTECTION_PCT") or 0.04)
        }

        changes = {}

        # 2. Calculate Hard Metrics
        ai_total = self.stats["ai_total"]
        ai_acc = (self.stats["ai_correct"] / ai_total * 100) if ai_total > 0 else 0

        total_trades = self.stats["wins"] + self.stats["losses"]
        win_rate = (self.stats["wins"] / total_trades * 100) if total_trades > 0 else 0

        # PnL is the ultimate truth
        avg_pnl = (self.stats["total_pnl"] / total_trades * 100) if total_trades > 0 else 0.0

        self.report['metrics'] = {
            "ai_accuracy": round(ai_acc, 2),
            "fomo_count": self.stats["fomo_count"],
            "trade_win_rate": round(win_rate, 2),
            "avg_profit_pct": round(avg_pnl, 2)
        }

        logger.info(
            f"📊 METRICS: Acc={ai_acc}% | WinRate={win_rate}% | PnL={avg_pnl}% | FOMO={self.stats['fomo_count']}")

        # ZONE 1: EMERGENCY BRAKE (Capital Protection)
        is_danger_zone = False

        # Rule 1: AI Confusion Defense (FIXED: Handles 0% Accuracy)
        # If we had signals (ai_total > 0) AND accuracy is bad (< 55%), we tighten.
        if ai_total > 0 and ai_acc < 55:
            is_danger_zone = True
            new_val = min(c["AI_CONF"] + 0.05, SAFETY_LIMITS['MAX_AI_CONFIDENCE'])
            if new_val != c["AI_CONF"]:
                changes["AI_CONFIDENCE_THRESHOLD"] = {"value": round(new_val, 2),
                                                      "reason": f"Low AI Accuracy {ai_acc:.1f}%"}

        # Rule 2: Drawdown Defense (PnL Protection)
        # If losing money OR win rate is abysmal, we tighten Technicals.
        if avg_pnl < 0 or (total_trades > 0 and win_rate < 40):
            is_danger_zone = True

            # Raise Liquidity Floor immediately (Stop trading junk)
            new_liq = min(int(c["LIQ"] * 1.2), 2_000_000)
            if new_liq != c["LIQ"]:
                changes["LIQUIDITY_SMALL_CAP"] = {"value": new_liq, "reason": f"Negative PnL {avg_pnl:.2f}%"}

            # Widen Stops to prevent chop
            new_atr = min(c["ATR"] + 0.5, SAFETY_LIMITS['MAX_ATR_MULTIPLIER'])
            if new_atr != c["ATR"]:
                changes["ATR_MULTIPLIER"] = {"value": round(new_atr, 1), "reason": "Stop Loss Protection"}

        # STOP: If we are in danger, we DO NOT loosen anything.
        if is_danger_zone:
            self.report['proposed_changes'] = changes
            return

        # ZONE 2: OPTIMIZATION (The "Ratchet")
        # Entered ONLY if: PnL >= 0 AND AI Accuracy >= 55%

        # Rule 3: High FOMO -> Careful Loosening
        # We only lower standards if we are missing too many winners.
        if self.stats["fomo_count"] >= 3:
            # Decay Liquidity by 10% (Slow Gas)
            new_liq = max(int(c["LIQ"] * 0.9), SAFETY_LIMITS['MIN_LIQUIDITY_CAP'])
            if new_liq != c["LIQ"]:
                changes["LIQUIDITY_SMALL_CAP"] = {"value": new_liq,
                                                  "reason": f"Catch FOMO ({self.stats['fomo_count']})"}

            # Lower RVOL by 0.1 (Slow Gas)
            new_rvol = max(c["RVOL"] - 0.1, SAFETY_LIMITS['MIN_RVOL'])
            if new_rvol != c["RVOL"]:
                changes["RVOL_THRESHOLD"] = {"value": round(new_rvol, 1), "reason": "Catch FOMO"}

        # Rule 4: High Win Rate -> Aggressive Profit Taking
        if win_rate > 75 and total_trades >= 3:
            # Tighten stops to lock profit (Trailing Stop effect)
            new_atr = max(c["ATR"] - 0.1, SAFETY_LIMITS['MIN_ATR_MULTIPLIER'])
            if new_atr != c["ATR"]:
                changes["ATR_MULTIPLIER"] = {"value": round(new_atr, 1), "reason": "Optimizing High WinRate"}

            # Relax AI Confidence slightly to find more trades
            if ai_acc > 85:
                new_conf = max(c["AI_CONF"] - 0.02, SAFETY_LIMITS['MIN_AI_CONFIDENCE'])
                if new_conf != c["AI_CONF"]:
                    changes["AI_CONFIDENCE_THRESHOLD"] = {"value": round(new_conf, 2), "reason": "AI High Conviction"}

        self.report['proposed_changes'] = changes
    async def run(self):
        logger.info("Analyzer Started (Streaming Mode)")
        await self.initialize()

        try:
            async for event in self.stream_events(lookback_days=1):
                await self.process_single_event(event)

            await self.calculate_tuning()

            # Apply Tuning
            if self.report['proposed_changes']:
                async with redis_client.pipeline() as pipe:
                    for k, v in self.report['proposed_changes'].items():
                        key = f"CONFIG:DYNAMIC:{k}"
                        pipe.set(key, str(v['value']))
                        logger.info(f"TUNING: {k} -> {v['value']}")
                    await pipe.execute()

            # Archive Report
            self.report['timestamp'] = datetime.now(IST)
            await system_performance.insert_one(self.report)
            await redis_client.set("REPORT:LATEST", json.dumps(self.report, default=str))
            logger.info(f"Audit Complete. Metrics: {json.dumps(self.report['metrics'])}")

        except Exception as e:
            logger.error(f"Analyzer Error: {e}")
        finally:
            await self.resolver.close()


if __name__ == "__main__":
    analyzer = PerformanceAnalyzer()
    try:
        asyncio.run(analyzer.run())
    except KeyboardInterrupt:
        pass