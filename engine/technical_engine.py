import asyncio
import json
import logging
import re
from datetime import datetime, time
from typing import Dict, Tuple, Optional

import aiohttp
import pandas as pd
import pandas_ta as ta
import pytz
from bson import ObjectId
from utils.angel_one_bridge import angel_bridge
from config import redis_client, trade_signals, technical_audit

IST = pytz.timezone('Asia/Kolkata')
MARKET_OPEN = time(9, 15)
MARKET_CLOSE = time(15, 30)
DEFAULT_ATR_MULTIPLIER = 1.5
LIQUIDITY_THRESHOLDS = {
    "HIGH_CAP": 10_000_000,
    "MID_CAP": 5_000_000,
    "SMALL_CAP": 1_000_000
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TECHNICAL_ENGINE")

def json_serial(obj):
    if isinstance(obj, (datetime, pd.Timestamp)): return obj.isoformat()
    if isinstance(obj, ObjectId): return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")

def get_market_phase() -> str:
    now = datetime.now(IST)
    if now.weekday() >= 5: return "WEEKEND"
    t = now.time()
    if t < MARKET_OPEN: return "PRE_OPEN"
    if MARKET_OPEN <= t <= MARKET_CLOSE: return "LIVE"
    return "POST_MARKET"


class MarketDataService:
    def __init__(self):
        pass

    @staticmethod
    async def get_ohlcv(ticker: str) -> Optional[pd.DataFrame]:
        try:
            df = await asyncio.to_thread(
                angel_bridge.get_historical_candles,
                symbol=ticker,
                interval="FIVE_MINUTE",
                days=5
            )

            if df is None or df.empty or len(df) < 50: return None
            if df.index.tz is None:
                df.index = df.index.tz_localize(IST)
            else:
                df.index = df.index.tz_convert(IST)

            daily_df = await asyncio.to_thread(
                angel_bridge.get_historical_candles,
                symbol=ticker,
                interval="ONE_DAY",
                days=5
            )

            if daily_df is not None and not daily_df.empty:
                if len(daily_df) >= 2:
                    prev_close = daily_df['Close'].iloc[-2]
                else:
                    prev_close = daily_df['Close'].iloc[0]
            else:
                prev_close = df['Close'].iloc[0]

            df.attrs['daily_prev_close'] = float(prev_close)
            return df

        except Exception as e:
            logger.warning(f"Fetch failed for {ticker}: {e}")
            return None

    @staticmethod
    async def get_realtime_price(ticker):
        return await asyncio.to_thread(angel_bridge.get_ltp, ticker)

    async def get_market_trend(self) -> str:
        try:
            df = await self.get_ohlcv("NIFTY")
            if df is None: return "NEUTRAL"
            close = df['Close'].iloc[-1]
            sma50 = df['Close'].rolling(50).mean().iloc[-1]
            if close > (sma50 * 1.001): return "BULLISH"
            if close < (sma50 * 0.999): return "BEARISH"
            return "NEUTRAL"
        except:
            return "NEUTRAL"

class SymbolResolver:
    def __init__(self):
        self.session = None
        self.nse_map = {}
        self.bse_map = {}
        self.list_map = {}
        self.session = None

    async def initialize(self):
        tasks = [
            redis_client.hgetall("CONFIG:ISIN:NSE"),
            redis_client.hgetall("CONFIG:ISIN:BSE"),
            redis_client.hgetall("CONFIG:ISIN:SYMBOL")
        ]
        results = await asyncio.gather(*tasks)

        def safe_parse(raw_dict):
            parsed = {}
            if not raw_dict: return parsed

            for k, v in raw_dict.items():
                key = k.decode() if isinstance(k, bytes) else k
                try:
                    val_str = v.decode() if isinstance(v, bytes) else v
                    if isinstance(val_str, str) and (val_str.startswith("{") or val_str.startswith("[")):
                        parsed[key] = json.loads(val_str)
                    else:
                        parsed[key] = val_str
                except:
                    parsed[key] = v
            return parsed
        self.nse_map = safe_parse(results[0])
        self.bse_map = safe_parse(results[1])
        self.list_map = safe_parse(results[2])
        self.session = aiohttp.ClientSession(headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        logger.info(f"Resolver Loaded: {len(self.list_map)} mappings")

    async def _search_yahoo(self, query: str) -> Optional[str]:
        async def fetch(q_str):
            clean_q = re.sub(r'[&.-]', ' ', q_str)
            clean_q = re.sub(r'\s+', ' ', clean_q).strip()

            url = "https://query2.finance.yahoo.com/v1/finance/search"
            params = {"q": clean_q, "quotesCount": 10, "newsCount": 0, "enableFuzzyQuery": "false"}

            try:
                async with self.session.get(url, params=params, timeout=5) as resp:
                    if resp.status != 200: return None
                    data = await resp.json()

                    if "quotes" in data and len(data["quotes"]) > 0:
                        for quote in data["quotes"]:

                            quote_type = quote.get("quoteType", "EQUITY")
                            if quote_type not in ["EQUITY", "INDEX", "CURRENCY"]:
                                continue

                            symbol = quote.get("symbol", "")
                            if ".NS" in symbol or ".BO" in symbol:
                                logger.info(f"Yahoo Found: '{q_str}' -> {symbol}")
                                return symbol
            except Exception as e:
                logger.warning(f"Search Error: {e}")
            return None

        result = await fetch(query)
        if result: return result

        words = query.split()
        if len(words) > 2:
            short_query = " ".join(words[:2])
            logger.info(f"Retrying Yahoo with shorter query: '{short_query}'")
            result = await fetch(short_query)
            if result: return result

        return None

    async def resolve(self, event: dict) -> Tuple[str, str]:
        # 1. ISIN Lookup
        isin = event.get('isin')
        if isin:
            if isin in self.nse_map:
                data = self.nse_map[isin]
                if isinstance(data, dict): return data.get("nse_symbol", ""), "NSE"

            if isin in self.list_map:
                symbols = self.list_map[isin]
                if isinstance(symbols, list):
                    nse = next((s for s in symbols if ".NS" in s), None)
                    if nse: return nse, "NSE"
                    if symbols: return symbols[0], "BSE"

        # 2. Yahoo Search
        raw_name = event.get('clean_name', '').replace(" LIMITED", "").replace(" LTD", "").strip()
        search_result = await self._search_yahoo(raw_name)
        if search_result:
            return search_result, "NSE" if ".NS" in search_result else "BSE"

        # 3. Regex
        clean = re.sub(r'[^A-Z0-9]', '', event.get('clean_name', '').upper())
        return f"{clean}.NS", "NSE"

    async def close(self):
        if self.session: await self.session.close()


class IndicatorEngine:
    @staticmethod
    def analyze(df: pd.DataFrame) -> Optional[Dict]:
        try:
            df = df.copy()

            # 1. VWAP
            df['date'] = df.index.date
            df['tpv'] = (df['High'] + df['Low'] + df['Close']) / 3 * df['Volume']
            df['vwap'] = df.groupby('date')['tpv'].cumsum() / df.groupby('date')['Volume'].cumsum()

            # 2. Advanced Indicators
            df['RSI_14'] = ta.rsi(df['Close'], length=14)
            df['ATRr_14'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)
            df['SMA_20'] = ta.sma(df['Close'], length=20)

            last = df.iloc[-1]

            daily_prev_close = df.attrs.get('daily_prev_close', last['Close'])
            daily_change_pct = (last['Close'] - daily_prev_close) / daily_prev_close

            # 3. Relative Volume (RVOL)
            avg_vol = df['Volume'].rolling(20).mean().iloc[-1]
            rvol = last['Volume'] / avg_vol if avg_vol > 0 else 1.0
            turnover = last['Close'] * avg_vol

            return {
                "price": float(last['Close']),
                "daily_prev_close": float(daily_prev_close),
                "daily_change_pct": float(daily_change_pct),
                "vwap": float(last['vwap']),
                "rsi": float(last['RSI_14']),
                "atr": float(last['ATRr_14']),
                "sma_20": float(last['SMA_20']),
                "rvol": float(rvol),
                "turnover": float(turnover),
                "timestamp": df.index[-1].to_pydatetime()
            }
        except Exception as e:
            logger.error(f"Indicator Error: {e}")
            return None

class TechnicalEngine:
    def __init__(self):
        self.resolver = SymbolResolver()
        self.data = MarketDataService()
        self.indicators = IndicatorEngine()
        self.input_queue = "QUEUE:AI_SIGNALS"
        self.output_queue = "QUEUE:TRADE_SIGNALS"
        self.liq_small_cap = 1_000_000
        self.atr_multiplier = 1.5
        self.rvol_threshold = 2.0
        self.chase_pct = 0.04

    async def initialize(self):
        await self.resolver.initialize()
        await angel_bridge.initialize()
        logger.info("Technical Engine Ready")

    async def update_config(self):
        try:
            liq = await redis_client.get("CONFIG:DYNAMIC:LIQUIDITY_SMALL_CAP")
            if liq: self.liq_small_cap = int(liq)

            atr = await redis_client.get("CONFIG:DYNAMIC:ATR_MULTIPLIER")
            if atr: self.atr_multiplier = float(atr)

            rvol = await redis_client.get("CONFIG:DYNAMIC:RVOL_THRESHOLD")
            if rvol: self.rvol_threshold = float(rvol)

            chase = await redis_client.get("CONFIG:DYNAMIC:CHASE_PROTECTION_PCT")
            if chase: self.chase_pct = float(chase)

            logger.info(
                f"CONFIG: Liq={self.liq_small_cap}, ATR={self.atr_multiplier}, RVOL={self.rvol_threshold}, Chase={self.chase_pct}")
        except Exception:
            pass

    @staticmethod
    def _validate_trade(stats, ai, phase, market_trend, liq_threshold, rvol_threshold, chase_pct) -> Tuple[str, str]:

        daily_move = stats['daily_change_pct']

        # Prevent Shorting if stock crashed > 9% (Trap Risk)
        if ai['signal'] == "SELL" and daily_move < -0.09:
            return "BLOCKED", f"CIRCUIT_RISK_DOWN_{daily_move * 100:.1f}%"

        # Prevent Buying if stock rallied > 12% (Trap Risk)
        if ai['signal'] == "BUY" and daily_move > 0.12:
            return "BLOCKED", f"CIRCUIT_RISK_UP_{daily_move * 100:.1f}%"

        # --- A. Pump & Dump Shield ---
        is_small_cap = stats['turnover'] < 5_000_000
        if is_small_cap and stats['rvol'] > 5.0 and ai.get('tier') != "EXTREME":
            return "BLOCKED", f"PUMP_DUMP_RISK_RVOL_{stats['rvol']:.1f}"

        # --- B. Liquidity Check ---
        min_liq = liq_threshold
        if stats['price'] > 500:
            min_liq = max(min_liq * 5, 5_000_000)

        if stats['turnover'] < min_liq:
            return "BLOCKED", f"LOW_LIQ_₹{int(stats['turnover'] / 1000)}k"

        # --- C. Chase Protection
        daily_chg = abs(stats['daily_change_pct'])
        if daily_chg > chase_pct and ai.get('tier') != "EXTREME":
            return "BLOCKED", f"MOVE_DONE_{daily_chg * 100:.1f}%"

        # --- D. Market Confluence ---
        tier = ai.get('tier', 'MODERATE')
        signal = ai['signal']

        if tier == "EXTREME":
            pass  # Free Pass

        elif tier == "VERY":
            is_headwind = (market_trend == "BEARISH" and signal == "BUY") or \
                          (market_trend == "BULLISH" and signal == "SELL")

            if is_headwind:
                # Dynamic RVOL check
                if stats['rvol'] < rvol_threshold:
                    return "BLOCKED", f"HEADWIND_LOW_VOL_{stats['rvol']:.1f}"

                # Headwind specific safety
                if signal == "BUY" and stats['price'] < stats['vwap']:
                    return "BLOCKED", "HEADWIND_BELOW_VWAP"
                if signal == "SELL" and stats['price'] > stats['vwap']:
                    return "BLOCKED", "HEADWIND_ABOVE_VWAP"

        else:  # MODERATE
            if market_trend == "BEARISH" and signal == "BUY":
                return "BLOCKED", "MARKET_HEADWIND"
            if market_trend == "BULLISH" and signal == "SELL":
                return "BLOCKED", "MARKET_HEADWIND"

        # --- E. Technical Setup ---
        if signal == "BUY":
            if stats['price'] > (stats['sma_20'] * 1.10): return "BLOCKED", "OVEREXTENDED_10%"
            if stats['rsi'] > 75: return "BLOCKED", f"RSI_HOT_{int(stats['rsi'])}"

            # VWAP Support
            if phase == "LIVE" and stats['price'] < stats['vwap'] and tier not in ["EXTREME", "VERY"]:
                return "BLOCKED", "BELOW_VWAP"

        elif signal == "SELL":
            if stats['rsi'] < 25: return "BLOCKED", f"RSI_COLD_{int(stats['rsi'])}"

            # VWAP Resistance
            if phase == "LIVE" and stats['price'] > stats['vwap'] and tier not in ["EXTREME", "VERY"]:
                return "BLOCKED", "ABOVE_VWAP"

        return "EXECUTED", f"VALID_{phase}"

    @staticmethod
    def _construct_order(ticker, ai, stats, reason, phase, atr_mult, event_id):
        limit_price, sl, tp = 0,0,0
        price = stats['price']
        atr = stats['atr']
        tier = ai.get('tier', 'MODERATE')

        # 1. Determine Tolerance (Slippage protection)
        gap_tolerance = 0.02 if tier == "EXTREME" else 0.01

        # 2. Calculate Limit Price (The Boundary)
        if ai['signal'] == "BUY":
            limit_price = price * (1 + gap_tolerance)

        elif ai['signal'] == "SELL":
            limit_price = price * (1 - gap_tolerance)

        # 3. Order Type
        order_type = "LIMIT"

        # 4. Dynamic Stops & Targets
        base_mult = atr_mult
        final_mult = base_mult + 0.5 if tier == "EXTREME" else base_mult

        if ai['signal'] == "BUY":
            sl = price - (atr * final_mult)
            tp = price + (atr * final_mult * 2.5)
        elif ai['signal'] == "SELL":
            sl = price + (atr * final_mult)
            tp = price - (atr * final_mult * 2.5)

        return {
            "event_id": event_id,
            "symbol": ticker,
            "signal": ai['signal'],
            "order_type": order_type,
            "entry_ref": round(price, 2),
            "limit_price": round(limit_price, 2),
            "stop_loss": round(sl, 2),
            "take_profit": round(tp, 2),
            "quantity_score": 10 if tier == "EXTREME" else 5,
            "reason": reason,
            "catalyst": ai.get('catalyst'),
            "phase": phase,
            "timestamp": datetime.now(IST)
        }

    @staticmethod
    async def _audit(event, ticker, decision, reason, phase, stats=None):
        try:
            doc = {
                "event_id": event.get('event_id'),
                "ticker": ticker,
                "decision": decision,
                "reason": reason,
                "phase": phase,
                "stats": stats,
                "ai_tier": event.get('ai_analysis', {}).get('tier'),
                "timestamp": datetime.now(IST)
            }
            await technical_audit.insert_one(doc)
        except Exception as e:
            logger.error(f"Audit Error: {e}")


    async def process_event(self, event: dict):
        ticker, exch = await self.resolver.resolve(event)
        phase = get_market_phase()
        ai = event.get('ai_analysis', {})

        logger.info(f"Processing: {ticker} | AI: {ai.get('signal')}")

        df = await self.data.get_ohlcv(ticker)

        if df is None or df.empty:
            await self._audit(event, ticker, "BLOCKED", "DATA_UNAVAILABLE", phase)
            return

        market_trend_task = asyncio.create_task(self.data.get_market_trend())
        market_trend = await market_trend_task

        stats = self.indicators.analyze(df)
        if not stats:
            await self._audit(event, ticker, "BLOCKED", "CALC_ERROR", phase)
            return

        decision, reason = self._validate_trade(
            stats, ai, phase, market_trend,
            liq_threshold=self.liq_small_cap,
            rvol_threshold=self.rvol_threshold,
            chase_pct=self.chase_pct
        )
        await self._audit(event, ticker, decision, reason, phase, stats)

        if decision == "EXECUTED":
            signal = self._construct_order(
                ticker, ai, stats, reason, phase,
                atr_mult=self.atr_multiplier,
                event_id=event.get('event_id')
            )
            await redis_client.rpush(self.output_queue, json.dumps(signal, default=json_serial))
            await trade_signals.insert_one(signal)
            logger.info(f"SIGNAL: {ticker} ({ai['signal']}) | {reason}")
        else:
            logger.info(f"BLOCKED: {ticker} | {reason}")

    async def run(self):
        await self.initialize()
        logger.info("Technical Engine Listening...")
        try:
            while True:
                try:
                    item = await redis_client.blpop(self.input_queue, timeout=2)
                    if item:
                        asyncio.create_task(self.process_event(json.loads(item[1])))
                    else:
                        await asyncio.sleep(0.1)
                except Exception as e:
                    logger.error(f"Loop Error: {e}")
                    await asyncio.sleep(1)
        finally:
            await self.resolver.close()


if __name__ == "__main__":
    engine = TechnicalEngine()
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        pass