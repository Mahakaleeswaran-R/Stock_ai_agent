import gc

import pandas as pd
import pyotp
import requests
import logging
import asyncio
import pytz
from datetime import datetime, timedelta, timezone
from SmartApi import SmartConnect
from tenacity import AsyncRetrying, stop_after_attempt, wait_fixed

from config import ANGEL_API_KEY, ANGEL_TOTP_KEY, ANGEL_CLIENT_ID, ANGEL_PIN

logger = logging.getLogger("ANGEL_BRIDGE")
logging.basicConfig(level=logging.INFO)

IST = pytz.timezone("Asia/Kolkata")


class AngelBridge:
    _instance = None

    def __init__(self):
        self.last_login_time = None
        self.api = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.api = None
            cls._instance.token_map = {}
            cls._instance.last_login_time = None
            cls._instance.login_lock = asyncio.Lock()
            cls._instance.api_lock = asyncio.Lock()
        return cls._instance

    async def initialize(self, force_refresh=False):
        async with self.login_lock:
            now = datetime.now(timezone.utc)
            session_age = (
                (now - self.last_login_time).total_seconds()
                if self.last_login_time else float("inf")
            )

            if self.api and session_age < 72000 and self.token_map and not force_refresh:
                return

            logger.info("Initializing Angel One session")

            try:
                self.api = SmartConnect(api_key=ANGEL_API_KEY)
                totp = pyotp.TOTP(ANGEL_TOTP_KEY).now()

                data = await asyncio.to_thread(
                    self.api.generateSession,
                    ANGEL_CLIENT_ID,
                    ANGEL_PIN,
                    totp
                )

                if not data.get("status"):
                    raise Exception(data.get("message"))

                self.last_login_time = now
                logger.info("Angel One login successful")

            except Exception as e:
                logger.critical(f"Angel login failed: {e}")
                raise

            await self._load_scrip_master()

    async def _load_scrip_master(self):
        logger.info("Loading Angel One scrip master")
        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

        try:
            # Optimize: Stream the request if possible, but requests.json() loads all in memory.
            data = await asyncio.to_thread(lambda: requests.get(url, timeout=30).json())

            self.token_map.clear()
            count = 0

            for s in data:
                # OPTIMIZATION: STRICT FILTERING TO SAVE RAM
                # Only keep NSE Equity and BSE Equity. Drop everything else.
                exch = s.get("exch_seg")
                if exch not in ["NSE", "BSE"]:
                    continue

                # Further filter: Ignore weird series if possible, but exch check is biggest saver
                symbol = s.get("symbol")
                token = s.get("token")

                if not symbol or not token: continue

                # Normalization Logic
                if exch == "NSE":
                    # Angel symbols often have '-EQ'. Strip it for cleaner matching.
                    root_symbol = symbol.split('-')[0]
                    self.token_map[f"{root_symbol}.NS"] = {"token": token, "exch": "NSE"}
                elif exch == "BSE":
                    self.token_map[symbol + ".BO"] = {"token": token, "exch": "BSE"}

                count += 1

            del data
            gc.collect()

            # Add Indices Manually
            self.token_map["NIFTY"] = {"token": "99926000", "exch": "NSE"}
            self.token_map["BANKNIFTY"] = {"token": "99926009", "exch": "NSE"}

            logger.info(f"Scrip master loaded: {count} symbols")

        except Exception as e:
            logger.critical(f"Failed to load scrip master: {e}")

    def get_token_info(self, symbol: str):
        if "." not in symbol and symbol not in ("NIFTY", "BANKNIFTY"):
            symbol += ".NS"

        info = self.token_map.get(symbol)
        if info:
            return info["token"], info["exch"]

        return None, None


    async def get_historical_candles(self, symbol, interval="FIVE_MINUTE", days=5):
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(3),
            wait=wait_fixed(1),
            reraise=False
        ):
            with attempt:
                return await self._fetch_candles(symbol, interval, days)

        return None

    async def _fetch_candles(self, symbol, interval, days):
        token, exch = self.get_token_info(symbol)
        if not token:
            logger.error(f"Token not found: {symbol}")
            return None

        to_dt = datetime.now(timezone.utc)
        from_dt = to_dt - timedelta(days=days)

        params = {
            "exchange": exch,
            "symboltoken": token,
            "interval": interval,
            "fromdate": from_dt.strftime("%Y-%m-%d %H:%M"),
            "todate": to_dt.strftime("%Y-%m-%d %H:%M")
        }

        async with self.api_lock:
            response = await asyncio.to_thread(
                self.api.getCandleData,
                params
            )

        if response.get("errorCode") == "AG8001":
            logger.warning("Session expired — reinitializing")
            await self.initialize(force_refresh=True)
            return await self._fetch_candles(symbol, interval, days)

        if not response.get("status") or not response.get("data"):
            logger.warning(f"No candle data for {symbol}")
            return None

        df = pd.DataFrame(
            response["data"],
            columns=["Date", "Open", "High", "Low", "Close", "Volume"]
        )

        df["Date"] = pd.to_datetime(df["Date"])
        df.set_index("Date", inplace=True)
        if df.index.tz is None:
            df.index = df.index.tz_localize(IST)
        else:
            df.index = df.index.tz_convert(IST)

        df.set_index("Date", inplace=True)

        df[["Open", "High", "Low", "Close", "Volume"]] = df[
            ["Open", "High", "Low", "Close", "Volume"]
        ].apply(pd.to_numeric)

        return df

    async def get_ltp(self, symbol):
        token, exch = self.get_token_info(symbol)
        if not token:
            return None

        async with self.api_lock:
            try:
                data = await asyncio.to_thread(
                    self.api.ltpData,
                    exch,
                    symbol.replace(".NS", "").replace(".BO", ""),
                    token
                )
                if data.get("status"):
                    return float(data["data"]["ltp"])
            except Exception as e:
                logger.error(f"LTP ERROR {symbol}: {e}")
                return None

        return None


angel_bridge = AngelBridge()
