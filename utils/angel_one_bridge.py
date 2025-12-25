import pandas as pd
import pyotp
import requests
import logging
import asyncio
from datetime import datetime, timedelta
from SmartApi import SmartConnect
from tenacity import retry, stop_after_attempt, wait_fixed

from config import ANGEL_API_KEY, ANGEL_TOTP_KEY, ANGEL_CLIENT_ID, ANGEL_PIN

logger = logging.getLogger("ANGEL_BRIDGE")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')



class AngelBridge:
    _instance = None

    def __init__(self):
        self.api = None
        self.last_login_time = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AngelBridge, cls).__new__(cls)
            cls._instance.api = None
            cls._instance.token_map = {}
            cls._instance.last_login_time = None
        return cls._instance

    async def initialize(self, force_refresh=False):
        session_age = 0
        if self.last_login_time:
            session_age = (datetime.now() - self.last_login_time).total_seconds()

        if not self.api or session_age > 72000:
            try:
                self.api = SmartConnect(api_key=ANGEL_API_KEY)
                totp_val = pyotp.TOTP(ANGEL_TOTP_KEY).now()
                data = self.api.generateSession(ANGEL_CLIENT_ID, ANGEL_PIN, totp_val)
                if data['status'] is False:
                    raise Exception(f"Login Failed: {data['message']}")
                self.last_login_time = datetime.now()
                logger.info("Angel One Login Successful")
            except Exception as e:
                logger.error(f"Angel Login Critical Error: {e}")
                raise

            if force_refresh or not self.token_map:
                logger.info("Refreshing Token Map (NSE & BSE)...")
                self.token_map.clear()

                url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
                scrip_data = await asyncio.to_thread(requests.get, url)
                scrip_data = scrip_data.json()

                for scrip in scrip_data:
                    symbol = scrip['symbol']
                    token = scrip['token']
                    exch = scrip['exch_seg']
                    if exch == 'NSE' and '-EQ' in symbol:
                        clean_sym = symbol.replace('-EQ', '') + ".NS"
                        self.token_map[clean_sym] = {"token": token, "exch": "NSE"}
                    elif exch == 'BSE':
                        clean_sym = symbol + ".BO"
                        self.token_map[clean_sym] = {"token": token, "exch": "BSE"}
                self.token_map['NIFTY'] = {"token": "99926000", "exch": "NSE"}
                self.token_map['BANKNIFTY'] = {"token": "99926009", "exch": "NSE"}

                logger.info(f"Token Map Updated: {len(self.token_map)} Scrips")


    def get_token_info(self, symbol):
        if "." not in symbol and symbol not in ["NIFTY", "BANKNIFTY"]:
            symbol = symbol + ".NS"

        info = self.token_map.get(symbol)
        if info:
            return info['token'], info['exch']
        return None, None

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def get_historical_candles(self, symbol, interval="FIVE_MINUTE", days=5):
        token, exch = self.get_token_info(symbol)
        if not token:
            logger.error(f"Token not found for {symbol}")
            return None

        to_date = datetime.now()
        from_date = to_date - timedelta(days=days)

        try:
            params = {
                "exchange": exch,
                "symboltoken": token,
                "interval": interval,
                "fromdate": from_date.strftime("%Y-%m-%d %H:%M"),
                "todate": to_date.strftime("%Y-%m-%d %H:%M")
            }

            response = self.api.getCandleData(params)

            if not response or not response.get('status') or not response.get('data'):
                logger.warning(f"No Candle Data for {symbol}")
                return None

            df = pd.DataFrame(response['data'], columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
            cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            df[cols] = df[cols].apply(pd.to_numeric)

            return df

        except Exception as e:
            logger.error(f"Candle Fetch Error {symbol}: {e}")
            return None

    def get_ltp(self, symbol):
        token, exch = self.get_token_info(symbol)
        if not token: return None
        try:
            data = self.api.ltpData(exch, symbol.replace('.NS', '').replace('.BO', ''), token)
            if data['status']:
                return float(data['data']['ltp'])
        except:
            pass
        return None


angel_bridge = AngelBridge()