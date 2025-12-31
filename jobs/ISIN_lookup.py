import pandas as pd
import requests
import io
import json
import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from config import redis_client
from utils.utility import normalize_company_name

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ISIN_SYNC")


class ISINLookupService:
    def __init__(self):
        self.isin_to_symbols = defaultdict(list)
        self.nse_metadata = {}
        self.bse_metadata = {}

        self.bse_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": "https://www.bseindia.com/",
            "Accept": "application/json, text/plain, */*"
        }

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
    def fetch_nse(self):
        url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()

        df = pd.read_csv(io.BytesIO(resp.content))
        df.columns = df.columns.str.upper().str.strip()
        return df[df["SERIES"].isin(["EQ", "BE", "BZ", "SM"])]

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        retry=retry_if_exception_type((requests.RequestException, ValueError))
    )
    def fetch_bse(self):
        url = "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?segment=Equity"
        resp = requests.get(url, headers=self.bse_headers, timeout=20)
        if resp.status_code != 200:
            raise ValueError(f"BSE HTTP {resp.status_code}")

        # BSE sometimes returns HTML when blocking
        if "text/html" in resp.headers.get("Content-Type", ""):
            raise ValueError("BSE anti-bot HTML response")

        data = resp.json()
        if not isinstance(data, list):
            raise ValueError("Invalid BSE JSON payload")

        return pd.DataFrame(data)

    async def run(self):
        logger.info("Starting ISIN Sync")

        # NSE must succeed, BSE is best-effort
        df_nse = await asyncio.to_thread(self.fetch_nse)

        try:
            df_bse = await asyncio.to_thread(self.fetch_bse)
        except Exception as e:
            logger.warning(f"BSE fetch failed (continuing with NSE only): {e}")
            df_bse = pd.DataFrame()

        nse_map, bse_map = {}, {}

        for _, r in df_nse.iterrows():
            isin = str(r.get("ISIN NUMBER", "")).strip()
            sym = str(r.get("SYMBOL", "")).strip().upper()
            name = str(r.get("NAME OF COMPANY", "")).strip().upper()

            if not isin or not sym:
                continue

            norm = normalize_company_name(name)
            if len(norm) >= 6:
                nse_map[norm] = isin

            nse_map[sym] = isin
            self.isin_to_symbols[isin].append(f"{sym}.NS")

            self.nse_metadata.setdefault(isin, {
                "symbol": f"{sym}.NS",
                "series": r.get("SERIES"),
                "segment": "SME" if r.get("SERIES") == "SM" else "EQ"
            })

        if not df_bse.empty:
            for _, r in df_bse.iterrows():
                isin = str(r.get("ISIN_NUMBER", "")).strip()
                code = str(r.get("SCRIP_CD", "")).strip()
                name = str(r.get("Scrip_Name", "")).strip().upper()

                if not isin or not code:
                    continue

                norm = normalize_company_name(name)
                if len(norm) >= 6:
                    bse_map[norm] = isin

                bse_map[code] = isin

                symbol = f"{code}.BO"
                if symbol not in self.isin_to_symbols[isin]:
                    self.isin_to_symbols[isin].append(symbol)

                self.bse_metadata.setdefault(isin, {
                    "symbol": symbol,
                    "segment": "EQ",
                })

        async with redis_client.pipeline() as pipe:
            if nse_map:
                pipe.hset("CONFIG:ISIN:NSE", mapping=nse_map)
            if bse_map:
                pipe.hset("CONFIG:ISIN:BSE", mapping=bse_map)

            pipe.hset(
                "CONFIG:ISIN:SYMBOL",
                mapping={k: json.dumps(v) for k, v in self.isin_to_symbols.items()}
            )

            if self.nse_metadata:
                pipe.hset(
                    "CONFIG:ISIN:METADATA:NSE",
                    mapping={k: json.dumps(v) for k, v in self.nse_metadata.items()}
                )
            if self.bse_metadata:
                pipe.hset(
                    "CONFIG:ISIN:METADATA:BSE",
                    mapping={k: json.dumps(v) for k, v in self.bse_metadata.items()}
                )

            pipe.set("CONFIG:ISIN:LAST_SYNC", datetime.now(timezone.utc).isoformat())
            await pipe.execute()

        logger.info(
            f"ISIN Sync Complete | NSE: {len(self.nse_metadata)} | BSE: {len(self.bse_metadata)}"
        )


if __name__ == "__main__":
    asyncio.run(ISINLookupService().run())
