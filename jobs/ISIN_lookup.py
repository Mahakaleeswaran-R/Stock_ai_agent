import pandas as pd
import requests
import io
import logging
import asyncio
import json
from collections import defaultdict
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from config import redis_client
from utils.utility import normalize_company_name

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ISIN_LOOKUP_SYNC")


class ISINLookupService:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.bseindia.com/',
            'Accept': 'application/json, text/plain, */*'
        }
        self.isin_to_symbols = defaultdict(list)
        self.nse_metadata = {}
        self.bse_metadata = {}

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry=retry_if_exception_type(Exception))
    def _fetch_data(self, url, headers, source_name, parser_func):
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            if source_name == 'BSE' and 'text/html' in resp.headers.get('Content-Type', ''):
                raise ValueError("BSE blocked request")
            resp.raise_for_status()
            return parser_func(resp)
        except Exception as e:
            logger.error(f"Error fetching {source_name} ({url}): {e}")
            return pd.DataFrame()

    def fetch_nse(self):
        urls = [
            "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
            "https://nsearchives.nseindia.com/content/equities/SME_EQUITY_L.csv"
        ]
        dfs = []
        for url in urls:
            df = self._fetch_data(
                url=url,
                headers={'User-Agent': 'Mozilla/5.0'},
                source_name="NSE",
                parser_func=lambda r: pd.read_csv(io.BytesIO(r.content))
            )
            if not df.empty:
                df.columns = df.columns.str.strip().str.upper()
                dfs.append(df)

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()

    def fetch_bse(self):
        return self._fetch_data(
            url="https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&industry=&segment=Equity&status=Active",
            headers=self.headers,
            source_name="BSE",
            parser_func=lambda r: pd.DataFrame(r.json())
        )

    def _register_scrip(self, isin, symbol_key, name, ticker, lookup_dict, metadata_store, metadata_update):
        lookup_dict[symbol_key] = isin
        lookup_dict[name] = isin
        lookup_dict[normalize_company_name(name)] = isin

        if ticker not in self.isin_to_symbols[isin]:
            self.isin_to_symbols[isin].append(ticker)
        if isin not in metadata_store:
            metadata_store[isin] = {}
        metadata_store[isin].update(metadata_update)
        if "name" not in metadata_store[isin]:
            metadata_store[isin]["name"] = name

    async def run(self):
        logger.info("Starting Daily ISIN & Metadata Sync")
        self.isin_to_symbols.clear()
        self.nse_metadata.clear()
        self.bse_metadata.clear()

        df_nse, df_bse = await asyncio.gather(
            asyncio.to_thread(self.fetch_nse),
            asyncio.to_thread(self.fetch_bse)
        )

        nse_lookup = {}
        bse_lookup = {}

        if not df_nse.empty:
            df_nse.columns = df_nse.columns.str.strip()
            if 'SERIES' in df_nse.columns:
                df_nse = df_nse[df_nse['SERIES'].isin(['EQ', 'BE', 'BZ', 'SM', 'ST'])]

            for _, row in df_nse.iterrows():
                isin = str(row.get('ISIN NUMBER', '')).strip()
                if not isin: continue
                symbol = str(row.get('SYMBOL', '')).strip().upper()
                name = str(row.get('NAME OF COMPANY', '')).strip().upper()

                self._register_scrip(
                    isin=isin, symbol_key=symbol, name=name, ticker=f"{symbol}.NS",
                    lookup_dict=nse_lookup,
                    metadata_store=self.nse_metadata,
                    metadata_update={
                        "nse_symbol": f"{symbol}.NS",
                        "series": str(row.get('SERIES', 'EQ')),
                        "lot_size": int(row.get('MARKET LOT', 1)),
                        "face_value": str(row.get('FACE VALUE', '')),
                        "listing_date": str(row.get('DATE OF LISTING', ''))
                    }
                )

        if not df_bse.empty:
            df_bse.columns = df_bse.columns.str.strip()

            def _get_col(candidates):
                return next((c for c in candidates if c in df_bse.columns), None)

            c_code = _get_col(['SCRIP_CD', 'Scrip_Code'])
            c_name = _get_col(['Scrip_Name', 'SecurityName'])
            c_isin = _get_col(['ISIN_NUMBER', 'ISIN'])
            c_group = _get_col(['GROUP', 'Group'])

            if c_isin and c_code:
                for _, row in df_bse.iterrows():
                    isin = str(row.get(c_isin, '')).strip()
                    if not isin: continue
                    code = str(row.get(c_code, '')).strip()
                    name = str(row.get(c_name, '')).strip().upper()

                    self._register_scrip(
                        isin=isin, symbol_key=code, name=name, ticker=f"{code}.BO",
                        lookup_dict=bse_lookup,
                        metadata_store=self.bse_metadata,  # Target BSE store
                        metadata_update={
                            "bse_symbol": f"{code}.BO",
                            "bse_code": code,
                            "bse_group": str(row.get(c_group, '')).strip()
                        }
                    )

        async with redis_client.pipeline() as pipe:
            if nse_lookup:
                pipe.hset("CONFIG:ISIN:NSE", mapping=nse_lookup)
            if bse_lookup:
                pipe.hset("CONFIG:ISIN:BSE", mapping=bse_lookup)

            if self.isin_to_symbols:
                symbol_map_json = {k: json.dumps(v) for k, v in self.isin_to_symbols.items()}
                pipe.hset("CONFIG:ISIN:SYMBOL", mapping=symbol_map_json)

            if self.nse_metadata:
                nse_meta_json = {k: json.dumps(v) for k, v in self.nse_metadata.items()}
                pipe.hset("CONFIG:ISIN:METADATA:NSE", mapping=nse_meta_json)

            if self.bse_metadata:
                bse_meta_json = {k: json.dumps(v) for k, v in self.bse_metadata.items()}
                pipe.hset("CONFIG:ISIN:METADATA:BSE", mapping=bse_meta_json)

            await pipe.execute()
        logger.info(f"Sync Complete.")


if __name__ == "__main__":
    service = ISINLookupService()
    asyncio.run(service.run())