import requests
import pandas as pd
from datetime import datetime
from typing import List, Optional

def get_twelvedata_daily_equity_batch(
    symbols: List[str],
    outputsize: int = 3000,
    api_key: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch daily OHLCV for multiple equities/ETFs from TwelveData.
    Returns a single DataFrame with columns: ['symbol', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'is_closed'] (UTC).
    """
    import os
    key = api_key or os.environ.get("TWELVEDATA_API_KEY")
    if not key:
        print(f"[{datetime.now()}] [WARNING] Missing TWELVEDATA_API_KEY; skipping TwelveData calls.")
        return pd.DataFrame()

    url = "https://api.twelvedata.com/time_series"
    all_data = []

    for symbol in symbols:
        symbol = symbol.strip()
        if not symbol:
            continue

        try:
            params = {
                "symbol": symbol,
                "interval": "1day",
                "outputsize": int(outputsize or 500),
                "order": "ASC",
                "timezone": "UTC",
                "apikey": key,
            }
            r = requests.get(url, params=params, timeout=12)
            if r.status_code != 200:
                print(f"[{datetime.now()}] [WARNING] TwelveData API error {r.status_code} for {symbol}: {r.text}")
                continue

            j = r.json()
            values = j.get("values")
            if not isinstance(values, list):
                print(f"[{datetime.now()}] [WARNING] Unexpected TwelveData schema for {symbol}: {j}")
                continue

            df = pd.DataFrame(values)
            # Ensure required columns
            for col in ["datetime", "open", "high", "low", "close", "volume"]:
                if col not in df.columns:
                    df[col] = float("nan") if col != "datetime" else None

            df["datetime"] = pd.to_datetime(df["datetime"], utc=True, infer_datetime_format=True)
            df = df.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")
            df["is_closed"] = True
            df["symbol"] = symbol  # Add symbol identifier
            df = df[["symbol", "datetime", "open", "high", "low", "close", "volume", "is_closed"]]
            all_data.append(df)

        except Exception as e:
            print(f"[{datetime.now()}] [ERROR] TwelveData fetch failed for {symbol}: {e}")
            continue

    if not all_data:
        return pd.DataFrame()

    result_df = pd.concat(all_data, ignore_index=True)
    result_df = result_df.sort_values(["symbol", "datetime"]).reset_index(drop=True)
    return result_df


symbol_list = [
    # Core 10 (của anh + cực mạnh)
    'IBIT', 'FBTC', 'ARKB', 'BITB', 'GBTC', 
    'BTCO', 'HODL', 'EZBC', 'BRRR', 'DEFI',
    
    # Top 5 thêm vào 2025 (thanh khoản >$50M/ngày, spread <3bps)
    'BITO',   # ProShares Bitcoin Strategy ETF (futures) - cực hay reversal
    'XBTF',   # VanEck Bitcoin Strategy ETF
    'BTF',    # Valkyrie Bitcoin and Ether Strategy ETF
    'BITI',   # ProShares Short Bitcoin Strategy ETF (inverse) - tăng short power!
    'ETHV'    # VanEck Ethereum ETF (mới ra Q4/2025, volume bùng nổ)
]

df = get_twelvedata_daily_equity_batch(symbol_list, outputsize=3000, api_key= "29423567d31d49c7a816c490c23b447f")
df.to_csv('/root/bobby/backtest_env/alphas_storage/data/etf_crypto-ohlcv_1d.csv.gz', compression='gzip')
