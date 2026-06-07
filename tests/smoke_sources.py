from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta

import pandas as pd
import requests

from collectors.common.env import load_environment


def test_binance_futures() -> tuple[bool, str]:
    response = requests.get(
        "https://fapi.binance.com/fapi/v1/klines",
        params={"symbol": "BTCUSDT", "interval": "1m", "limit": 2},
        timeout=30,
    )
    response.raise_for_status()
    rows = response.json()
    return bool(rows), f"rows={len(rows)}"


def test_binance_options() -> tuple[bool, str]:
    response = requests.get("https://eapi.binance.com/eapi/v1/mark", timeout=30)
    response.raise_for_status()
    rows = response.json()
    return isinstance(rows, list), f"rows={len(rows) if isinstance(rows, list) else 'n/a'}"


def test_vnstock_daily() -> tuple[bool, str]:
    from vnstock.explorer.vci import Quote as VCIQuote

    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
    df = VCIQuote("FPT", show_log=False).history(start=start, end=end, interval="1D", show_log=False)
    return df is not None and not df.empty, f"rows={0 if df is None else len(df)}"


def test_vnstock_intraday() -> tuple[bool, str]:
    from vnstock import Quote

    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
    df = Quote(symbol="FPT", source="KBS", show_log=False).history(start=start, end=end, interval="1m", show_log=False)
    return df is not None and not df.empty, f"rows={0 if df is None else len(df)}"


def test_dnse() -> tuple[bool, str]:
    if not os.getenv("DNSE_API_KEY") or not os.getenv("DNSE_API_SECRET_KEY"):
        return True, "skipped: missing DNSE credentials"
    from collectors.vn_intraday_dnse import fetch_ohlc

    end = pd.Timestamp.now()
    start = end - pd.Timedelta(days=3)
    df = fetch_ohlc("VN30F1M", start, end)
    return not df.empty, f"rows={len(df)}"


def test_yfinance() -> tuple[bool, str]:
    import yfinance as yf

    ticker = yf.Ticker("SPY")
    expiries = ticker.options
    return bool(expiries), f"expiries={len(expiries)}"


TESTS = {
    "binance-futures": test_binance_futures,
    "binance-options": test_binance_options,
    "vnstock-daily": test_vnstock_daily,
    "vnstock-intraday": test_vnstock_intraday,
    "dnse": test_dnse,
    "yfinance": test_yfinance,
}


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", action="append", choices=list(TESTS) + ["all"], default=["all"])
    args = parser.parse_args()

    selected = list(TESTS) if "all" in args.source else args.source
    failed: list[str] = []
    for name in selected:
        try:
            ok, detail = TESTS[name]()
            status = "ok" if ok else "fail"
            print(f"{name}: {status} {detail}")
            if not ok:
                failed.append(name)
        except Exception as exc:
            print(f"{name}: fail {type(exc).__name__}: {exc}")
            failed.append(name)
    if failed:
        raise SystemExit(f"failed sources: {', '.join(failed)}")


if __name__ == "__main__":
    main()

