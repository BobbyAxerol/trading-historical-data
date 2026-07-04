# Binance USD-M Quarterly 1m Collector

## Purpose

This dataset stores concrete Binance USD-M quarterly futures contracts such as `BTCUSDT_240329` and `ETHUSDT_260925` for historical basis/backtest workflows.

The collector does not build a continuous contract. It stores raw contract-level candles only. Consumers can stitch, roll, or trade multiple contracts downstream.

## Storage

Quarterly contracts are stored in the same canonical Binance futures 1m lake used by perpetual contracts:

```text
storage/crypto/binance_futures/1m/
  symbol=BTCUSDT_240329/
    year=2024/
      month=03/
        part.csv.gz
```

The schema is aligned with `CryptoBinance1m`:

```text
time,symbol,open,high,low,close,volume,close_time,quote_volume,
number_of_trades,taker_buy_base_volume,taker_buy_quote_volume,source,ingested_at
```

`time` and `close_time` are UTC naive datetimes. This matches the existing `_get_data` convention for Binance crypto data.

## Sources And Fallback

The collector uses three layers:

1. Binance Vision monthly ZIP archives:
   `data/futures/um/monthly/klines/{SYMBOL}/1m/{SYMBOL}-1m-YYYY-MM.zip`
2. Binance Vision daily ZIP archives for active contracts and not-yet-monthly data:
   `data/futures/um/daily/klines/{SYMBOL}/1m/{SYMBOL}-1m-YYYY-MM-DD.zip`
3. Binance REST `/fapi/v1/klines` for active tail data up to the latest closed candle.

Monthly/daily Vision files are canonical for historical storage. REST is used only for active tail catch-up or when the active contract needs data newer than Vision daily files.

## Contract Discovery

Active contracts are discovered from `/fapi/v1/exchangeInfo`, filtered by:

- `contractType in {"CURRENT_QUARTER", "NEXT_QUARTER"}`
- `quoteAsset == "USDT"`
- `marginAsset == "USDT"`
- configured pair in `configs/symbols.binance_usdm_quarterly.yml`

Historical contracts are discovered from Binance Vision S3 listings. As of 2026-07-04, Binance Vision exposes BTCUSDT and ETHUSDT quarterly contracts from `2021-02` onward.

State is written to:

```text
state/binance_usdm_quarterly_contracts.json
state/manifests/crypto_binance_usdm_quarterly_1m.json
```

## Operation

Run one sync:

```bash
PYTHONPATH=. python -m collectors.binance_usdm_quarterly_1m --mode once
```

Run one contract smoke:

```bash
PYTHONPATH=. python -m collectors.binance_usdm_quarterly_1m --mode once --symbols BTCUSDT_240329
```

Docker service:

```bash
docker compose up -d binance-usdm-quarterly-1m
```

The service uses the shared `get_data-collectors:latest` image and `restart: unless-stopped`.

## Reading

Existing consumers can keep using:

```python
from data_loader import CryptoBinance1m

df = CryptoBinance1m().load(
    symbols=["BTCUSDT_240329", "ETHUSDT_240329"],
    start_date="2024-01-01",
)
```

A semantic alias is also available:

```python
from data_loader import CryptoBinanceQuarterly1m

df = CryptoBinanceQuarterly1m().load("BTCUSDT_240329")
```

## Backtest Roll Policy

`_get_data` intentionally does not roll contracts. For basis arbitrage, raw contract-level data is safer than a pre-adjusted continuous series because basis depends on the actual contract delivery date.

Recommended downstream approaches:

- Trade all available quarterly contracts independently and select by liquidity, basis, and days-to-expiry.
- Build a front-quarter continuous view by rolling before delivery or when liquidity shifts.
- Build a fixed-DTE view by selecting the contract with the nearest target days-to-expiry.

Do not back-adjust prices for basis calculations unless the strategy explicitly needs a charting-only continuous series.
