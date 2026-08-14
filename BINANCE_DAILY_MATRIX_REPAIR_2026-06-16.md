# Binance Daily Matrix Repair 2026-06-16

## New VPS clean rebuild — 2026-08-14

The records below describe earlier implementation/repair history; they are not
evidence that old-VPS files were copied to this host. The clean new-VPS rebuild
ran from direct Binance USD-M source under exact-gate commit `d9327fb`.

- Canonical path: `storage/crypto/binance_daily_matrix/{open,high,low,close,volume}.parquet`.
- Current eligible universe: 380 USD-M `COIN` perpetual USDT contracts at
  least 365 days old. Non-crypto/TradFi/Alpha/index contracts are excluded.
- Five matrices contain 2,417 UTC daily rows from `2020-01-01` through closed
  day `2026-08-13`.
- Durable audit: `state/audits/binance_daily_matrix_phase_e.json`, status
  `pass`; zero observed incomplete OHLC, invalid bounds, negative values,
  continuity gaps, or missing closed-day tails. Zero-volume cells are only
  pre-listing/missing-OHLC positions, not observed candles.
- `CryptoDailyMatrix` was added to the accepted non-Deribit consumer manifest
  only after that audit passed. The first run encountered one Binance 429 for
  `SYRUPUSDT` and failed closed; its idempotent repair run passed.

## Problem

`storage/crypto/binance_daily_matrix/{open,high,low,close,volume}.csv.gz` chi co 16 dong:

- Date range: `2026-06-01 -> 2026-06-16`
- Shape moi feature: `(16, 400)`
- `BTCUSDT` va `ETHUSDT` deu chi co 16 non-null rows

Nguyen nhan la collector cu doc global latest index cua matrix, neu file da ton tai thi chi fetch `latest_date - 5 days`. Vi matrix dau tien duoc tao tu `2026-06-01`, service khong bao gio quay lai `2020-01-01` de backfill phan dau. Day la loi policy cho backtest: data co san bi coi nhu da du thay vi kiem tra head/tail va gap noi bo.

## Fix

`collectors/binance_daily_matrix.py` da duoc refactor theo policy history-first:

- Doc matrix hien co truoc khi fetch, lay status rieng cho tung symbol.
- Neu symbol chua co data, cot rong, hoac earliest date lon hon `--backfill-start`, fetch lai tu `backfill_start`.
- Neu co gap daily noi bo sau khi symbol da bat dau co data, fetch lai tu truoc gap dau tien voi overlap.
- Neu chi thieu phan duoi, fetch tu `latest - overlap_days`.
- Chi ghi toi daily candle da dong hoan toan: ngay hien tai `2026-06-16` khong duoc ghi; max date sau repair la `2026-06-15`.
- Merge bang `pivoted_new.combine_first(existing_df)`, uu tien data moi trong vung overlap, dedupe theo `time,symbol`, sort index/columns, atomic write.
- Symbol universe lay top 400 Binance USD-M Futures theo score hang thang: `50%` rank `24h quoteVolume`, `30%` rank tuoi listing, `20%` rank volume stability 180 ngay. Policy chi them khong bot trong universe hop le. Cot lich su cu duoc giu de backtest; symbol inactive chi bi bo qua khi fetch moi.

## Universe correction

Sau repair dau tien, universe raw `status=TRADING + endswith("USDT")` bi phat hien la qua rong. Binance USD-M Futures hien co ca `TRADIFI_PERPETUAL`, equity, pre-market, commodity va index contracts nhu `AMDUSDT`, `GOOGLUSDT`, `OPENAIUSDT`, `TSLAUSDT`, `XAUUSDT`, `BTCDOMUSDT`. Chung la symbol Binance tra ve that, nhung khong thuoc crypto coin daily matrix.

Policy da chinh lai:

- Chi nhan `contractType=PERPETUAL`.
- Chi nhan `underlyingType=COIN`.
- Bat buoc `quoteAsset=USDT` va `marginAsset=USDT`.
- Loai `underlyingSubType` trong `Alpha`, `Index`, `TradFi`.
- Mac dinh yeu cau `min_history_days=365` tinh tu `onboardDate`.
- Thu tu cot: core big/liquid symbols truoc (`BTCUSDT`, `ETHUSDT`, `BNBUSDT`, `SOLUSDT`, ...), sau do moi toi nhom score cao con lai.

Ket qua sau correction:

- Matrix shape: `(2358, 361)`.
- First columns: `BTCUSDT`, `ETHUSDT`, `BNBUSDT`, `SOLUSDT`, `XRPUSDT`, `ADAUSDT`, `DOGEUSDT`, `AVAXUSDT`, ...
- Rejected examples:
  - `AMDUSDT`, `ANTHROPICUSDT`, `GOOGLUSDT`, `OPENAIUSDT`, `TSLAUSDT`: `contractType=TRADIFI_PERPETUAL`.
  - `BTCDOMUSDT`: `underlyingType=INDEX`.
  - `BSBUSDT`, `BEATUSDT`, `4USDT`: `underlyingSubType=Alpha`.
  - `0GUSDT`: `history_days < 365`.

Docker service `binance-daily-matrix` chay luc `00:05 UTC` voi `--backfill-start 2020-01-01`.

## Loader compatibility

`CryptoDailyMatrix.load(feature="close")` van giu schema cu:

```python
daily_close = CryptoDailyMatrix().load("close", start_date="2020-01-01")
```

Them API moi cho pipeline chien luoc cu:

```python
from data_loader import CryptoDailyMatrix

data_dict = CryptoDailyMatrix().load_ohlcv(
    symbols=None,
    start_date="2020-01-01",
    check_val=True,
)

# data_dict["BTCUSDT"] -> DataFrame index=datetime,
# columns=["open", "high", "low", "close", "volume"]
```

Neu can long format:

```python
df = CryptoDailyMatrix().load_ohlcv_frame(
    symbols=["BTCUSDT", "ETHUSDT"],
    start_date="2020-01-01",
)
```

## Repair run

Command:

```bash
run-py -m collectors.binance_daily_matrix --mode once --backfill-start 2020-01-01 --top-n 400
```

Result:

- Tracked symbols: `396`
- Active symbols fetched: `396`
- Written matrices:
  - `open.csv.gz`: `(2358, 400)`
  - `high.csv.gz`: `(2358, 400)`
  - `low.csv.gz`: `(2358, 400)`
  - `close.csv.gz`: `(2358, 400)`
  - `volume.csv.gz`: `(2358, 400)`
- Date range: `2020-01-01 -> 2026-06-15`
- `2026-06-16` current-day partial candle: removed / not present

## Validation

Continuity audit after repair:

- All 5 features: `gap_symbols = 0`
- `BTCUSDT`: `2020-01-01 -> 2026-06-15`, `2358` rows, `0` daily gaps
- `ETHUSDT`: `2020-01-01 -> 2026-06-15`, `2358` rows, `0` daily gaps

Loader validation:

- `CryptoDailyMatrix().load("close", symbols=["BTCUSDT", "ETHUSDT"], start_date="2020-01-01")`
  - Shape: `(2358, 2)`
  - Range: `2020-01-01 -> 2026-06-15`
- `CryptoDailyMatrix().load_ohlcv(symbols=["BTCUSDT", "ETHUSDT"], start_date="2020-01-01")`
  - Keys: `BTCUSDT`, `ETHUSDT`
  - Each frame shape: `(2358, 5)`
  - Dtypes: OHLC `float64`, volume `int64`

Test commands:

```bash
run-py -m compileall collectors/binance_daily_matrix.py data_loader.py tests/test_data_loader.py
run-py -m unittest tests.test_data_loader
run-py -m tests.smoke_storage
run-py -m tests.smoke_sources
```

Results:

- Compile: ok
- Unit tests: `Ran 7 tests ... OK`
- Storage smoke: ok
- Source smoke: Binance futures/options, vnstock daily/intraday, DNSE, yfinance all ok

## Volume overwrite follow-up 2026-08-03

Audit of the Parquet matrix found a second, narrower corruption path. In the
old merge order, `pivoted_new.fillna(0)` ran before
`pivoted_new.combine_first(existing_df)`. Because the pivot index is shared by
all symbols, a missing BTC/ETH/BCH volume cell was converted to zero and then
used as a new observation, overwriting the existing value. The fetch window
was also derived only from `open`, so a complete OHLC matrix hid an incomplete
volume matrix.

The collector now:

- scans fetch coverage per feature and uses the earliest required start;
- treats positive Binance daily volume as the persisted coverage signal for
  volume repair, while using the symbol's OHLC first date as the listing bound;
- merges new cells with `combine_first` before the final dense volume fill;
- keeps the existing Parquet matrix dtype and `CryptoDailyMatrix` endpoint
  contract;
- loads each existing matrix once per run and reuses it during the write.

The service backfills affected symbols automatically on the next scheduled
run. No synthetic volume is created. A post-repair audit must confirm that
volume is positive on every date where the symbol has an OHLC candle, except
for genuine zero-volume source rows.
