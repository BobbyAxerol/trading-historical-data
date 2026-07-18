# `_get_data` - Market Data Storage & Loader SDK

`_get_data` là layer ingest dữ liệu thị trường tập trung cho Pool Alpha. Mục tiêu chính là phục vụ backtest: kéo lịch sử dài nhất có thể, tự resume khi server/container restart, append/dedupe thông minh, audit gap/duplicate, và expose một bộ `data_loader.py` ổn định để các strategy/service khác chỉ cần import rồi gọi.

Hiện storage chính dùng **Parquet**. Sau phase cleanup, `storage/` không còn CSV runtime; CSV chỉ còn được nhắc trong migration docs như legacy/fallback history.

## Quick Status

| Nhóm | Trạng thái hiện tại |
| :--- | :--- |
| Container runtime | Docker Compose, `restart: unless-stopped` |
| Storage root | `storage/` |
| State root | `state/` |
| Format chính | Parquet, atomic write |
| Write policy | Append/merge/dedupe, không FIFO |
| Resume policy | Đọc storage/manifest/tail để xác định mốc còn thiếu, fetch bù với overlap |
| Validation | Schema, duplicate key, monotonic time, OHLC logic, continuity theo dataset |
| Loader SDK | [`data_loader.py`](data_loader.py) |

## Supported Data Sources

| Dataset | Độ phân giải | Universe hiện tại | Historical/warmup | Update | Loader endpoint |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Binance USD-M Futures perpetual | `1m` | Configured core crypto symbols | Binance Vision + REST tail | Live mỗi phút | `CryptoBinance1m`, `load_data("crypto_1m")` |
| Binance USD-M Quarterly concrete contracts | `1m` | BTC/ETH quarterly historical + active contracts | Binance Vision monthly/daily + REST active tail | Định kỳ | `CryptoBinanceQuarterly1m`, `load_data("binance_usdm_quarterly_1m")` |
| Binance Spot BTCUSDT | `1m` | `BTCUSDT` từ `2018-01-01` | Binance Vision + REST tail; gap được fill bằng USD-M futures proxy khi đã duyệt | Định kỳ/live tail | `CryptoBinanceSpot1m`, `load_data("binance_spot_1m")` |
| Binance Daily Matrix | `1d` | Top/liquid USD-M perpetual symbols, policy chỉ thêm trong universe hợp lệ | Backfill từ `2020-01-01` | Hằng ngày `00:05 UTC` | `CryptoDailyMatrix`, `load_data("binance_daily_matrix", feature=...)` |
| Binance Futures Metrics | `5m` | `BTCUSDT`, `ETHUSDT`, relation symbols, active BTC/ETH quarterlies | Binance Vision `daily/metrics`, scan full coverage | Định kỳ cuối ngày/REST tail perpetual | `BinanceFuturesMetrics5m`, `load_data("binance_futures_metrics_5m")` |
| Binance Order Book Snapshot | `1h` | `BTCUSDT` perpetual + active BTCUSDT quarterlies | Rolling 30 ngày từ Vision `bookDepth` + REST current snapshot | Mỗi giờ | `BinanceOrderBookSnapshot1h`, `load_data("binance_orderbook_snapshot_1h")` |
| Binance Options Snapshot | `5m` | Options Binance theo cấu hình hiện tại | Snapshot incremental | Mỗi 5 phút | `BinanceOptions5m`, `load_data("options_5m")` |
| VN Equity Daily raw | `1d` | Universe VN curated khoảng 300 symbols | Provider VN daily, lưu partition theo symbol/year | Hằng ngày `16:30 Asia/Ho_Chi_Minh` | `VnStockDaily`, `load_data("vn_stock_daily")` |
| VN Daily Matrix | `1d` | Các symbols có raw daily trong `storage/vn/equity/1d` | Build từ canonical raw Parquet | Chạy builder khi cần sau raw daily update | `VNDailyMatrix`, `load_data("vn_daily_matrix", feature=...)` |
| VN Equity Intraday | `1m` | VN stock symbols trong config | Provider VN intraday | Hằng ngày `16:30 Asia/Ho_Chi_Minh` | `VnStock1m`, `load_data("vn_stock_1m")` |
| VN Futures Intraday | `1m` | `VN30F1M` và symbols futures configured | DNSE/VN provider | Hằng ngày `16:30 Asia/Ho_Chi_Minh` | `VnFutures1m`, `load_data("vn_futures_1m")` |

## Storage Layout

```text
storage/
├── crypto/
│   ├── binance_futures/1m/symbol=BTCUSDT/year=YYYY/month=MM/part.parquet
│   ├── binance_futures/1m/symbol=BTCUSDT_240329/year=YYYY/month=MM/part.parquet
│   ├── binance_spot/1m/symbol=BTCUSDT/year=YYYY/month=MM/part.parquet
│   ├── binance_orderbook_snapshot/1h/symbol=BTCUSDT/year=YYYY/month=MM/part.parquet
│   ├── binance_futures_metrics/5m/symbol=BTCUSDT/year=YYYY/month=MM/part.parquet
│   └── binance_daily_matrix/
│       ├── open.parquet
│       ├── high.parquet
│       ├── low.parquet
│       ├── close.parquet
│       └── volume.parquet
├── vn/
│   ├── equity/1d/symbol=FPT/year=YYYY/part.parquet
│   ├── equity/1m/symbol=FPT/year=YYYY/month=MM/part.parquet
│   ├── equity/daily_matrix/
│   │   ├── open.parquet
│   │   ├── high.parquet
│   │   ├── low.parquet
│   │   ├── close.parquet
│   │   └── volume.parquet
│   └── futures/1m/symbol=VN30F1M/year=YYYY/month=MM/part.parquet
└── options/
    └── binance/snapshot_5m/underlying=BTC/year=YYYY/month=MM/part.parquet
```

Partitioned datasets lưu schema dài theo dòng. Matrix datasets lưu wide table: `index=time`, `columns=symbols`, value là từng feature.

## Data Contracts

### OHLCV Long Format

Các loader OHLCV dài trả về DataFrame chuẩn:

| Column | Type | Meaning |
| :--- | :--- | :--- |
| `time` | `datetime64[ns]` | Open time, timezone-normalized thành naive datetime |
| `symbol` | `str` | Mã giao dịch |
| `open` | `float64` | Open |
| `high` | `float64` | High |
| `low` | `float64` | Low |
| `close` | `float64` | Close |
| `volume` | numeric | Volume |
| `source` | `str`, optional | Nguồn dòng dữ liệu |
| `ingested_at` | `str`, optional | ISO UTC ingest timestamp |

Crypto dùng naive UTC. VN dùng naive `Asia/Ho_Chi_Minh`.

### Matrix Format

`CryptoDailyMatrix` và `VNDailyMatrix` hỗ trợ:

- `load(feature=...)`: trả về một matrix wide cho `open/high/low/close/volume`.
- `load_features()`: trả về dict `{feature: matrix_df}`.
- `load_ohlcv()`: trả về `data_dict[symbol] = DataFrame(index=time, columns=open/high/low/close/volume)`, tương thích pipeline strategy cũ.
- `load_ohlcv_frame()`: trả về long OHLCV DataFrame từ matrix.

## Docker Services

Các service chạy bằng `docker compose` và có `restart: unless-stopped`.

| Service | Collector | Lịch/cơ chế |
| :--- | :--- | :--- |
| `crypto-1m-live` | `collectors.crypto_1m_live` | Cập nhật futures 1m liên tục |
| `binance-usdm-quarterly-1m` | `collectors.binance_usdm_quarterly_1m` | Sync quarterly historical/current |
| `binance-spot-1m` | `collectors.binance_spot_1m` | Sync BTCUSDT spot historical/current |
| `binance-daily-matrix` | `collectors.binance_daily_matrix` | Daily matrix lúc `00:05 UTC` |
| `binance-futures-metrics-5m` | `collectors.binance_futures_metrics_5m` | Metrics 5m theo lịch |
| `binance-orderbook-snapshot-1h` | `collectors.binance_orderbook_snapshot_1h` | Snapshot order book mỗi giờ |
| `options-binance-5m` | `collectors.options_binance_5m` | Options snapshot mỗi 5 phút |
| `vn-daily` | `collectors.vn_daily` | VN daily raw lúc `16:30 Asia/Ho_Chi_Minh` |
| `vn-intraday-stocks` | `collectors.vn_intraday_vnstock` | VN stock 1m lúc `16:30 Asia/Ho_Chi_Minh` |
| `vn30f1m-dnse` | `collectors.vn_intraday_dnse` | VN futures 1m lúc `16:30 Asia/Ho_Chi_Minh` |

```bash
docker compose up -d --build
docker compose ps
docker compose logs -f binance-daily-matrix
python -m collectors.healthcheck
```

## One-Shot Commands

Chạy bằng Python local trong thư mục `_get_data`, hoặc chạy qua container `seed-existing-history` nếu muốn dùng đúng dependency/image của service.

```bash
# Binance futures 1m gap audit/repair
PYTHONPATH=. python -m collectors.audit_continuity --dataset crypto --all-symbols
PYTHONPATH=. python -m collectors.fill_crypto_gaps --dry-run
PYTHONPATH=. python -m collectors.fill_crypto_gaps

# Binance daily matrix từ 2020
PYTHONPATH=. python -m collectors.binance_daily_matrix --mode once --backfill-start 2020-01-01

# Binance quarterly contracts
PYTHONPATH=. python -m collectors.binance_usdm_quarterly_1m --mode once

# Binance spot BTCUSDT
PYTHONPATH=. python -m collectors.binance_spot_1m --mode once

# Order book snapshot 1h
PYTHONPATH=. python -m collectors.binance_orderbook_snapshot_1h --mode once

# Futures metrics 5m
PYTHONPATH=. python -m collectors.binance_futures_metrics_5m --mode once

# VN daily matrix rebuild từ raw Parquet
PYTHONPATH=. python -m collectors.vn_daily_matrix
```

## Loader Endpoints

Import trực tiếp:

```python
from data_loader import (
    BinanceFuturesMetrics5m,
    BinanceOptions5m,
    BinanceOrderBookSnapshot1h,
    CryptoBinance1m,
    CryptoBinanceQuarterly1m,
    CryptoBinanceSpot1m,
    CryptoDailyMatrix,
    VNDailyMatrix,
    VnFutures1m,
    VnStock1m,
    VnStockDaily,
    load_data,
)
```

### Endpoint Table

| Class | Router dataset names | Return |
| :--- | :--- | :--- |
| `VnStock1m` | `vn_stock_1m`, `vn_equity_1m` | Long OHLCV |
| `VnStockDaily` | `vn_stock_daily`, `vn_equity_1d`, `vn_stock_1d` | Long OHLCV |
| `VNDailyMatrix` | `vn_daily_matrix` | Matrix feature hoặc OHLCV dict/frame |
| `VnFutures1m` | `vn_futures_1m` | Long OHLCV |
| `CryptoBinance1m` | `crypto_1m` | Long OHLCV futures perpetual |
| `CryptoBinanceQuarterly1m` | `crypto_binance_quarterly_1m`, `binance_usdm_quarterly_1m` | Long OHLCV concrete quarterly |
| `CryptoBinanceSpot1m` | `crypto_binance_spot_1m`, `binance_spot_1m`, `crypto_spot_1m` | Long OHLCV spot |
| `CryptoDailyMatrix` | `binance_daily_matrix` | Matrix feature hoặc OHLCV dict/frame |
| `BinanceOrderBookSnapshot1h` | `crypto_binance_orderbook_snapshot_1h`, `binance_orderbook_snapshot_1h`, `orderbook_snapshot_1h` | Long feature table |
| `BinanceFuturesMetrics5m` | `crypto_binance_futures_metrics_5m`, `binance_futures_metrics_5m`, `futures_metrics_5m` | Long metrics table |
| `BinanceOptions5m` | `options_5m` | Long options snapshot |

Tham số chung:

- `symbols`: string, list string, hoặc `None` để load toàn bộ symbols hiện có.
- `start_date`, `end_date`: inclusive datetime filter.
- `limit`: giới hạn số dòng sau khi sort.
- `check_val`: mặc định `True`; không tự tắt validation trong service downstream.
- `feature`: bắt buộc khi dùng router cho matrix datasets.

### Common Examples

```python
from data_loader import CryptoDailyMatrix, VNDailyMatrix, load_data

# Binance daily close matrix
daily_close = CryptoDailyMatrix().load(
    feature="close",
    symbols=["BTCUSDT", "ETHUSDT"],
    start_date="2020-01-01",
    check_val=True,
)

# Binance daily OHLCV data_dict, tương thích pipeline cũ
crypto_daily = CryptoDailyMatrix().load_ohlcv(
    symbols=None,
    start_date="2020-01-01",
    check_val=True,
)

# VN daily OHLCV data_dict
vn_daily = VNDailyMatrix().load_ohlcv(
    symbols=["FPT", "VCB", "HPG"],
    start_date="2018-01-01",
    check_val=True,
)

# Binance quarterly concrete contracts
quarterly = load_data(
    "binance_usdm_quarterly_1m",
    symbols=["BTCUSDT_240329", "ETHUSDT_260925"],
    start_date="2024-01-01",
    check_val=True,
)

# Binance Spot BTCUSDT 1m
spot = load_data(
    "binance_spot_1m",
    symbols="BTCUSDT",
    start_date="2018-01-01",
    check_val=True,
)

# Order book snapshot 1h
orderbook = load_data(
    "binance_orderbook_snapshot_1h",
    symbols=["BTCUSDT", "BTCUSDT_260925"],
    start_date="2026-06-13",
    check_val=True,
)

# Futures metrics helpers
from data_loader import BinanceFuturesMetrics5m

metrics = BinanceFuturesMetrics5m().load(symbols=["BTCUSDT", "ETHUSDT"], check_val=True)
open_interest = BinanceFuturesMetrics5m().load_open_interest(symbols="BTCUSDT")
long_short = BinanceFuturesMetrics5m().load_long_short_ratios(symbols="BTCUSDT")
```

### Bind Mount From Another Project

```yaml
services:
  my-alpha-model:
    image: my-alpha-image:latest
    volumes:
      - /root/bobby/pool_alpha/alphas_storage/_get_data:/app/market_data_sdk
```

```python
import sys

sys.path.insert(0, "/app/market_data_sdk")

from data_loader import CryptoDailyMatrix, load_data
```

`data_loader.py` tự resolve `storage/` tương đối với chính file SDK, nên bind mount cả thư mục `_get_data` là đủ.

## Integrity Rules

- Tất cả write vào Parquet đi qua atomic temp file + replace.
- Partitioned storage merge/dedupe theo key dataset, thường là `symbol,time`.
- Matrix storage overwrite atomic sau khi build/merge đủ feature.
- Collector dùng retry/backoff cho network calls.
- Live/daily jobs dùng overlap window để tránh mất nến quanh điểm resume.
- Validation mặc định bật trong loader (`check_val=True`) và chỉ log warning, không im lặng bỏ lỗi.
- VN intraday được filter giờ giao dịch/ngày nghỉ qua [`collectors/common/calendar_vn.py`](collectors/common/calendar_vn.py).

## Detailed Design Notes

- [PARQUET_MIGRATION_PLAN.md](PARQUET_MIGRATION_PLAN.md): migration CSV.GZ -> Parquet và cleanup guard.
- [BINANCE_DAILY_MATRIX_REPAIR_2026-06-16.md](BINANCE_DAILY_MATRIX_REPAIR_2026-06-16.md): repair/backfill Binance daily matrix.
- [BINANCE_SPOT_1M.md](BINANCE_SPOT_1M.md): BTCUSDT spot 1m, gap policy và futures proxy fill.
- [BINANCE_USDM_QUARTERLY_1M.md](BINANCE_USDM_QUARTERLY_1M.md): quarterly contracts.
- [BINANCE_ORDERBOOK_SNAPSHOT_1H.md](BINANCE_ORDERBOOK_SNAPSHOT_1H.md): order book snapshot conventions.
- [BINANCE_FUTURES_METRICS_5M.md](BINANCE_FUTURES_METRICS_5M.md): open interest và long/short metrics.
- [CONTINUITY_REPAIR_2026-06-12.md](CONTINUITY_REPAIR_2026-06-12.md): crypto 1m continuity incident/repair.
