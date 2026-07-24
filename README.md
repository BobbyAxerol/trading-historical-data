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

## Agent Operating Rules

- Sau mỗi lượt test/compile/live smoke có đọc/ghi Parquet hoặc gọi loader lớn, agent phải cleanup RAM rác bằng `gc.collect()` và `pyarrow.default_memory_pool().release_unused()` best-effort trước khi kết luận hoặc chuyển phase.
- Báo cáo ngắn kết quả cleanup nếu task vừa chạy có nguy cơ giữ RAM, ví dụ: `gc.collect()` thu được bao nhiêu object và PyArrow memory pool còn bao nhiêu bytes allocated.

## Supported Data Sources

| Dataset | Độ phân giải | Universe hiện tại | Historical/warmup | Update | Loader endpoint |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Binance USD-M Futures perpetual | `1m` | Configured core crypto symbols | Binance Vision + REST tail | Live mỗi phút | `CryptoBinance1m`, `load_data("crypto_1m")` |
| Binance USD-M Quarterly concrete contracts | `1m` | BTC/ETH quarterly historical + active contracts | Binance Vision monthly/daily + REST active tail | Định kỳ | `CryptoBinanceQuarterly1m`, `load_data("binance_usdm_quarterly_1m")` |
| Binance Spot BTCUSDT | `1m` | `BTCUSDT` từ `2018-01-01` | Binance Vision + REST tail; gap được fill bằng USD-M futures proxy khi đã duyệt | Định kỳ/live tail | `CryptoBinanceSpot1m`, `load_data("binance_spot_1m")` |
| Binance Daily Matrix | `1d` | Top/liquid USD-M perpetual symbols, policy chỉ thêm trong universe hợp lệ | Backfill từ `2020-01-01` | Hằng ngày `00:05 UTC` | `CryptoDailyMatrix`, `load_data("binance_daily_matrix", feature=...)` |
| Binance Futures Metrics | `5m` | `BTCUSDT`, `ETHUSDT`, relation symbols, active BTC/ETH quarterlies | Binance Vision `daily/metrics`, scan full coverage | Định kỳ cuối ngày/REST tail perpetual | `BinanceFuturesMetrics5m`, `load_data("binance_futures_metrics_5m")` |
| Binance Order Book Snapshot | `1h` | `BTCUSDT` perpetual + active BTCUSDT quarterlies | Rolling 30 ngày từ Vision `bookDepth` + REST current snapshot | Mỗi giờ | `BinanceOrderBookSnapshot1h`, `load_data("binance_orderbook_snapshot_1h")` |
| Binance Options Snapshot | `5m` | Options Binance theo cấu hình hiện tại | Snapshot incremental, append theo ngày | Mỗi 5 phút | `BinanceOptions5m`, `load_data("options_5m")` |
| Deribit BTC Options V1 | trades + `5m` snapshots | BTC inverse options compact-liquid universe | Deribit History API trades-only, gated by probe/pilot | Planned phased build | `DeribitOptionTrades`, `DeribitOptionSnapshots5m`, `load_data("deribit_option_trades")` |
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
    ├── binance/snapshot_5m/underlying=BTC/year=YYYY/month=MM/day=DD/part.parquet
    └── deribit/
        ├── instruments/version=v1/instruments.parquet
        ├── trades/version=v1/currency=BTC/year=YYYY/month=MM/day=DD/part-*.parquet
        └── snapshot_5m/version=v1/currency=BTC/year=YYYY/month=MM/day=DD/part-*.parquet
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

### Options Snapshot Format

`BinanceOptions5m` dùng `snapshot_time` làm timestamp canonical, timezone-normalized thành naive UTC. `symbols` ở endpoint này là `underlying` (`BTC`, `ETH`, ...), còn cột `symbol` trong output là option contract cụ thể như `BTC-260925-100000-C`.

Storage hiện tại là daily partition để append không phải rewrite cả tháng:

```text
storage/options/binance/snapshot_5m/underlying=BTC/year=YYYY/month=MM/day=DD/part.parquet
```

Loader vẫn đọc được monthly legacy `year=YYYY/month=MM/part.parquet` nếu còn tồn tại trong giai đoạn chuyển đổi, nhưng writer mới chỉ ghi vào daily partition.

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

# Deribit BTC options V1 API probe
PYTHONPATH=. python -m collectors.deribit_option_trades probe --version v1 --json

# Deribit probe with lightweight rate verification before production backfill phases
PYTHONPATH=. python -m collectors.deribit_option_trades probe --version v1 --rate-ramp --max-rps 2 --requests-per-rps 1 --json

# Deribit BTC option instrument dimension + checkpoint state
PYTHONPATH=. python -m collectors.deribit_option_trades discover --version v1 --json

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
    DeribitOptionSnapshots5m,
    DeribitOptionTrades,
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
| `DeribitOptionTrades` | `deribit_option_trades`, `deribit_btc_option_trades`, `deribit_options_trades_v1` | Long canonical option trade events |
| `DeribitOptionSnapshots5m` | `deribit_option_snapshots_5m`, `deribit_btc_option_snapshots_5m`, `deribit_options_5m` | Long compact-liquid option snapshot tape |

Tham số chung:

- `symbols`: string, list string, hoặc `None` để load toàn bộ symbols hiện có.
- `start_date`, `end_date`: inclusive datetime filter.
- `limit`: giới hạn số dòng sau khi sort.
- `check_val`: mặc định `True`; không tự tắt validation trong service downstream.
- `columns`: với OHLCV loaders, mặc định chỉ đọc `time/symbol/open/high/low/close/volume` để giảm RAM. Truyền `columns="full"` nếu cần toàn bộ schema như `source`, `ingested_at`, `quote_volume`.

### Deribit BTC Options V1 Notes

Deribit V1 hiện đi theo phase riêng vì dữ liệu option trades rất lớn. Phase 1 chỉ probe API behavior và ghi report:

```text
state/deribit_options/version=v1/api_probe_report.json
```

`probe` không tải historical backfill. Nếu chạy không có `--rate-ramp`, report có thể có `status=blocked`; đây là guardrail nghĩa là chưa đủ điều kiện cho production backfill, không phải lỗi loader endpoint. Production backfill chỉ được mở khi report có đủ mandatory fields và rate ramp đã verify thật.

Phase 2 `discover` tạo instrument dimension và checkpoint state, vẫn chưa tải option trades:

```text
storage/options/deribit/instruments/version=v1/instruments.parquet
state/deribit_options/version=v1/BTC.sqlite
```

Do path có thư mục hive-style `version=v1`, code nội bộ đọc physical file bằng `pyarrow.parquet.ParquetFile(...).read()` khi cần schema chính xác, tránh PyArrow tự thêm virtual column `version`.
- `timeframe`: khi gọi qua `load_data`, truyền `timeframe="5min"`/`"15min"`/`"1h"` để dùng endpoint resample-on-read.
- `feature`: bắt buộc khi dùng router cho matrix datasets.

Raw `1m` vẫn là canonical storage. Endpoint resample không tạo thêm timeframe file mặc định; nó query Parquet 1m bằng DuckDB, chỉ đọc OHLCV columns, aggregate trước rồi mới trả về Pandas. Nếu DuckDB không khả dụng, loader fallback sang Pandas chunk theo partition.

### Common Examples

```python
from data_loader import CryptoBinance1m, CryptoDailyMatrix, VNDailyMatrix, load_data

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

# Binance futures 1m nhưng trả thẳng OHLCV 5m, không load full 1m vào RAM trước
sol_5m = CryptoBinance1m().load_resampled(
    symbols="SOLUSDT",
    timeframe="5min",
    start_date="2020-01-01",
    check_val=True,
)

# Router tương đương
sol_15m = load_data(
    "crypto_1m",
    symbols="SOLUSDT",
    timeframe="15min",
    start_date="2020-01-01",
    check_val=True,
)

# Opt-in full schema nếu downstream thật sự cần metadata/source columns
sol_full = CryptoBinance1m().load(
    symbols="SOLUSDT",
    start_date="2026-07-01",
    columns="full",
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

# Deribit BTC options V1. Phase 0 freezes endpoint/schema; data appears after probe/pilot/backfill phases.
from data_loader import DeribitOptionSnapshots5m, DeribitOptionTrades

deribit_trades = DeribitOptionTrades().load(
    start_date="2024-01-01",
    currency="BTC",
    version="v1",
)

deribit_5m = DeribitOptionSnapshots5m().load(
    start_date="2024-01-01",
    currency="BTC",
    entry_eligible_only=False,
    version="v1",
)

# Binance options snapshot 5m. symbols là underlying, timestamp chính là snapshot_time.
from data_loader import BinanceOptions5m

btc_options = BinanceOptions5m().load(
    symbols="BTC",
    start_date="2026-07-01",
    check_val=True,
)
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
- [MEMORY_OPTIMIZATION_PLAN.md](MEMORY_OPTIMIZATION_PLAN.md): loader projection, resample-on-read và Phase 2 giảm RAM collectors.
- [BINANCE_DAILY_MATRIX_REPAIR_2026-06-16.md](BINANCE_DAILY_MATRIX_REPAIR_2026-06-16.md): repair/backfill Binance daily matrix.
- [BINANCE_SPOT_1M.md](BINANCE_SPOT_1M.md): BTCUSDT spot 1m, gap policy và futures proxy fill.
- [BINANCE_USDM_QUARTERLY_1M.md](BINANCE_USDM_QUARTERLY_1M.md): quarterly contracts.
- [BINANCE_ORDERBOOK_SNAPSHOT_1H.md](BINANCE_ORDERBOOK_SNAPSHOT_1H.md): order book snapshot conventions.
- [BINANCE_FUTURES_METRICS_5M.md](BINANCE_FUTURES_METRICS_5M.md): open interest và long/short metrics.
- [CONTINUITY_REPAIR_2026-06-12.md](CONTINUITY_REPAIR_2026-06-12.md): crypto 1m continuity incident/repair.
