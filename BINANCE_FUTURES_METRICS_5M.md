# Binance Futures Metrics 5m

Nguồn này lưu Binance USD-M Futures `metrics` 5 phút từ Binance Vision. Một file metrics đã bao gồm cả open interest và các long/short ratio, nên `_get_data` lưu thành một canonical dataset thay vì tách hai storage vật lý.

## Config

```yaml
symbols:
  - BTCUSDT
  - ETHUSDT
  - LINKUSDT
  - ARBUSDT
  - OPUSDT
  - POLUSDT
  - AAVEUSDT
quarterly_pairs:
  - BTCUSDT
  - ETHUSDT
start_date: "2023-01-01"
vision_overlap_days: 7
```

`quarterly_pairs` được discover qua Binance `/fapi/v1/exchangeInfo`, chỉ lấy active `CURRENT_QUARTER` và `NEXT_QUARTER`, rồi lưu bằng concrete symbol như `BTCUSDT_260925`.

## Storage

```text
storage/crypto/binance_futures_metrics/5m/symbol=BTCUSDT/year=YYYY/month=MM/part.csv.gz
```

Schema:

```text
time,market,symbol,contract_type,
sum_open_interest,sum_open_interest_value,
count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,
count_long_short_ratio,sum_taker_long_short_vol_ratio,
source,ingested_at
```

`time` là UTC-naive 5-minute timestamp. Dedupe key là `symbol,time`.

## Column Semantics

Tên cột giữ gần vendor schema để tránh rename sai nghĩa:

- `sum_open_interest`: open interest theo base quantity.
- `sum_open_interest_value`: open interest notional theo quote asset.
- `count_toptrader_long_short_ratio`: top trader account long/short ratio.
- `sum_toptrader_long_short_ratio`: top trader position long/short ratio.
- `count_long_short_ratio`: global account long/short ratio.
- `sum_taker_long_short_vol_ratio`: taker buy/sell volume ratio.

Không tự đổi tên `count_*` thành `account_*` hoặc `sum_*` thành `position_*` trong storage canonical; downstream có thể alias sau khi hiểu rõ semantic.

## Sync Logic

Collector [`collectors/binance_futures_metrics_5m.py`](collectors/binance_futures_metrics_5m.py) chạy theo thứ tự:

1. Optional legacy seed từ `/root/bobby/pool_alpha/Arbops/binance_basis_arb/data_storage/*_metrics_synced.csv.gz` nếu file tồn tại.
2. Binance Vision daily ZIP:
   `data/futures/um/daily/metrics/{SYMBOL}/{SYMBOL}-metrics-YYYY-MM-DD.zip`.
3. Append/dedupe vào partition monthly.
4. Khi storage đã có data, live sync chỉ quét lại từ `latest_time - vision_overlap_days`, không tải lại toàn bộ lịch sử.
5. Ngày nào có đủ `min_rows_per_full_day=288` rows sẽ được skip. Ngày partial hoặc ngày Vision publish trễ sẽ được retry ở các vòng sau.

Binance Vision metrics daily ZIP có thể đặt bucket rìa ngày trong file liền trước. Vì vậy collector luôn quét thêm `start - 1 day`, nhưng chỉ append những timestamp `>= start_date`; mục tiêu là bù được bucket `00:00` của ngày đầu cửa sổ mà không làm storage lẫn dữ liệu ngoài range cấu hình.

Hiện không dùng REST tail vì Binance Vision metrics là source canonical trong thiết kế này. File daily thường publish trễ, nên latest intraday có thể chậm hơn hiện tại một ngày.

## Loader

```python
from data_loader import BinanceFuturesMetrics5m

metrics = BinanceFuturesMetrics5m().load(
    symbols=["BTCUSDT", "ETHUSDT", "LINKUSDT"],
    start_date="2023-01-01",
    check_val=True,
)

oi = BinanceFuturesMetrics5m().load_open_interest(symbols="BTCUSDT")
ratios = BinanceFuturesMetrics5m().load_long_short_ratios(symbols="BTCUSDT")
```

Router aliases:

```python
load_data("crypto_binance_futures_metrics_5m", symbols="BTCUSDT")
load_data("binance_futures_metrics_5m", symbols="BTCUSDT")
load_data("futures_metrics_5m", symbols="BTCUSDT")
```

## Disk Estimate

Metrics Vision files are small, about `10-12 KB/day/symbol` compressed at 5-minute frequency (`288 rows/day`).

Expected storage:

- 30 days, around 11 configured/active symbols: roughly `3-7 MB`.
- 1 year, around 11 symbols: roughly `40-80 MB`.
- From `2023-01-01` to current, around 11 symbols: usually below a few hundred MB in csv.gz.
