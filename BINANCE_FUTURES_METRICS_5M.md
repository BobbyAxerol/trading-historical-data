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
start_date: null
vision_overlap_days: 7
include_rest_tail: true
```

`quarterly_pairs` được discover qua Binance `/fapi/v1/exchangeInfo`, chỉ lấy active `CURRENT_QUARTER` và `NEXT_QUARTER`, rồi lưu bằng concrete symbol như `BTCUSDT_260925`.

`start_date: null` nghĩa là auto-discover ngày sớm nhất có thật trên Binance Vision cho từng symbol. Nếu cần giới hạn dữ liệu vì lý do test/disk, truyền `--start-date YYYY-MM-DD`; collector sẽ lấy `max(ngày sớm nhất Vision có, start_date)`.

## Storage

```text
storage/crypto/binance_futures_metrics/5m/symbol=BTCUSDT/year=YYYY/month=MM/part.parquet
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
2. Normalize/dedupe partition hiện có theo `symbol,time`, sửa timestamp về bucket 5 phút và ép numeric schema.
3. List toàn bộ keys có thật từ Binance Vision S3 prefix:
   `data/futures/um/daily/metrics/{SYMBOL}/{SYMBOL}-metrics-YYYY-MM-DD.zip`.
4. Tính `effective_start` theo earliest Vision key nếu config để `null/auto`.
5. Quét coverage toàn bộ từ `effective_start` tới latest Vision key. Ngày nào thiếu/partial so với `min_rows_per_full_day=288`, hoặc có field metric nullable, sẽ schedule tải lại file ngày đó và file liền trước để bù bucket rìa ngày.
6. Với nhiều raw observations trong cùng bucket 5 phút, từng field được lấy từ observation trực tiếp cuối cùng có giá trị. Giá trị không được dựng khi mọi observation nguồn đều null. Sau đó append/dedupe vào partition monthly.
7. Với perpetual symbols, REST tail dùng Binance Futures Data endpoints để bù vài ngày cuối nếu Vision publish trễ. Quarterly concrete contracts không dùng REST tail vì Binance REST metrics không expose concrete quarterly contract theo cùng schema; quarterly lấy từ Vision.
8. Audit lại duplicate, internal 5-minute gaps và partial days; kết quả ghi dưới `state/audits/`.

Binance Vision metrics daily ZIP thường chứa `00:05` của ngày file tới `00:00` của ngày kế tiếp. Vì vậy coverage repair luôn xét cả file liền trước khi một calendar day bị thiếu bucket `00:00`.

REST tail chỉ là lớp bù đuôi cho perpetual; khi Binance Vision publish file daily, vòng sau vẫn quét coverage và dedupe theo `symbol,time`, nên storage không bị duplicate.

Một REST response thiếu một hoặc nhiều metric không được phép ghi đè một row Vision hoàn chỉnh. Collector ghi warning, bỏ row REST partial đó và thử lại ở overlap kế tiếp.

## Upstream Availability And Phase D Audit

Binance Vision không bảo đảm tất cả sáu metric fields có giá trị ở mọi bucket
5 phút lịch sử. Một số ngày cũng có dưới 288 buckets. Đây là availability của
nguồn, không phải giá trị được phép forward-fill.

Phase D BTCUSDT chạy với `--no-legacy --audit-phase-d` và audit tách hai lớp:

- structural integrity: schema, timestamp/bucket, duplicate, malformed
  numeric, negative values, market/contract/symbol/source provenance; mọi lỗi
  này fail closed;
- direct-source availability: rows/gaps ngày ngắn và fields nullable được ghi
  đếm theo source/column. Nếu structural integrity pass nhưng source còn sparse,
  status là `pass_with_documented_source_gaps`, không phải strict completeness.

Snapshot new-VPS 2026-08-13 cho `BTCUSDT`: `625,109` rows từ
`2020-09-01T00:00:00` tới `2026-08-13T17:30:00`; zero duplicate, malformed
numeric, negative, bucket, market/contract/symbol/source-provenance errors.
Audit vẫn ghi `160` 5-minute gaps, `75` ngày short, và `92,275` rows có metric
nullable trực tiếp từ Binance Vision. Không có số liệu synthetic nào được ghi.
Evidence canonical là
`state/audits/crypto_binance_futures_metrics_5m_BTCUSDT_phase_d.json`.

## Loader

```python
from data_loader import BinanceFuturesMetrics5m

metrics = BinanceFuturesMetrics5m().load(
    symbols=["BTCUSDT", "ETHUSDT", "LINKUSDT"],
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
- Full available Vision history to current, around 11 symbols: usually below a few hundred MB in csv.gz.
