# Binance Spot 1m Collector

Nguồn này lưu dữ liệu nến spot Binance 1 phút, hiện cấu hình mặc định chỉ theo dõi `BTCUSDT` từ `2018-01-01`.

## Storage

```text
storage/crypto/binance_spot/1m/symbol=BTCUSDT/year=YYYY/month=MM/part.csv.gz
```

Schema được giữ gần giống các nguồn Binance 1m khác:

```text
time,symbol,open,high,low,close,volume,close_time,quote_volume,
number_of_trades,taker_buy_base_volume,taker_buy_quote_volume,source,ingested_at
```

`time` và `close_time` là UTC naive datetime để loader/strategy không phải convert timezone nhiều lần.

## Sync Logic

Collector [`collectors/binance_spot_1m.py`](collectors/binance_spot_1m.py) chạy theo thứ tự:

1. Binance Vision monthly ZIP: `data/spot/monthly/klines/BTCUSDT/1m/`.
2. Binance Vision daily ZIP cho đoạn gần hiện tại, cấu hình bởi `daily_lookback_days`.
3. Binance Spot REST `/api/v3/klines` để bù tail tới candle đã đóng mới nhất.

Mỗi lần append đều ghi qua `PartitionedCsvGzStore`, dedupe theo `symbol,time`, sort lại partition, và đọc tail storage để resume. Khi restart Docker, service không phụ thuộc vào tmux hay state RAM.

## Validation & Repair

Khi audit được bật, collector kiểm tra:

- thiếu continuity 1 phút;
- duplicate theo `symbol,time`;
- OHLC logical errors;
- giá/volume âm.

Nếu `--repair-gaps` được bật, các gap có độ dài không vượt `max_gap_minutes` sẽ được gọi lại bằng daily Vision và REST rồi merge vào partition hiện có. Collector không forward-fill và không dựng nến synthetic từ giá gần nhất.

Theo policy backtest đã duyệt ngày `2026-07-11`, nếu `proxy_fill_from_futures: true` hoặc `--proxy-fill-futures-gaps` được bật, các gap spot mà Binance USD-M Futures `BTCUSDT` local cover đủ 100% từng phút sẽ được fill thẳng vào canonical spot storage với:

- `source=binance_usdm_futures_proxy_gap_fill`;
- OHLC lấy từ USD-M Futures 1m cùng phút;
- `volume`, `quote_volume`, `taker_buy_*`, `number_of_trades` được scale bằng median tỷ lệ spot/futures trong cửa sổ context hai bên gap (`proxy_context_hours`, mặc định 6 giờ);
- gap không đủ futures coverage được giữ nguyên, không fill.

Snapshot audit sau futures proxy repair ngày `2026-07-11 08:26 UTC` cho `BTCUSDT`:

```text
rows=4,477,485
range=2018-01-01 00:00:00 -> 2026-07-11 08:24:00 UTC
duplicate_rows=0
ohlc_bad_rows=0
negative_rows=0
remaining_gaps=16
futures_proxy_fill_rows=2,325
```

Các gap nguồn là các đoạn Binance spot historical không có candle trong monthly Vision, daily Vision, Spot REST, spot trades hoặc spot aggTrades. Nghiên cứu bổ sung ngày `2026-07-11`: Binance USD-M Futures `BTCUSDT` 1m cover được 15/31 gap, tổng `2,325` phút. Các đoạn này đã được fill vào raw spot theo policy trên với source `binance_usdm_futures_proxy_gap_fill`. Phần còn lại nằm trong 2018/2019, trước khi USD-M Futures local có coverage, nên được giữ nguyên để backtest dùng dữ liệu nào có dữ liệu đó.

## Loader

```python
from data_loader import CryptoBinanceSpot1m

df = CryptoBinanceSpot1m().load(
    symbols="BTCUSDT",
    start_date="2018-01-01",
    check_val=True,
)
```

Router aliases:

```python
load_data("crypto_binance_spot_1m", symbols="BTCUSDT")
load_data("binance_spot_1m", symbols="BTCUSDT")
load_data("crypto_spot_1m", symbols="BTCUSDT")
```
