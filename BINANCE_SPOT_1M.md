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

Nếu `--repair-gaps` được bật, các gap có độ dài không vượt `max_gap_minutes` sẽ được gọi lại bằng daily Vision và REST rồi merge vào partition hiện có. Nếu vẫn còn gap nguồn không thể vá, bật thêm `--fill-source-gaps` để fill-forward phục vụ backtest:

- `open/high/low/close` = previous close trước gap;
- `volume/quote_volume/taker_buy_*` = `0`;
- `number_of_trades` = `0`;
- `source` = `synthetic_gap_fill_forward`.

Sau backfill đầu tiên ngày `2026-07-11`, audit `BTCUSDT` cho kết quả:

```text
rows=4,483,093
range=2018-01-01 00:00:00 -> 2026-07-11 06:12:00 UTC
duplicate_rows=0
ohlc_bad_rows=0
negative_rows=0
source_level_gaps_before_fill=31
synthetic_gap_fill_rows=8,065
remaining_gaps_after_fill=0
```

Các gap nguồn ban đầu là các đoạn Binance spot historical không có candle trong cả monthly Vision, daily Vision và Spot REST. Sau khi anh chọn fill cho backtest, collector ghi synthetic zero-volume candles với `source=synthetic_gap_fill_forward` để lưới 1m liên tục nhưng vẫn truy vết được đâu là dữ liệu fill.

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
