# Binance Order Book Snapshot 1h

Nguồn này lưu feature snapshot order book USD-M Futures theo giờ, phục vụ microstructure/backtest nhẹ trước khi nâng lên resolution dày hơn.

## Config

```yaml
depth_limit: 20
snapshot_interval: "1h"
lookback_days: 30
percent_bands: [0.002, 0.01, 0.02, 0.05]
primary_feature_band: 0.01
symbols: [BTCUSDT]
quarterly_pairs: [BTCUSDT]
```

`quarterly_pairs` được discover qua Binance `/fapi/v1/exchangeInfo`, chỉ lấy active `CURRENT_QUARTER` và `NEXT_QUARTER`, rồi lưu bằng concrete symbol như `BTCUSDT_260925`.

## Storage

```text
storage/crypto/binance_orderbook_snapshot/1h/symbol=BTCUSDT/year=YYYY/month=MM/part.csv.gz
```

Schema feature:

```text
time,sample_time,market,symbol,contract_type,mid_price,best_bid,best_ask,spread,spread_bps,
bid_depth_0_2pct,ask_depth_0_2pct,q_bid_depth_0_2pct,q_ask_depth_0_2pct,imbalance_0_2pct,
bid_depth_1pct,ask_depth_1pct,q_bid_depth_1pct,q_ask_depth_1pct,imbalance_1pct,
bid_depth_2pct,ask_depth_2pct,q_bid_depth_2pct,q_ask_depth_2pct,imbalance_2pct,
bid_depth_5pct,ask_depth_5pct,q_bid_depth_5pct,q_ask_depth_5pct,imbalance_5pct,
primary_bid_depth,primary_ask_depth,primary_q_bid_depth,primary_q_ask_depth,primary_imbalance,
depth_limit,source,ingested_at
```

`time` là bucket 1h UTC-naive dùng để dedupe/load. `sample_time` là timestamp thật của snapshot được chọn trong bucket giờ đó.

## Naming Convention

Trong dataset này, prefix `q_` trong các feature như `q_bid_depth_1pct` và `q_ask_depth_1pct` **luôn có nghĩa là quote-notional**, không có nghĩa là quarterly.

Quy ước bắt buộc:

- `bid_depth_*` / `ask_depth_*`: base quantity depth, ví dụ BTC quantity.
- `q_bid_depth_*` / `q_ask_depth_*`: quote-notional depth, tức `sum(price * quantity)`, đơn vị quote asset như USDT.
- `quarterly`: được biểu diễn bằng `symbol` concrete như `BTCUSDT_260925` và `contract_type` như `CURRENT_QUARTER` hoặc `NEXT_QUARTER`.
- Không dùng prefix `q_` để chỉ quarterly contract.

Do đó service downstream không được map `q_bid_depth_1pct` sang nghĩa "quarterly bid depth". Nếu cần feature riêng cho quarterly, hãy lọc theo `contract_type` hoặc `symbol`, rồi giữ nguyên nghĩa `q_ = quote-notional`.

## Sync Logic

Collector [`collectors/binance_orderbook_snapshot_1h.py`](collectors/binance_orderbook_snapshot_1h.py) chạy theo thứ tự:

1. Seed rolling 30 ngày từ Binance Vision USD-M `daily/bookDepth/{SYMBOL}/`.
2. Downsample Vision từ khoảng 30s về 1h bằng snapshot cuối cùng trong mỗi bucket giờ.
3. Append REST `/fapi/v1/depth` với `limit=20` cho giờ hiện tại để giảm độ trễ do Vision thường publish ngày hôm sau.
4. Dedupe theo `symbol,time`, sort partition, rồi prune rows cũ hơn `lookback_days`.

Vision `bookDepth` là dữ liệu đã aggregate theo percent band (`timestamp,percentage,depth,notional`), nên historical features lấy trực tiếp từ band `-0.20/+0.20`, `-1/+1`, `-2/+2`, `-5/+5`. REST rows được tính từ top `depth_limit` L2 levels hiện tại; band xa như `5%` vì vậy chỉ phản ánh liquidity nằm trong top 20 levels tại thời điểm gọi REST.

## Delayed Vision Catch-up

Binance Vision `daily/bookDepth` thường publish trễ so với thời gian thực. Service live **không cần chạy tay lại** để bù những ngày Vision publish muộn, miễn là container vẫn chạy với `--mode live` và không bật `--no-vision`.

Mỗi vòng service sẽ:

- quét lại toàn bộ rolling `lookback_days` gần nhất trên Vision;
- skip ngày nào storage đã có ít nhất một row trong ngày đó;
- tiếp tục thử các ngày còn thiếu trong storage;
- khi ZIP ngày thiếu xuất hiện trên Vision, tải file đó, downsample về 1h, append/dedupe vào storage;
- sau đó vẫn append REST snapshot giờ hiện tại.

Vì vậy audit có thể tạm thời báo `gap_count=1` khi Vision mới có dữ liệu tới ngày cũ hơn hôm nay, còn REST đã append snapshot hiện tại. Gap này sẽ tự khép lại khi Binance publish các file daily còn thiếu trong rolling 30 ngày. `gap_count` là số đoạn đứt continuity hourly, không phải số giờ thiếu.

## Feature Meaning

- `bid_depth_1pct`: tổng base quantity phía bid trong band 1%.
- `ask_depth_1pct`: tổng base quantity phía ask trong band 1%.
- `q_bid_depth_1pct`: tổng quote notional phía bid trong band 1%.
- `q_ask_depth_1pct`: tổng quote notional phía ask trong band 1%.
- `imbalance_1pct`: `(q_bid_depth_1pct - q_ask_depth_1pct) / (q_bid_depth_1pct + q_ask_depth_1pct)`.
- `primary_*`: alias của band `1%`.

## Loader

```python
from data_loader import BinanceOrderBookSnapshot1h

df = BinanceOrderBookSnapshot1h().load_features(
    symbols=["BTCUSDT", "BTCUSDT_260925"],
    start_date="2026-06-13",
    check_val=True,
)
```

Router aliases:

```python
load_data("crypto_binance_orderbook_snapshot_1h", symbols="BTCUSDT")
load_data("binance_orderbook_snapshot_1h", symbols="BTCUSDT")
load_data("orderbook_snapshot_1h", symbols="BTCUSDT")
```


## Downstream Contract: VPS2 basis_arb_binance

Consumer currently using this dataset:

```text
/root/bobby/execution_alpha/alphas/basis_arb_binance
```

Basis-arb loads two concrete symbols separately, for example `BTCUSDT` and `BTCUSDT_260925`, then maps quote-notional 1pct depth into the research feature names:

- perp `q_bid_depth_1pct` / `q_ask_depth_1pct` -> `bid_depth_1` / `ask_depth_1`;
- delivery `q_bid_depth_1pct` / `q_ask_depth_1pct` -> `q_bid_depth_1` / `q_ask_depth_1`.

This preserves the mandatory naming rule that source `q_` means quote-notional, not quarterly. Quarterly identity must always come from the concrete `symbol`/`contract_type` rows.

The consumer expects about 30 days of hourly rows. Temporary Vision catch-up gaps are acceptable if the latest rows are fresh enough, but the consumer may fail closed if the local cache is missing or stale.
