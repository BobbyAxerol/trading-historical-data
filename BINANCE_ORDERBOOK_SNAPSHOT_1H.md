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

## Sync Logic

Collector [`collectors/binance_orderbook_snapshot_1h.py`](collectors/binance_orderbook_snapshot_1h.py) chạy theo thứ tự:

1. Seed rolling 30 ngày từ Binance Vision USD-M `daily/bookDepth/{SYMBOL}/`.
2. Downsample Vision từ khoảng 30s về 1h bằng snapshot cuối cùng trong mỗi bucket giờ.
3. Append REST `/fapi/v1/depth` với `limit=20` cho giờ hiện tại để giảm độ trễ do Vision thường publish ngày hôm sau.
4. Dedupe theo `symbol,time`, sort partition, rồi prune rows cũ hơn `lookback_days`.

Vision `bookDepth` là dữ liệu đã aggregate theo percent band (`timestamp,percentage,depth,notional`), nên historical features lấy trực tiếp từ band `-0.20/+0.20`, `-1/+1`, `-2/+2`, `-5/+5`. REST rows được tính từ top `depth_limit` L2 levels hiện tại; band xa như `5%` vì vậy chỉ phản ánh liquidity nằm trong top 20 levels tại thời điểm gọi REST.

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
