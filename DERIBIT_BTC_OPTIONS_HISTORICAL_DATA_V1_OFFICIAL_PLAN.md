# KẾ HOẠCH CHÍNH THỨC V1
# DERIBIT BTC OPTIONS HISTORICAL DATA PIPELINE
## Compact-Liquid Dataset cho QuantBT Option Engine

> **Trạng thái:** kế hoạch chính thức cho Version 1
> **Repository đích:** `BobbyAxerol/trading-historical-data`
> **Backtest consumer chính:** `BobbyAxerol/quantbt`, nhánh `feat/option-engine`
> **Nguồn tham khảo kiến trúc ingestion:** `RiveChen/deribit-historical-data`
> **Phạm vi:** historical BTC inverse option trades từ Deribit
> **Không thuộc phạm vi:** historical order book, WebSocket streaming, paper trading và live trading
> **Mục tiêu dung lượng permanent sau validation/cleanup:** khoảng **6–9 GiB**, không vượt **10 GiB**

---

## Mục lục

1. [Tóm tắt quyết định chính thức](#1-tóm-tắt-quyết-định-chính-thức)
2. [Bối cảnh và mục tiêu](#2-bối-cảnh-và-mục-tiêu)
3. [Phạm vi V1 và những phần không làm](#3-phạm-vi-v1-và-những-phần-không-làm)
4. [Yêu cầu từ QuantBT Option Engine](#4-yêu-cầu-từ-quantbt-option-engine)
5. [Những gì học từ repository RiveChen](#5-những-gì-học-từ-repository-rivechen)
6. [Những gì không bê nguyên từ repository tham khảo](#6-những-gì-không-bê-nguyên-từ-repository-tham-khảo)
7. [Kiến trúc tổng thể V1](#7-kiến-trúc-tổng-thể-v1)
8. [Nguyên tắc versioning](#8-nguyên-tắc-versioning)
9. [Deribit History API và chiến lược pagination](#9-deribit-history-api-và-chiến-lược-pagination)
10. [Instrument discovery](#10-instrument-discovery)
11. [Sequence-based lazy scheduling](#11-sequence-based-lazy-scheduling)
12. [Disk-first và bounded-memory ingestion](#12-disk-first-và-bounded-memory-ingestion)
13. [Coverage ledger và checkpoint SQLite](#13-coverage-ledger-và-checkpoint-sqlite)
14. [V1 broad ingestion universe](#14-v1-broad-ingestion-universe)
15. [Contract activation và retention](#15-contract-activation-và-retention)
16. [Canonical trade-event schema](#16-canonical-trade-event-schema)
17. [Instrument dimension schema](#17-instrument-dimension-schema)
18. [Staging Parquet và atomic write](#18-staging-parquet-và-atomic-write)
19. [Daily compaction](#19-daily-compaction)
20. [Validation và repair](#20-validation-và-repair)
21. [Snapshot 5m Compact-Liquid](#21-snapshot-5m-compact-liquid)
22. [Expiry selection](#22-expiry-selection)
23. [Strike và delta selection](#23-strike-và-delta-selection)
24. [Liquidity và activity filter](#24-liquidity-và-activity-filter)
25. [Hard cap 64 contracts mỗi snapshot](#25-hard-cap-64-contracts-mỗi-snapshot)
26. [Reconstruction giữa các trades](#26-reconstruction-giữa-các-trades)
27. [Global BTC index từ historical option trades](#27-global-btc-index-từ-historical-option-trades)
28. [Greeks và pricing provenance](#28-greeks-và-pricing-provenance)
29. [Candidate tape và held-position overlay](#29-candidate-tape-và-held-position-overlay)
30. [Snapshot 1m on-demand](#30-snapshot-1m-on-demand)
31. [Execution proxy từ trade và mark](#31-execution-proxy-từ-trade-và-mark)
32. [Block, combo và liquidation trades](#32-block-combo-và-liquidation-trades)
33. [Look-ahead prevention](#33-look-ahead-prevention)
34. [Storage layout trong trading-historical-data](#34-storage-layout-trong-trading-historical-data)
35. [Cấu trúc module migration](#35-cấu-trúc-module-migration)
36. [Các storage primitive cần bổ sung](#36-các-storage-primitive-cần-bổ-sung)
37. [Data loader và QuantBT adapter](#37-data-loader-và-quantbt-adapter)
38. [CLI và chế độ vận hành](#38-cli-và-chế-độ-vận-hành)
39. [Docker và lịch incremental sync](#39-docker-và-lịch-incremental-sync)
40. [Dependencies](#40-dependencies)
41. [Cấu hình YAML chính thức V1](#41-cấu-hình-yaml-chính-thức-v1)
42. [Disk budget và capacity planning](#42-disk-budget-và-capacity-planning)
43. [Cleanup transaction](#43-cleanup-transaction)
44. [Disk-pressure fallback policy](#44-disk-pressure-fallback-policy)
45. [Ước tính API calls, RAM và thời gian](#45-ước-tính-api-calls-ram-và-thời-gian)
46. [Metrics và observability](#46-metrics-và-observability)
47. [Test plan](#47-test-plan)
48. [Pilot benchmark bắt buộc](#48-pilot-benchmark-bắt-buộc)
49. [Implementation phases](#49-implementation-phases)
50. [Acceptance criteria](#50-acceptance-criteria)
51. [Các invariant bắt buộc](#51-các-invariant-bắt-buộc)
52. [Các giới hạn đã chấp nhận của V1](#52-các-giới-hạn-đã-chấp-nhận-của-v1)
53. [Lộ trình V2 và các phiên bản sau](#53-lộ-trình-v2-và-các-phiên-bản-sau)
54. [Kết luận](#54-kết-luận)
55. [Nguồn tham khảo](#55-nguồn-tham-khảo)

---

# 1. Tóm tắt quyết định chính thức

V1 được định nghĩa là:

> **Một historical BTC option dataset tập trung vào vùng thanh khoản, đủ cho các option strategy phổ biến trong QuantBT, được tải theo `trade_seq`, ghi xuống disk theo chunk, xây candidate tape 5 phút có giới hạn cứng và giữ permanent storage dưới 10 GiB.**

Các quyết định mặc định:

| Hạng mục | Quyết định V1 |
|---|---|
| Nguồn dữ liệu | Deribit historical public option trades |
| Underlying | BTC |
| Kiểu option | BTC inverse options |
| Pagination chính | Theo từng instrument và `trade_seq` |
| API page size ban đầu | 5.000 rows; có thể benchmark tăng lên 10.000 |
| Checkpoint | SQLite, per instrument + per requested sequence range |
| Hot storage | Immutable Parquet micro-parts |
| JSONL | Không bật mặc định |
| Raw full-history archive | Không lưu toàn bộ |
| Broad retained events | DTE tối đa 120; broad moneyness; sticky activation |
| Canonical snapshot | 5 phút |
| 5m expiries tối đa | 7 |
| 5m rows tối đa | 64 contracts/timestamp |
| 1m full-history | Không lưu |
| 1m | On-demand, DTE ≤ 2, gần ATM |
| Static contract fields | Tách thành instrument dimension |
| Full Greeks | Tính on load; không lưu lặp lại mặc định |
| Bid/ask proxy | Tính on load từ execution distribution |
| Permanent storage target | 6–9 GiB |
| Hard operational stop | 9 GiB canonical budget |
| Filesystem target | Không vượt 10 GiB sau cleanup |
| Peak free disk khi build | Nên có 15–20 GiB |
| Dataset ID | `deribit_btc_options_v1_compact_liquid` |

---

# 2. Bối cảnh và mục tiêu

Repository `trading-historical-data` là data layer tập trung, ưu tiên:

- Historical coverage.
- Resume sau restart.
- Deduplication.
- Gap audit.
- Parquet storage.
- Stable loader API.
- Phục vụ backtest thay vì phục vụ trực tiếp order execution live.

QuantBT nhánh `feat/option-engine` đang xây một option backtest engine với các package strategy phổ biến:

- Long/short call.
- Long/short put.
- Straddle.
- Strangle.
- Vertical spread.
- Butterfly.
- Condor.
- Calendar.
- Covered call.
- Collar.
- Risk reversal.
- Các biến thể delta-hedged.

Những strategy này cần:

- Nhiều expiries đại diện.
- ATM và các strikes lân cận.
- Một số target delta phổ biến.
- Giá và IV đủ để mark position.
- Liquidity proxy hợp lý.
- Không nhất thiết cần toàn bộ deep wings và long-dated contracts.

V1 vì vậy không nhắm tới việc lưu “mọi thứ Deribit từng có”.

Mục tiêu đúng là:

1. Tải và xác minh historical trades chính xác.
2. Chỉ giữ data có giá trị cho vùng backtest phổ biến.
3. Không dùng RAM theo quy mô toàn lịch sử.
4. Không lưu dense full-chain.
5. Có thể mở rộng bằng dataset version mới.
6. Có provenance rõ giữa observed, reconstructed và proxy data.
7. Permanent storage sau cleanup không vượt 10 GiB.

---

# 3. Phạm vi V1 và những phần không làm

## 3.1 V1 làm

- Tải historical BTC option trades.
- Discover active và expired instruments.
- Pagination theo `trade_seq`.
- Resume chính xác theo instrument.
- Ghi Parquet theo chunk.
- Lưu coverage ledger.
- Filter broad universe.
- Compact thành canonical daily Parquet.
- Xây sparse trade bars.
- Xây candidate snapshot 5m.
- Reconstruct mark giữa hai lần trade.
- Tính model delta phục vụ selection.
- Xây execution proxy từ actual trade so với exchange mark.
- Adapter cho QuantBT.
- Optional 1m near-ATM cache.
- Versioning và disk-budget enforcement.

## 3.2 V1 không làm

- Historical order-book reconstruction.
- Historical top-of-book chính xác.
- Streaming WebSocket.
- Paper-trading market data.
- Live-trading market data.
- Live order gateway.
- Queue-position simulation.
- Exact market impact.
- Full portfolio margin.
- Full volatility surface SVI/SSVI.
- Long-dated tail universe.
- 5-delta wings.
- Full ETH option history.
- Permanent full-history 1m chain.

---

# 4. Yêu cầu từ QuantBT Option Engine

QuantBT option engine sử dụng long-form option rows và selectors theo:

- ATM.
- Delta target.
- DTE target.
- Moneyness.
- Option type.
- Expiry pairing.
- Package leg construction.

Do đó data không cần là dense matrix:

```text
timestamp × toàn bộ listed contracts
```

Data phù hợp hơn là ragged tape:

```text
timestamp
  ├── contract A
  ├── contract B
  ├── contract C
  └── ...
```

V1 phải hỗ trợ đủ contract candidates cho:

| Strategy | Coverage cần |
|---|---|
| Single call/put | ATM hoặc target delta |
| Straddle | ATM call + ATM put cùng expiry |
| Strangle | OTM call + OTM put |
| Vertical | Hai strikes cùng option type và expiry |
| Butterfly | Ba strikes ordered cùng expiry |
| Condor | Bốn strikes ordered cùng expiry |
| Calendar | Cùng/sát strike, khác expiry |
| Covered call | OTM call đủ thanh khoản |
| Collar | OTM call + OTM put |
| Risk reversal | Put delta target + call delta target |
| Gamma scalping | Gần ATM, DTE ngắn, frequency cao hơn |

V1 vì vậy giữ:

- ATM.
- Adjacent strikes.
- 15/25-delta wings ở short maturities.
- Near-ATM far expiry cho calendar.
- 1m chỉ cho near-ATM 0DTE/near-expiry.

---

# 5. Những gì học từ repository RiveChen

Repository tham khảo có các quyết định đúng domain và nên được áp dụng:

## 5.1 Page bằng `trade_seq`

`trade_seq`:

- Monotonic theo instrument.
- Có thể chia exact ranges.
- Resume đơn giản.
- Không bị lỗi nhiều trades cùng millisecond.
- Có thể audit exact range.

Kế hoạch V1 dùng:

```text
instrument
start_seq
end_seq
```

thay vì pagination chính bằng time window.

## 5.2 Option lazy scheduling

Số option instruments rất lớn nhưng phần lớn có ít hoặc không có trade.

Không pre-allocate mọi chunk.

Flow:

```text
enqueue task đầu tiên của instrument
→ fetch
→ nếu còn dữ liệu thì enqueue chunk kế tiếp
→ nếu hết thì complete
```

## 5.3 Bounded producer–consumer

Tách:

```text
task queue
→ API workers
→ bounded write queue
→ disk writer
```

Queue có max size để backpressure hoạt động.

## 5.4 SQLite checkpoint

SQLite phù hợp với hàng chục/hàng trăm nghìn instrument states hơn JSON manifest lớn.

Dùng:

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
```

V1 có thể dùng `FULL` cho critical commit nếu benchmark vẫn đủ nhanh, nhưng mặc định `NORMAL` hợp lý khi disk artifacts là source khôi phục.

## 5.5 Disk trước, checkpoint sau

Thứ tự bắt buộc:

```text
write temporary file
→ fsync/close
→ atomic rename
→ record file/checksum
→ update checkpoint
```

Nếu crash giữa file write và checkpoint, chỉ phát sinh duplicate fetch.

Nếu checkpoint trước file write, có thể mất dữ liệu vĩnh viễn.

## 5.6 Deduplicate tại compaction

Không làm dedup phức tạp trong hot path.

Hot path ưu tiên:

- Fetch.
- Filter.
- Write.
- Checkpoint.

Compactor xử lý:

```text
(instrument_id, trade_seq)
```

## 5.7 Phân biệt empty và unknown

Ba trạng thái:

```text
0 trades confirmed
positive sequence/data
unknown because request failed
```

Không bao giờ dùng `0` để đại diện cả empty lẫn error.

## 5.8 Tôn trọng `Retry-After`

Khi HTTP 429:

1. Dùng `Retry-After` nếu có.
2. Nếu không có, exponential backoff + jitter.
3. Không dùng `sleep(0.06)` cố định làm rate limiter duy nhất.

---

# 6. Những gì không bê nguyên từ repository tham khảo

## 6.1 Không lưu một JSONL file mỗi instrument

Vấn đề:

- Quá nhiều files.
- Tốn inode.
- Scan chậm.
- Backup/rsync khó.
- JSONL + Parquet làm peak disk tăng.

V1 mặc định ghi direct immutable Parquet micro-parts.

## 6.2 Không giữ full raw history

Repository tham khảo tải toàn bộ, trong khi V1 có disk budget 10 GiB và chỉ cần liquidity-focused universe.

V1 vẫn probe/download theo sequence nhưng chỉ persist broad relevant data.

## 6.3 Không tạo một giant Parquet

Canonical data partition theo trade date.

Lợi ích:

- Incremental append.
- Partition pruning.
- Compaction theo ngày.
- Repair theo partition.
- Không rewrite file nhiều GB.

## 6.4 Không dùng full-history Pandas concatenation

Không có:

```python
all_trades = []
frames = []
pd.concat(frames)
```

ở quy mô lịch sử.

## 6.5 Không chỉ report gap

V1 có:

```text
validate
→ produce unresolved ranges
→ repair exact ranges
→ revalidate
```

---

# 7. Kiến trúc tổng thể V1

```text
Deribit History API
        │
        ▼
Instrument Discovery
        │
        ▼
SQLite Instrument State
        │
        ▼
Lazy Sequence Task Scheduler
        │
        ▼
Bounded Async API Workers
        │
        ▼
Chunk Parse + Broad Filter
        │
        ▼
Immutable Staging Parquet
        │
        ▼
Coverage Ledger Commit
        │
        ▼
Checkpoint Advance
        │
        ▼
Daily External-Memory Compactor
        │
        ├── Canonical Trade Events
        ├── Instrument Dimension
        └── Validation Reports
        │
        ▼
5m Compact-Liquid Snapshot Builder
        │
        ├── Candidate Tape
        ├── Execution Statistics
        └── Terminal State
        │
        ▼
QuantBT Adapter
        ├── Ragged Candidate Tape
        └── Held-Instrument Overlay
```

---

# 8. Nguyên tắc versioning

Mọi artifact phải có version rõ ràng:

```text
dataset_version
schema_version
universe_version
pricing_version
snapshot_version
execution_proxy_version
```

V1:

```yaml
versions:
  dataset: deribit_btc_options_v1
  schema: trade_schema_v1
  universe: compact_liquid_v1
  pricing: anchored_iv_v1
  snapshot_5m: compact_5m_v1
  snapshot_1m: near_atm_1m_v1
  execution_proxy: trade_mark_v1
```

## 8.1 V2 không overwrite V1

Ví dụ V2 mở rộng:

- DTE 180.
- Delta 0.05.
- ETH.
- Futures-forward curve.
- Better IV interpolation.

Storage:

```text
version=v1
version=v2
```

## 8.2 Checkpoint không dùng chung giữa universe versions

V1 có thể discard một trade mà V2 muốn giữ.

Do đó checkpoint path phải chứa dataset/universe version:

```text
state/deribit_options/version=v1/BTC.sqlite
```

---

# 9. Deribit History API và chiến lược pagination

Repository tham khảo đã test với:

```text
Base URL:
https://history.deribit.com/api/v2/public
```

Endpoints chính:

```text
/get_instruments
/get_last_trades_by_instrument
```

Repository tham khảo ghi nhận page size tối đa 10.000.

V1 phải có API smoke test vì behavior có thể thay đổi:

```text
count max
response sorting
has_more semantics
trade_seq overlap
expired instrument availability
rate limit
```

## 9.1 Page size V1

Default:

```yaml
sequence_chunk_size: 5000
```

Lý do:

- Giảm response peak memory.
- Một JSON response 5.000 trades vẫn đủ nhanh.
- Có thể tăng lên 10.000 sau pilot.

## 9.2 Trade ordering

Response có thể descending theo `trade_seq`.

Trong chunk:

```python
trades.sort(key=lambda x: x["trade_seq"])
```

trước khi apply stateful activation/filter.

## 9.3 Chunk boundaries

Task:

```text
instrument_name
start_seq
end_seq
```

Next:

```text
next_seq = max_trade_seq_received + 1
```

Hoặc exact range increment nếu API đã xác nhận full range.

Không dùng timestamp cursor.

---

# 10. Instrument discovery

Gọi:

```text
get_instruments(currency=BTC, kind=option, expired=false)
get_instruments(currency=BTC, kind=option, expired=true)
```

Merge theo `instrument_name`.

Lưu dimension metadata:

```text
instrument_name
creation_timestamp
expiration_timestamp
strike
option_type
contract_size
tick_size
min_trade_amount
settlement_currency
is_active
discovered_at
```

Nếu field không có trong historical response, parse từ name như fallback.

## 10.1 Parse format

Ví dụ:

```text
BTC-27MAR26-100000-C
```

Kết quả:

```text
currency = BTC
expiry = 2026-03-27 08:00:00 UTC
strike = 100000
option_type = call
```

## 10.2 Invalid metadata

Không silently set 0.

Dùng:

```text
parse_status
metadata_source
quality_flags
```

---

# 11. Sequence-based lazy scheduling

Mỗi incomplete instrument bắt đầu với một task:

```text
start_seq = last_processed_seq + 1
end_seq = start_seq + chunk_size - 1
```

Sau success:

- Nếu response có trades và có thể còn data: enqueue next range.
- Nếu expired instrument đã hết: mark completed.
- Nếu active instrument đã hết hiện tại: mark caught-up, không completed vĩnh viễn.
- Nếu confirmed empty expired instrument: completed.
- Nếu request unknown/error: retry hoặc dead-letter, không completed.

## 11.1 Instrument states

```text
NEW
IN_PROGRESS
CAUGHT_UP_ACTIVE
COMPLETE_EXPIRED
EMPTY_CONFIRMED
RETRYABLE_ERROR
DEAD_LETTER
```

## 11.2 Active instrument

Active instruments:

- Không permanent completed.
- `sync-once` tiếp tục từ cursor cũ.
- Refresh instrument discovery trước mỗi incremental run.

---

# 12. Disk-first và bounded-memory ingestion

## 12.1 Default limits

```yaml
runtime:
  api_workers: 4
  task_queue_size: 16
  write_queue_size: 8
  writer_workers: 1
  chunk_size: 5000
  max_inflight_chunks: 8
```

## 12.2 Hot path

```text
HTTP response
→ parse tối đa 5.000 trades
→ sort theo trade_seq
→ derive fields tối thiểu
→ apply broad retention logic
→ Arrow Table
→ write .parquet.tmp
→ close/fsync
→ atomic rename
→ coverage ledger commit
→ instrument checkpoint update
→ release objects
```

## 12.3 Không batch quá lớn trong consumer

Write consumer không gom hàng chục chunks.

Default:

```text
một API result → một immutable part
```

Có thể merge 2–4 tiny results nếu tổng rows nhỏ, nhưng phải có hard row/byte cap.

## 12.4 Memory target

```yaml
memory:
  ingestion_target_rss_mb: 500
  ingestion_hard_rss_mb: 750
  compactor_memory_limit_mb: 1024
  snapshot_builder_target_rss_mb: 1000
  snapshot_builder_hard_rss_mb: 1400
```

---

# 13. Coverage ledger và checkpoint SQLite

Vì V1 discard intentional trades, sequence trong canonical Parquet không liên tục.

Validation phải dựa vào coverage ledger.

## 13.1 `instrument_state`

```sql
CREATE TABLE instrument_state (
    instrument_name TEXT PRIMARY KEY,
    instrument_id INTEGER,
    is_expired INTEGER NOT NULL,
    status TEXT NOT NULL,
    last_processed_seq INTEGER NOT NULL DEFAULT 0,
    last_success_at TEXT,
    last_attempt_at TEXT,
    failure_count INTEGER NOT NULL DEFAULT 0,
    last_error_code TEXT,
    last_error_message TEXT,
    dataset_version TEXT NOT NULL,
    config_hash TEXT NOT NULL
);
```

## 13.2 `download_ranges`

```sql
CREATE TABLE download_ranges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_name TEXT NOT NULL,
    requested_start_seq INTEGER NOT NULL,
    requested_end_seq INTEGER NOT NULL,

    response_min_seq INTEGER,
    response_max_seq INTEGER,
    response_trade_count INTEGER NOT NULL DEFAULT 0,

    retained_trade_count INTEGER NOT NULL DEFAULT 0,
    discarded_trade_count INTEGER NOT NULL DEFAULT 0,

    output_file TEXT,
    output_checksum TEXT,

    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,

    dataset_version TEXT NOT NULL,
    config_hash TEXT NOT NULL,

    UNIQUE (
        instrument_name,
        requested_start_seq,
        requested_end_seq,
        dataset_version
    )
);
```

## 13.3 Commit order

```text
1. Write temporary file.
2. Atomic rename.
3. Insert/commit download_ranges.
4. Advance instrument_state.last_processed_seq.
```

## 13.4 Empty retained result

Nếu response có 5.000 trades nhưng không row nào qua V1 filter:

- Không ghi empty Parquet.
- Ghi `response_trade_count=5000`.
- Ghi `retained_trade_count=0`.
- Ghi `discarded_trade_count=5000`.
- `output_file=NULL`.
- Advance checkpoint sau coverage commit.

---

# 14. V1 broad ingestion universe

V1 không persist toàn bộ raw history.

Broad filter phải:

- Đủ rộng để không khóa strategy quá chặt.
- Không phụ thuộc quá sâu vào pricing model.
- Loại phần rõ ràng không dùng.
- Cho phép retain contract sau activation.

## 14.1 Broad conditions

Trade hợp lệ để xét retention:

```text
expiry parse được
timestamp < expiry
0 <= DTE <= 120
index_price > 0
mark_price >= 0
IV > 0
price >= 0
amount > 0
```

Broad moneyness:

$$
M_t = \frac{K}{S_t}
$$

Default:

$$
0.50 \leq M_t \leq 2.00
$$

## 14.2 Vì sao không dùng delta làm hot ingestion filter chính

Delta phụ thuộc:

- Pricing convention.
- IV.
- Forward.
- Model version.

DTE + moneyness:

- Rẻ.
- Dễ audit.
- Model-independent hơn.
- Phù hợp hot path.

Delta được dùng ở snapshot stage.

## 14.3 Optional broader activation trigger

Để tránh bỏ contract trong volatility regime rất lớn:

```yaml
activation_moneyness:
  normal_min: 0.50
  normal_max: 2.00
  emergency_min: 0.40
  emergency_max: 2.50
  emergency_requires_regular_trade_count: 2
```

V1 mặc định có thể tắt emergency envelope nếu pilot cho thấy disk cao.

---

# 15. Contract activation và retention

Nếu filter từng trade độc lập, một contract có thể biến mất khi underlying dịch chuyển.

V1 dùng stateful activation.

## 15.1 Activation

Contract activated khi:

```text
DTE <= 120
và moneyness nằm trong broad envelope
và trade record hợp lệ
```

Lưu:

```text
activated_at
activation_seq
activation_reason
```

## 15.2 Retention

Sau activation:

- Giữ các trade tiếp theo đến expiry.
- Không tiếp tục áp moneyness hard drop cho từng trade.
- Có thể bỏ malformed records.
- Có thể drop block/combo payload fields sau classification.

Lợi ích:

- Position không mất anchor khi contract trở thành deep ITM/OTM.
- Có dữ liệu cập nhật cho settlement path.
- Broad filter vẫn giảm phần history trước khi contract trở nên relevant.

## 15.3 Historical ordering

Phải process theo ascending `trade_seq` để activation state đúng.

---

# 16. Canonical trade-event schema

Canonical event fact table phải compact.

## 16.1 Persistent columns

| Column | Type | Ý nghĩa |
|---|---|---|
| `timestamp_ms` | Int64 | Exchange timestamp |
| `instrument_id` | UInt32 | Numeric dimension key |
| `trade_seq` | Int64 | Sequence trong instrument |
| `trade_id_hash` | UInt64 nullable | Hash của trade ID để audit |
| `price_btc` | Float64 | Actual trade premium |
| `mark_price_btc` | Float64 | Exchange mark tại trade |
| `iv_pct` | Float32 | Trade IV |
| `index_price_usd` | Float64 | BTC index tại trade |
| `amount_base` | Float32 | Trade amount |
| `contracts` | Float32 nullable | Contracts nếu API trả |
| `direction` | Int8 | buy/sell encoded |
| `tick_direction` | Int8 nullable | Deribit tick direction |
| `flags` | UInt16 | block/combo/liquidation/quality |
| `dataset_version_id` | UInt16 | Version key |

## 16.2 Derived columns không cần persist

Tính on load:

```text
datetime_utc
dte
ttm_years
strike_to_index
log_moneyness
premium_usd
mark_usd
trade_mark_relative_cost
```

Static expiry/strike/type lấy từ instrument dimension.

## 16.3 Vì sao không giữ full `trade_id` string

`trade_seq` đã unique trong instrument.

Để tiết kiệm disk:

- Staging có thể giữ full `trade_id`.
- Compactor validate uniqueness.
- Canonical chỉ giữ `trade_id_hash` nếu cần audit.
- Full trade ID không phục vụ backtest trực tiếp.

## 16.4 Rare IDs

V1 canonical không giữ full:

```text
block_trade_id
combo_trade_id
combo_id
block_rfq_id
```

Chỉ giữ classification flags.

V2 combo-research có thể dùng schema khác.

---

# 17. Instrument dimension schema

```text
instrument_id             UInt32
instrument_name           String
currency                  dictionary
expiry_timestamp_ms       Int64
strike_usd                Float64
option_type               Int8
creation_timestamp_ms     Int64 nullable
contract_size             Float32
tick_size                 Float64 nullable
min_trade_amount          Float32 nullable
settlement_currency       dictionary
is_expired                Boolean
activated_at_ms           Int64 nullable
activation_seq            Int64 nullable
metadata_source           Int8
parse_status              Int8
dataset_version_id        UInt16
```

Static fields không lặp lại trong trade/snapshot facts.

---

# 18. Staging Parquet và atomic write

## 18.1 Path

```text
storage/_staging/
└── options/
    └── deribit/
        └── version=v1/
            └── currency=BTC/
                └── shard=00/
                    └── run_id=20260724T.../
                        └── instrument=12345/
                            └── seq_000000001_000005000.parquet
```

Có thể bỏ directory instrument và encode vào filename để giảm directory count.

Alternative:

```text
shard=00/run_id=.../part-000001.parquet
```

mỗi part chứa một instrument chunk.

## 18.2 Sharding

```python
shard = stable_hash(instrument_name) % 64
```

## 18.3 Atomic publish

```text
filename.parquet.tmp
→ write
→ close
→ fsync
→ os.replace(tmp, final)
```

## 18.4 File metadata

Parquet metadata:

```text
dataset_version
config_hash
instrument_name
requested_start_seq
requested_end_seq
response_count
retained_count
created_at
```

---

# 19. Daily compaction

Compactor đọc staging files và phân vùng theo trade date.

## 19.1 Canonical path

```text
storage/options/deribit/trades/
└── version=v1/
    └── currency=BTC/
        └── year=2026/
            └── month=07/
                └── day=24/
                    ├── part-00000.parquet
                    └── part-00001.parquet
```

## 19.2 Không dùng append-rewrite store hiện tại

Không:

```text
read existing partition
→ concat new batch
→ rewrite all
```

Thay bằng:

```text
immutable parts
→ external-memory compaction
→ atomic publish compacted partition
```

## 19.3 DuckDB external-memory query

Ví dụ:

```sql
PRAGMA memory_limit='1GB';
PRAGMA temp_directory='storage/_tmp/duckdb';

COPY (
    SELECT * EXCLUDE (rn)
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY instrument_id, trade_seq
                ORDER BY source_priority DESC, ingested_at DESC
            ) AS rn
        FROM read_parquet($INPUTS, union_by_name=true)
    )
    WHERE rn = 1
    ORDER BY timestamp_ms, instrument_id, trade_seq
)
TO $OUTPUT
(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 250000);
```

## 19.4 Compaction granularity

- Theo ngày.
- Không load nhiều năm.
- Có thể compact một ngày nhiều output files 128–256 MiB.

---

# 20. Validation và repair

## 20.1 Acquisition validation

Coverage ledger kiểm tra:

- Không có unknown requested ranges.
- Không có output file missing khi retained_count > 0.
- Checksums match.
- `response_count = retained + discarded`.
- Instrument cursor không rollback.
- Error range không bị mark success.

## 20.2 Canonical validation

- Không duplicate `(instrument_id, trade_seq)`.
- Timestamp hợp lệ.
- Price/index/IV finite.
- DTE tại trade trong retention policy hoặc contract đã activated.
- Static dimension tồn tại.
- Flags hợp lệ.
- Schema version đúng.

## 20.3 Trade ID conflict

Nếu cùng `(instrument_id, trade_seq)` nhưng payload khác:

- Không silent choose.
- Ghi quarantine.
- Chọn source theo deterministic priority.
- Raise validation warning/error tùy field conflict.

## 20.4 Repair

CLI:

```bash
python -m collectors.deribit_option_trades validate \
  --version v1 \
  --currency BTC

python -m collectors.deribit_option_trades repair \
  --version v1 \
  --currency BTC \
  --only-unresolved
```

Repair theo exact sequence range.

## 20.5 Intentional canonical gaps

Canonical sequence không cần dense vì V1 discard pre-activation trades.

Correctness nằm ở coverage ledger, không nằm ở:

$$
max(seq)-min(seq)+1 = row\_count
$$

của canonical filtered table.

---

# 21. Snapshot 5m Compact-Liquid

Permanent derived dataset:

```text
snapshot_5m_v1
```

## 21.1 Mục tiêu

- Candidate universe đủ cho strategy phổ biến.
- Tập trung contracts có thanh khoản tương đối.
- Không giữ toàn chain.
- Không vượt disk budget.
- Ragged long-form compatible với QuantBT.

## 21.2 Snapshot cadence

UTC grid:

```text
00:00
00:05
00:10
...
```

Deribit options trade 24/7 nhưng expiry convention phải được xử lý tại thời điểm expiry.

## 21.3 Snapshot fact schema

| Column | Type |
|---|---|
| `timestamp_ms` | Int64 |
| `instrument_id` | UInt32 |
| `mark_price_btc` | Float64 |
| `last_trade_price_btc` | Float64 nullable |
| `index_price_usd` | Float64 |
| `iv_pct` | Float32 |
| `model_delta` | Float32 |
| `volume_5m` | Float32 |
| `trade_count_5m` | UInt16 |
| `buy_volume_5m` | Float32 |
| `sell_volume_5m` | Float32 |
| `anchor_age_seconds` | UInt32 |
| `quality_flags` | UInt16 |
| `entry_eligible` | Boolean |

Không persist:

```text
gamma
vega
theta
proxy_bid
proxy_ask
proxy_size
instrument_name
expiry
strike
option_type
```

---

# 22. Expiry selection

V1 target DTE ladder:

```yaml
target_dte_days:
  - 0
  - 7
  - 14
  - 30
  - 60
  - 90
```

Optional far calendar:

```yaml
far_calendar_target_dte: 120
```

## 22.1 Selection

Tại mỗi universe rebalance:

1. Lấy expiries còn sống.
2. Tính DTE.
3. Với mỗi target DTE, chọn expiry gần nhất.
4. Deduplicate.
5. Optional chọn một expiry 91–120 nếu đủ activity.
6. Cap:

```text
max_core_expiries = 6
max_total_expiries = 7
```

## 22.2 Rebalance frequency

Không cần thay expiry selection mỗi 5 phút.

Default:

```text
expiry universe rebalance = 1 lần/ngày, 00:00 UTC
```

Nếu expiry trong ngày:

- 0DTE expiry được xử lý.
- Sau expiry, remove.
- Next daily/forced rebalance chọn replacement.

## 22.3 Calendar protection

Near/far pairs cần phủ:

```text
7 → 30/45
14 → 45/60
30 → 60/90
45/60 → 90/120
```

V1 target ladder không có 45 mặc định để tiết kiệm, nhưng selector có thể chọn expiry thực tế gần target khi 30/60 spacing không phù hợp.

---

# 23. Strike và delta selection

V1 không giữ mọi contract trong delta range.

## 23.1 DTE 0–14

```yaml
delta_range: 0.12_to_0.88
atm_strikes_each_side: 3
include_15_delta_if_active: true
max_contracts_per_expiry: 16
```

## 23.2 DTE 15–45

```yaml
delta_range: 0.15_to_0.85
atm_strikes_each_side: 2
include_25_delta_if_active: true
max_contracts_per_expiry: 12
```

## 23.3 DTE 46–90

```yaml
delta_range: 0.20_to_0.80
atm_strikes_each_side: 2
max_contracts_per_expiry: 10
```

## 23.4 DTE 91–120

```yaml
delta_range: 0.40_to_0.60
atm_strikes_each_side: 1
max_contracts_per_expiry: 6
max_expiries: 1
```

## 23.5 Selection priority

Mỗi expiry:

1. ATM call.
2. ATM put.
3. Adjacent strikes.
4. 25-delta call/put.
5. 15-delta call/put ở DTE ngắn.
6. Calendar-compatible strikes.
7. Remaining high-liquidity contracts.

## 23.6 Package completeness

Butterfly/condor cần ordered strikes.

Selector phải ưu tiên contiguous strike clusters quanh ATM thay vì chỉ chọn isolated target deltas.

---

# 24. Liquidity và activity filter

Historical trades-only không có quote spread thật.

V1 dùng actual regular trade activity làm liquidity evidence.

## 24.1 Regular trade

Mặc định không tính vào activity:

- Block.
- Combo.
- Liquidation.

## 24.2 Activity windows

| DTE | Active nếu |
|---|---|
| 0–14 | ≥2 regular trades trong 6h hoặc volume ≥1 BTC |
| 15–45 | ≥2 regular trades trong 12h hoặc volume ≥1 BTC |
| 46–90 | ≥2 regular trades trong 24h |
| 91–120 | ≥2 regular trades trong 72h |

Các threshold phải được freeze sau pilot.

## 24.3 Anchor TTL

| DTE | Entry anchor TTL |
|---|---:|
| 0–14 | 2 giờ |
| 15–45 | 4 giờ |
| 46–90 | 8 giờ |
| 91–120 | 24 giờ |

Contract stale hơn TTL:

```text
entry_eligible = false
```

Nhưng state vẫn có thể được dựng cho held-position overlay nếu có anchor hợp lệ trong valuation TTL.

## 24.4 Liquidity score

$$
L =
w_r R +
w_c C +
w_v V +
w_t T
$$

Trong đó:

- $R$: recency score.
- $C$: trade count score.
- $V$: volume score.
- $T$: target/package coverage score.

Default weights pilot:

```yaml
weights:
  recency: 0.35
  trade_count: 0.25
  volume: 0.20
  target_coverage: 0.20
```

---

# 25. Hard cap 64 contracts mỗi snapshot

```yaml
max_rows_per_timestamp: 64
```

Cap áp dụng cho permanent candidate tape.

## 25.1 Khi candidates > 64

Reserved slots:

1. ATM call/put của core expiries.
2. Required adjacent strike clusters.
3. 25-delta structures.
4. Far-calendar ATM cluster.

Sau đó rank remaining theo liquidity score.

## 25.2 Tại sao cap 64

Maximum daily rows:

$$
64 \times 288 = 18{,}432
$$

Maximum rows cho 3.400 ngày:

$$
18{,}432 \times 3{,}400
\approx 62.7\text{ triệu}
$$

Thực tế thấp hơn vì:

- Early history ít contracts.
- Không phải snapshot nào đạt cap.
- Activity/TTL loại candidates.
- Far calendar optional.

## 25.3 Disk fallback

Nếu pilot cho thấy 64 quá lớn:

```text
64 → 56 → 48
```

Không thay schema/version; thay universe config phải tạo `universe_version` mới hoặc minor version.

---

# 26. Reconstruction giữa các trades

Historical trades-only cần reconstructed state để có 5m tape.

## 26.1 Observed anchor

Tại trade gần nhất $\tau$:

```text
exchange mark Mτ
trade IV στ
index Sτ
expiry T
strike K
```

## 26.2 Anchored relative repricing

$$
\widehat{M}_t
=
M_\tau^{exchange}
\cdot
\frac{
M_{model}(S_t,K,T-t,\sigma_\tau)
}{
M_{model}(S_\tau,K,T-\tau,\sigma_\tau)
}
$$

Ưu điểm:

- Anchor đúng exchange mark.
- Model chỉ quyết định relative movement.
- Giảm absolute model bias.
- Không cần volatility surface.

## 26.3 Source

```text
mark_source:
  exchange_mark_at_trade
  anchored_iv_reconstruction
  unavailable
  expired
```

## 26.4 Không default IV

Nếu chưa có IV anchor:

```text
mark = null
quality_flag = NO_IV_ANCHOR
```

Không dùng 50% fallback.

## 26.5 Expiry

Sau expiry:

- Không epsilon-pricing.
- Không forward-fill.
- Mark source `expired`.
- Settlement thuộc portfolio/backtest settlement adapter.

---

# 27. Global BTC index từ historical option trades

Không forward-fill index riêng theo instrument.

## 27.1 Global index events

Từ tất cả retained option trade observations:

```text
timestamp
index_price
```

Nếu nhiều trades cùng millisecond:

```text
median index_price
```

## 27.2 5m index grid

Lưu/derive:

```text
index_open
index_high
index_low
index_close
last_index_timestamp
index_age_seconds
```

## 27.3 TTL

Nếu không có option trade toàn market quá lâu:

- Carry-forward có age.
- Nếu age vượt global TTL, reconstruction unavailable.

V1 default:

```yaml
index_max_age_minutes: 30
```

Pilot có thể điều chỉnh.

## 27.4 Limitation

Index derived từ option trades không phải official continuous index history.

Provenance:

```text
index_source = option_trade_observation_v1
```

V2 có thể bổ sung official index history hoặc futures data.

---

# 28. Greeks và pricing provenance

## 28.1 Persist tối thiểu

Permanent 5m tape chỉ persist:

```text
model_delta
```

vì delta cần cho universe selection và QuantBT selectors.

## 28.2 Tính on load

```text
model_gamma
model_vega
model_theta
```

được tính khi loader/adapter cần.

## 28.3 Pricing adapter

Interface:

```python
class OptionPricingAdapter(Protocol):
    version: str

    def price_and_greeks(
        self,
        *,
        index_price: float,
        strike: float,
        ttm_years: float,
        iv: float,
        option_type: int,
    ) -> PricingResult:
        ...
```

V1:

```text
version = anchored_iv_v1
forward_source = index_proxy_v1
```

## 28.4 Units

Schema metadata phải ghi:

```text
premium currency = BTC
index/strike currency = USD
iv unit = percent
delta convention = model-specific
theta unit = per day
vega unit = per 1 vol point
```

---

# 29. Candidate tape và held-position overlay

Permanent tape cap 64 không thể bảo đảm mọi contract một strategy từng chọn luôn nằm trong candidate universe đến expiry.

Giải pháp tách hai lớp.

## 29.1 Candidate tape

Permanent 5m dataset:

- Dùng cho contract discovery/entry.
- Cap 64.
- Liquidity-focused.
- `entry_eligible`.

## 29.2 Held-position overlay

Khi strategy mở contract không còn xuất hiện trong candidate tape ở các bar sau:

- Loader reconstruct state riêng từ canonical trade events.
- Pin instrument cho holding interval.
- Merge vào QuantBT tape.

Interface:

```python
overlay = load_held_instrument_state(
    instrument_ids=[...],
    start=...,
    end=...,
    resolution="5m",
)
```

## 29.3 Lợi ích

- Permanent tape nhỏ.
- Không làm held position biến mất.
- Không cần retain mọi historical candidate đến expiry.
- Strategy-specific state chỉ được dựng khi thật sự dùng.

## 29.4 Cache

Held overlay cache:

```text
storage/_cache/deribit_options/held_overlay/
```

TTL/LRU, không permanent canonical.

---

# 30. Snapshot 1m on-demand

## 30.1 Không persistent full history

```yaml
persistent_full_history: false
```

## 30.2 Profile

```yaml
max_dte_days: 2
min_abs_delta: 0.30
max_abs_delta: 0.70
atm_strikes_each_side: 2
max_expiries: 2
max_rows_per_timestamp: 20
require_trade_within_minutes: 30
```

## 30.3 Use cases

- 0DTE.
- ATM straddle.
- Near-expiry gamma.
- Gamma scalping.

## 30.4 Cache lifecycle

```yaml
cache:
  max_size_mib: 512
  ttl_days: 30
  delete_after_experiment: true
  lru_cleanup: true
```

1m cache không tính vào permanent dataset budget.

---

# 31. Execution proxy từ trade và mark

Không có historical bid/ask.

V1 dùng empirical taker execution residual.

## 31.1 Relative cost

Taker buy:

$$
c_i^{buy}
=
\frac{P_i-M_i}{M_i}
$$

Taker sell:

$$
c_i^{sell}
=
\frac{M_i-P_i}{M_i}
$$

## 31.2 Absolute cost

$$
a_i^{BTC}=|P_i-M_i|
$$

$$
a_i^{USD}=a_i^{BTC}S_i
$$

Relative cost với tiny premium có thể cực lớn, nên proxy kết hợp relative và absolute caps.

## 31.3 Buckets

```text
side
DTE bucket
absolute-delta bucket
premium bucket
activity bucket
trade-size bucket
```

## 31.4 Statistics

```text
sample_count
median_cost
p60_cost
p75_cost
p90_cost
p95_cost
median_trade_amount
p25_trade_amount
fit_start
fit_end
```

## 31.5 Synthetic fill

Buy:

$$
P_{fill}^{buy}
=
\widehat{M}_t(1+q^{buy})
$$

Sell:

$$
P_{fill}^{sell}
=
\widehat{M}_t(1-q^{sell})
$$

## 31.6 Size impact

$$
q_{size}
=
q_{bucket}
\sqrt{
\max\left(
1,
\frac{Q_{order}}{Q_{median}}
\right)
}
$$

Có caps theo premium/tick.

## 31.7 Proxy size

```text
proxy_top_size = p25 regular trade amount
```

hoặc conservative combination:

```text
min(
  p25_trade_amount,
  rolling_5m_volume × capacity_fraction
)
```

## 31.8 Không persist bid/ask trên từng snapshot

Lưu execution table riêng.

Adapter tạo proxy bid/ask on load.

---

# 32. Block, combo và liquidation trades

## 32.1 Hot classification

Flags:

```text
IS_BLOCK
IS_COMBO
IS_LIQUIDATION
IS_REGULAR
```

## 32.2 Canonical event policy

Nếu trade qua broad retention:

- Có thể giữ row.
- Không giữ full rare IDs.
- Giữ flags.

## 32.3 Activity/execution policy

Mặc định:

```yaml
exclude_block: true
exclude_combo: true
exclude_liquidation: true
```

khỏi:

- Liquidity activity.
- Normal execution calibration.

## 32.4 V2

Combo execution nghiên cứu riêng bằng expanded schema/version.

---

# 33. Look-ahead prevention

## 33.1 Bar timing

5m bar chứa events:

```text
(t-5m, t]
```

Signal tại close $t$:

```text
earliest fill > t
```

Default fill:

```text
next bar/open event
```

## 33.2 Same timestamp

```yaml
same_timestamp_fill_allowed: false
```

## 33.3 Execution model

Execution statistics dùng:

```text
observation_timestamp < simulated_order_timestamp
```

Không fit toàn bộ history rồi áp ngược.

## 33.4 Rolling fit

Default:

```text
lookback = 90 days
refit frequency = daily
minimum samples = 100
```

Fallback hierarchy:

```text
exact bucket
→ wider delta bucket
→ DTE + side
→ global side
→ execution quality LOW
```

---

# 34. Storage layout trong trading-historical-data

```text
storage/
├── _staging/
│   └── options/
│       └── deribit/
│           └── version=v1/
│               └── currency=BTC/
│                   └── shard=00/
│
├── _tmp/
│   ├── duckdb/
│   └── compaction/
│
├── _cache/
│   └── deribit_options/
│       ├── snapshot_1m/
│       └── held_overlay/
│
└── options/
    └── deribit/
        ├── instruments/
        │   └── version=v1/
        │       └── instruments.parquet
        │
        ├── trades/
        │   └── version=v1/
        │       └── currency=BTC/
        │           └── year=YYYY/
        │               └── month=MM/
        │                   └── day=DD/
        │
        ├── snapshot_5m/
        │   └── version=v1/
        │       └── currency=BTC/
        │           └── year=YYYY/
        │               └── month=MM/
        │                   └── day=DD/
        │
        ├── execution_proxy/
        │   └── version=v1/
        │
        └── manifests/
            └── version=v1/
```

State:

```text
state/
└── deribit_options/
    └── version=v1/
        ├── BTC.sqlite
        ├── validation_summary.json
        ├── unresolved_ranges.parquet
        ├── storage_report.json
        └── locks/
```

---

# 35. Cấu trúc module migration

```text
collectors/
├── deribit_option_trades.py
│
└── deribit/
    ├── __init__.py
    ├── config.py
    ├── client.py
    ├── rate_limit.py
    ├── instruments.py
    ├── tasks.py
    ├── engine.py
    ├── checkpoints.py
    ├── coverage.py
    ├── schema.py
    ├── normalize.py
    ├── filters.py
    ├── staging.py
    ├── parquet_parts.py
    ├── compact.py
    ├── validate.py
    ├── repair.py
    ├── snapshot_5m.py
    ├── snapshot_1m.py
    ├── pricing.py
    ├── execution_proxy.py
    ├── cleanup.py
    └── metrics.py
```

Loader:

```text
loaders/
└── deribit_options.py
```

Nếu repo chưa có package `loaders/`, có thể giữ adapter trong `data_loader.py` nhưng core logic nên tách module.

---

# 36. Các storage primitive cần bổ sung

## 36.1 `ImmutableParquetPartWriter`

Responsibilities:

- Không read existing data.
- Write temp.
- Atomic rename.
- Return checksum/metadata.
- Enforce max rows/file.
- Enforce schema.

## 36.2 `DailyPartitionCompactor`

Responsibilities:

- Discover staging parts.
- External-memory dedup.
- Partition by trade date.
- Sort.
- Atomic publish.
- Validation hooks.
- Cleanup candidate list.

## 36.3 `DiskBudgetGuard`

Responsibilities:

- Measure canonical bytes.
- Predict output bytes.
- Warning/hard stop.
- Trigger cleanup.
- Write storage report.

## 36.4 `CacheManager`

Responsibilities:

- TTL.
- LRU.
- Size cap.
- Delete experiment caches.
- Never delete canonical artifacts.

## 36.5 Không thay global store hiện tại

Storage primitive hiện tại vẫn dùng cho collectors nhỏ.

Deribit subsystem dùng append-only primitives riêng.

---

# 37. Data loader và QuantBT adapter

## 37.1 Public loader

```python
load_data(
    data_type="deribit_option_trades",
    start_date=...,
    end_date=...,
    currency="BTC",
    instruments=None,
    option_type=None,
    dte_min=None,
    dte_max=None,
    columns=None,
    version="v1",
)
```

## 37.2 Candidate snapshots

```python
load_deribit_option_snapshots(
    start=...,
    end=...,
    resolution="5m",
    version="v1",
    entry_eligible_only=False,
)
```

## 37.3 On-demand held overlay

```python
load_deribit_option_overlay(
    instrument_ids=[...],
    start=...,
    end=...,
    resolution="5m",
    pricing_version="anchored_iv_v1",
)
```

## 37.4 DuckDB query

Use hive partition pruning:

```sql
SELECT ...
FROM read_parquet(
  'storage/options/deribit/snapshot_5m/version=v1/**/*.parquet',
  hive_partitioning=true
)
WHERE timestamp_ms >= ?
  AND timestamp_ms < ?;
```

## 37.5 QuantBT mapping

Observed:

```text
last_trade_price
exchange_mark_at_anchor
IV
volume
trade count
```

Reconstructed:

```text
mark_price
delta
```

Proxy:

```text
bid_price
ask_price
bid_size
ask_size
```

Provenance fields:

```text
mark_source
execution_source
forward_source
quality_flags
```

## 37.6 Ragged tape

Adapter output phải giữ long-form rows và compile theo QuantBT CSR/ragged representation.

Không pivot thành full dense chain.

---

# 38. CLI và chế độ vận hành

```bash
# API smoke test
python -m collectors.deribit_option_trades probe \
  --config configs/deribit_historical_v1.yml

# Discover instruments
python -m collectors.deribit_option_trades discover \
  --version v1

# Full historical backfill
python -m collectors.deribit_option_trades backfill \
  --version v1

# Incremental historical update
python -m collectors.deribit_option_trades sync-once \
  --version v1

# Compact staging
python -m collectors.deribit_option_trades compact \
  --version v1

# Validate
python -m collectors.deribit_option_trades validate \
  --version v1

# Repair unresolved ranges
python -m collectors.deribit_option_trades repair \
  --version v1 \
  --only-unresolved

# Build permanent 5m snapshots
python -m collectors.deribit_option_trades build-snapshot-5m \
  --version v1

# Build temporary 1m cache
python -m collectors.deribit_option_trades build-snapshot-1m \
  --start 2025-01-01 \
  --end 2025-03-01

# Fit execution tables
python -m collectors.deribit_option_trades fit-execution \
  --version v1

# Cleanup
python -m collectors.deribit_option_trades cleanup \
  --version v1

# Storage report
python -m collectors.deribit_option_trades storage-report \
  --version v1
```

---

# 39. Docker và lịch incremental sync

## 39.1 Backfill

Run manually:

```yaml
deribit-option-backfill:
  build: .
  command:
    - python
    - -m
    - collectors.deribit_option_trades
    - backfill
    - --version
    - v1
  volumes:
    - ./storage:/app/storage
    - ./state:/app/state
    - ./configs:/app/configs:ro
    - ./logs:/app/logs
```

## 39.2 Incremental

Ưu tiên `sync-once` bằng cron/systemd timer:

```text
mỗi 6 giờ hoặc mỗi ngày
```

Historical sync không cần process sống liên tục.

## 39.3 Suggested daily workflow

```text
01:00 UTC discover + sync-once
02:00 UTC compact
02:30 UTC validate/repair
03:00 UTC build latest 5m partitions
03:30 UTC fit/update execution stats
04:00 UTC cleanup + storage report
```

---

# 40. Dependencies

Đề xuất:

```text
httpx
aiolimiter
aiosqlite
tenacity
orjson
pyarrow
duckdb
pandas
pyyaml
```

Polars không bắt buộc trong V1.

V1 ưu tiên dùng stack đã gần với repo:

- PyArrow.
- DuckDB.
- Pandas chỉ cho small result/control flows.

---

# 41. Cấu hình YAML chính thức V1

```yaml
dataset:
  name: deribit_btc_options
  version: v1
  universe_version: compact_liquid_v1
  schema_version: trade_schema_v1
  snapshot_version: compact_5m_v1
  pricing_version: anchored_iv_v1
  execution_proxy_version: trade_mark_v1

scope:
  currency: BTC
  kind: option
  historical_trades_only: true
  streaming: false
  orderbook: false

api:
  base_url: https://history.deribit.com/api/v2/public
  instruments_method: get_instruments
  trades_method: get_last_trades_by_instrument

  chunk_size: 5000
  max_supported_chunk_size: 10000

  timeout_seconds: 60
  connect_timeout_seconds: 10

  target_requests_per_second: 12
  max_requests_per_second: 18

  retry:
    max_attempts: 10
    prefer_retry_after: true
    exponential_backoff: true
    min_delay_seconds: 1
    max_delay_seconds: 60
    jitter: true

runtime:
  api_workers: 4
  task_queue_size: 16
  write_queue_size: 8
  writer_workers: 1
  max_inflight_chunks: 8

memory:
  ingestion_target_rss_mb: 500
  ingestion_hard_rss_mb: 750
  compactor_memory_limit_mb: 1024
  snapshot_target_rss_mb: 1000
  snapshot_hard_rss_mb: 1400

checkpoint:
  backend: sqlite
  path: state/deribit_options/version=v1/BTC.sqlite
  journal_mode: WAL
  synchronous: NORMAL
  disk_before_checkpoint: true

instrument_discovery:
  include_active: true
  include_expired: true
  refresh_before_sync: true
  parse_name_fallback: true

broad_ingestion:
  max_dte_days: 120

  moneyness:
    min_strike_to_index: 0.50
    max_strike_to_index: 2.00

  emergency_moneyness:
    enabled: false
    min_strike_to_index: 0.40
    max_strike_to_index: 2.50
    min_regular_trades: 2

  require:
    valid_expiry: true
    positive_index: true
    nonnegative_price: true
    nonnegative_mark: true
    positive_iv: true
    positive_amount: true

  activation:
    stateful: true
    retain_subsequent_trades_until_expiry: true

staging:
  format: parquet
  jsonl_enabled: false
  root: storage/_staging/options/deribit/version=v1
  shard_count: 64
  atomic_rename: true
  checksum: xxh64

canonical_trades:
  root: storage/options/deribit/trades/version=v1
  partition_by:
    - currency
    - year
    - month
    - day

  compression: zstd
  compression_level: 6
  target_file_size_mb: 192
  static_fields_in_dimension: true
  keep_full_trade_id: false
  keep_trade_id_hash: true
  keep_rare_identifiers: false

compaction:
  engine: duckdb
  memory_limit_mb: 1024
  temp_directory: storage/_tmp/duckdb
  dedup_key:
    - instrument_id
    - trade_seq
  sort_by:
    - timestamp_ms
    - instrument_id
    - trade_seq

snapshot_5m:
  persistent: true
  resolution: 5m
  root: storage/options/deribit/snapshot_5m/version=v1

  max_rows_per_timestamp: 64
  expiry_rebalance: 1d

  target_dte_days:
    - 0
    - 7
    - 14
    - 30
    - 60
    - 90

  far_calendar:
    enabled: true
    target_dte: 120
    max_expiries: 1

  max_core_expiries: 6
  max_total_expiries: 7

  tiers:
    - dte_min: 0
      dte_max: 14
      min_abs_delta: 0.12
      max_abs_delta: 0.88
      atm_strikes_each_side: 3
      max_contracts_per_expiry: 16
      include_15_delta_if_active: true
      activity_window_hours: 6
      min_regular_trades: 2
      min_volume_btc: 1.0
      anchor_ttl_hours: 2

    - dte_min: 14
      dte_max: 45
      min_abs_delta: 0.15
      max_abs_delta: 0.85
      atm_strikes_each_side: 2
      max_contracts_per_expiry: 12
      include_25_delta_if_active: true
      activity_window_hours: 12
      min_regular_trades: 2
      min_volume_btc: 1.0
      anchor_ttl_hours: 4

    - dte_min: 45
      dte_max: 90
      min_abs_delta: 0.20
      max_abs_delta: 0.80
      atm_strikes_each_side: 2
      max_contracts_per_expiry: 10
      activity_window_hours: 24
      min_regular_trades: 2
      anchor_ttl_hours: 8

    - dte_min: 90
      dte_max: 120
      min_abs_delta: 0.40
      max_abs_delta: 0.60
      atm_strikes_each_side: 1
      max_contracts_per_expiry: 6
      activity_window_hours: 72
      min_regular_trades: 2
      anchor_ttl_hours: 24

  columns:
    persist_full_greeks: false
    persist_execution_proxy: false
    persist_model_delta: true
    static_fields_in_dimension: true

snapshot_1m:
  persistent_full_history: false
  build_on_demand: true
  cache_root: storage/_cache/deribit_options/snapshot_1m

  max_dte_days: 2
  min_abs_delta: 0.30
  max_abs_delta: 0.70
  atm_strikes_each_side: 2
  max_expiries: 2
  max_rows_per_timestamp: 20
  anchor_ttl_minutes: 30

  cache:
    max_size_mib: 512
    ttl_days: 30
    lru: true
    delete_after_experiment: true

pricing:
  method: anchored_relative_iv
  default_iv: null
  index_source: option_trade_observation_v1
  index_max_age_minutes: 30
  persist_delta_only: true
  calculate_other_greeks_on_load: true
  prohibit_after_expiry: true

execution_proxy:
  source: trade_vs_exchange_mark

  exclude:
    block: true
    combo: true
    liquidation: true

  buckets:
    - side
    - dte
    - abs_delta
    - premium_btc
    - activity
    - trade_amount

  quantiles:
    - 0.50
    - 0.60
    - 0.75
    - 0.90
    - 0.95

  default_quantile: 0.75
  min_bucket_samples: 100
  rolling_lookback_days: 90
  refit_frequency: 1d
  prevent_future_leakage: true

held_overlay:
  enabled: true
  build_on_demand: true
  cache_root: storage/_cache/deribit_options/held_overlay
  cache_max_mib: 512
  cache_ttl_days: 30

disk_budget:
  canonical_target_gib: 8.0
  warning_gib: 8.5
  hard_stop_gib: 9.0
  post_cleanup_filesystem_limit_gib: 10.0

cleanup:
  delete_staging_after_validation: true
  delete_jsonl_after_validation: true
  delete_compaction_temp: true
  delete_orphan_tmp: true
  delete_experiment_cache_after_run: true
  cache_ttl_cleanup: true
  rotate_logs_days: 14
  vacuum_sqlite: true

validation:
  verify_coverage_ranges: true
  verify_output_checksums: true
  verify_dedup: true
  verify_dimension_fk: true
  quarantine_conflicts: true
  auto_repair_ranges: true

monitoring:
  log_api_requests: true
  log_retry_count: true
  log_response_rows: true
  log_retained_rows: true
  log_discarded_rows: true
  log_bytes_written: true
  log_peak_rss: true
  log_disk_budget: true
  log_snapshot_rows: true
```

---

# 42. Disk budget và capacity planning

## 42.1 Permanent budget

| Thành phần | Low | High |
|---|---:|---:|
| Canonical filtered trade events | 1.5 GiB | 2.5 GiB |
| Permanent 5m candidate tape | 3.4 GiB | 4.8 GiB |
| Instrument dimension | 0.03 GiB | 0.10 GiB |
| SQLite + manifests + reports | 0.10 GiB | 0.35 GiB |
| Execution tables | 0.10 GiB | 0.30 GiB |
| Logs/filesystem overhead | 0.20 GiB | 0.50 GiB |
| **Total** | **5.33 GiB** | **8.55 GiB** |

Target:

```text
6–9 GiB permanent
```

Hard requirement:

```text
<=10 GiB after cleanup
```

## 42.2 Peak build disk

Trong compaction có thể tồn tại:

- Staging parts.
- Canonical old partition.
- Canonical new temp partition.
- DuckDB spill.
- Reports.

Khuyến nghị:

```text
15–20 GiB free khi initial build
```

Nếu filesystem chỉ có đúng 10 GiB total, safe atomic compaction toàn history không khả thi.

## 42.3 Không giữ JSONL

Default:

```text
jsonl_enabled = false
```

Nếu debug bật JSONL:

- Chỉ giữ ngắn hạn.
- Xóa sau validation.
- Không tính là canonical.

## 42.4 Daily growth

Vì V1 curated:

Planning:

```text
trade events: khoảng 1–8 MiB/ngày bình thường
5m snapshots: khoảng 1–4 MiB/ngày
metadata/stats: rất nhỏ
```

High-volatility:

```text
khoảng 5–20 MiB/ngày tổng
```

Phải đo sau pilot.

## 42.5 Long-term 10 GiB conflict

Giữ toàn bộ lịch sử và hard cap 10 GiB mãi mãi cuối cùng có thể mâu thuẫn.

Do đó V1 có disk-pressure fallback policy.

---

# 43. Cleanup transaction

Staging chỉ được xóa khi:

```text
1. Canonical output tồn tại.
2. Atomic publish hoàn tất.
3. Row counts hợp lệ.
4. Dedup pass.
5. Foreign-key/dimension pass.
6. Coverage ledger pass.
7. Checksums pass.
8. Manifest đã commit.
9. Validation report status = PASS.
```

Sau đó:

```text
delete staging files thuộc partition
delete temporary compaction files
delete optional JSONL
delete orphan .tmp
VACUUM SQLite nếu đạt threshold
rotate logs
refresh storage report
```

## 43.1 Không xóa khi fail

Nếu validation fail:

- Giữ staging.
- Mark partition quarantined.
- Không publish/replace canonical.
- Emit repair plan.

## 43.2 Cleanup idempotent

Chạy nhiều lần không được:

- Xóa canonical.
- Xóa active staging.
- Xóa unresolved repair evidence.

---

# 44. Disk-pressure fallback policy

## 44.1 Warning tại 8.5 GiB

Actions:

1. Full cache cleanup.
2. Delete orphan staging.
3. Compact small files.
4. VACUUM SQLite.
5. Recompute storage forecast.

## 44.2 Hard stop tại 9.0 GiB

Không publish thêm permanent snapshot partition nếu projected total > 9 GiB.

Trade-event ingestion có thể:

- Tạm dừng toàn pipeline.
- Hoặc tiếp tục only if budget policy cho phép.

Default an toàn:

```text
pause publish and require policy action
```

## 44.3 Fallback order

1. Giảm `max_rows_per_timestamp`: 64 → 56.
2. Tắt persistent 91–120 DTE snapshots.
3. Giảm far-calendar contracts từ 6 → 4.
4. Giảm 0–14 cap từ 16 → 14.
5. Chuyển old snapshot history sang 15m.
6. Chỉ giữ event data cho earliest low-activity years.

## 44.4 Resolution tier fallback

Ví dụ:

```yaml
resolution_tiering:
  before_2020:
    snapshot: event_only
  2020_to_2021:
    snapshot: 15m
  from_2022:
    snapshot: 5m
```

Không dùng mặc định trước pilot.

---

# 45. Ước tính API calls, RAM và thời gian

## 45.1 API calls

Full Deribit instrument count rất lớn.

Với lazy sequence scheduling:

- Ít nhất một request/probe per instrument.
- Instruments có trades cần additional chunks.
- Tổng request có thể ở mức hàng trăm nghìn tùy API behavior và số instruments.

Planning:

```text
~115.000–150.000 requests initial run
```

Không phải 2.000 requests chỉ vì chunk size 10.000; empty/tiny instruments vẫn cần probe.

## 45.2 Time

At effective 10–15 req/s:

```text
~2–4 giờ API phase
```

Có thể nhanh/chậm hơn tùy:

- Rate limit.
- Proxy/network.
- Retries.
- Empty instruments.
- Disk speed.

## 45.3 RAM

Với 4 workers × 5.000 trades/chunk:

Target:

```text
<750 MiB ingestion RSS
```

Compactor:

```text
DuckDB memory limit 1 GiB + disk spill
```

Snapshot builder:

```text
process one day + terminal state
<1.4 GiB hard target
```

---

# 46. Metrics và observability

## 46.1 Ingestion

```text
requests_total
requests_success
requests_429
requests_retry
requests_failed
response_rows
retained_rows
discarded_rows
empty_instruments
unknown_instruments
active_instruments
expired_instruments
task_queue_size
write_queue_size
api_latency_ms
write_latency_ms
peak_rss_mb
```

## 46.2 Storage

```text
staging_bytes
canonical_trade_bytes
snapshot_5m_bytes
cache_bytes
sqlite_bytes
temp_bytes
total_permanent_bytes
disk_budget_ratio
bytes_per_retained_trade
bytes_per_snapshot_row
```

## 46.3 Quality

```text
duplicate_rows
conflicting_rows
unresolved_ranges
missing_dimension_rows
invalid_price_rows
invalid_iv_rows
snapshot_unavailable_rows
stale_anchor_rows
entry_eligible_ratio
```

## 46.4 Snapshot universe

```text
rows_per_timestamp_mean
rows_per_timestamp_p95
rows_per_timestamp_max
expiries_per_timestamp
contracts_per_expiry
activity_rejection_rate
delta_coverage
strategy_package_coverage
```

---

# 47. Test plan

## 47.1 Client

- History base URL.
- Count limits.
- Descending trades.
- `has_more`.
- Boundary overlap.
- Retry-After.
- Timeout.
- Invalid JSON.
- JSON-RPC error.
- Empty vs unknown.

## 47.2 Scheduler

- Lazy next chunk.
- No pre-allocation explosion.
- Queue backpressure.
- Graceful shutdown.
- Dead-letter.
- Active instrument caught-up.
- Expired complete.

## 47.3 Checkpoint

Crash simulations:

```text
write success → crash before checkpoint
```

Expected: duplicate refetch, no loss.

```text
checkpoint must never advance before durable file
```

## 47.4 Activation/filter

- Pre-activation trade discarded.
- Activation trade retained.
- Post-activation out-of-band trades retained.
- Expiry stops retention.
- Invalid IV/index rejected.
- Emergency envelope optional.

## 47.5 Compaction

- Cross-file duplicate.
- Boundary overlap.
- Conflict quarantine.
- External-memory behavior.
- Idempotent rerun.
- Atomic publish.
- Partition pruning.

## 47.6 Snapshot

- DTE tier selection.
- ATM cluster.
- Delta target.
- Calendar far expiry.
- Activity filter.
- TTL.
- Hard cap.
- Package completeness.
- No post-expiry rows.

## 47.7 Reconstruction

- Anchor equality.
- No IV anchor.
- Stale index.
- No negative prices.
- No forward-fill after expiry.
- Provenance flags.

## 47.8 Execution

- Buy/sell sign.
- Tiny premium caps.
- No future leakage.
- Bucket fallback.
- Exclude block/combo/liquidation.

## 47.9 Loader

- Date pruning.
- Column projection.
- DTE/option-type filters.
- Candidate tape.
- Held overlay.
- Ragged output.
- Cache cleanup.

## 47.10 Disk budget

- Warning.
- Hard stop.
- Cleanup.
- No canonical deletion.
- Projected output prevention.

---

# 48. Pilot benchmark bắt buộc

Không chạy full history trước khi pilot.

## 48.1 Three regimes

Chọn ba cửa sổ 30 ngày:

- Low volatility.
- Normal.
- High volatility.

## 48.2 Measure

```text
instrument requests
response trades
retained trades
retention ratio
bytes/trade
snapshot rows
bytes/snapshot row
contracts per expiry
package coverage
peak RSS
API throughput
write throughput
compaction throughput
execution bucket samples
```

## 48.3 Freeze parameters

Sau pilot mới freeze:

- Moneyness envelope.
- Activity thresholds.
- Anchor TTL.
- Cap 64 hay 56.
- ZSTD level.
- Chunk size 5.000 hay 10.000.
- Expected full-history size.

## 48.4 Pilot acceptance

```yaml
pilot_acceptance:
  ingestion_peak_rss_mb: 750
  snapshot_peak_rss_mb: 1400
  no_unbounded_queue: true
  permanent_size_projection_gib: 9.0
  unresolved_coverage_ranges: 0
  duplicate_conflicts: 0
  strategy_package_coverage_pct: 95
```

`strategy_package_coverage_pct` đo tỷ lệ timestamps mà required package legs có thể được dựng trong eligible universe, không phải tỷ lệ mọi contract.

---

# 49. Implementation phases

## Phase 0 — Freeze interfaces

Deliverables:

- Config schema.
- Directory layout.
- SQLite schema.
- Canonical schemas.
- Version IDs.
- CLI contracts.

## Phase 1 — API probe

- Validate history API.
- Validate count 5.000/10.000.
- Validate sorting.
- Validate overlaps.
- Validate active/expired discovery.
- Produce probe report.

## Phase 2 — Instrument discovery + checkpoint

- Dimension table.
- SQLite state.
- Resume.
- Empty/unknown states.

## Phase 3 — Disk-first downloader

- Async client.
- Limiter.
- Lazy tasks.
- Immutable Parquet part writer.
- Coverage commit.
- Graceful stop.

## Phase 4 — Compactor + canonical events

- Daily partitioning.
- Dedup.
- Dimension encoding.
- Rare field dropping.
- Validation.
- Cleanup transaction.

## Phase 5 — Pilot

- Three regimes.
- Storage/RAM metrics.
- Tune universe.
- Freeze V1 config.

## Phase 6 — Full historical backfill

- Run downloader.
- Incremental compaction.
- Validation/repair.
- Cleanup.
- Storage report.

## Phase 7 — 5m candidate tape

- Global index.
- Pricing adapter.
- Activity score.
- Expiry/strike selection.
- Hard cap.
- Daily stateful build.

## Phase 8 — QuantBT adapter

- Candidate ragged tape.
- Model Greeks on load.
- Execution proxy.
- Held overlay.

## Phase 9 — Optional 1m

- Near-ATM profile.
- Cache manager.
- Experiment cleanup.

## Phase 10 — Operations

- Scheduled sync.
- Daily compaction.
- Metrics.
- Disk budget enforcement.
- Documentation.

---

# 50. Acceptance criteria

V1 chỉ hoàn thành khi:

## Ingestion

- Resume idempotent.
- Disk-first ordering verified.
- No unknown completed instrument.
- No unresolved coverage ranges.
- No unbounded memory behavior.

## Canonical events

- No duplicate `(instrument_id, trade_seq)`.
- Dimension FK valid.
- Static fields not repeated.
- Partitioned by trade date.
- Staging deleted only after validation.

## Snapshots

- 5m candidate tape cap ≤64.
- At most 7 expiries.
- No post-expiry rows.
- Provenance flags present.
- No default IV.
- 1m not permanent full history.

## QuantBT

- Can build every target package type in tests.
- Candidate tape loads as ragged representation.
- Held contract does not disappear due to candidate cap.
- Proxy fields clearly marked non-observed.

## Resources

- Initial full build does not OOM.
- Permanent storage ≤10 GiB after cleanup.
- Target ≤9 GiB under measured dataset date.
- Cache cleanup works.
- Peak disk documented.

---

# 51. Các invariant bắt buộc

## 51.1 Ingestion

```text
No checkpoint before durable file.
```

```text
No error interpreted as confirmed empty.
```

```text
No timestamp pagination as primary method.
```

```text
No unbounded task/write queues.
```

## 51.2 Storage

```text
No full-history Pandas concat.
```

```text
No rewrite of full partition for every API chunk.
```

```text
No permanent JSONL by default.
```

```text
No staging deletion before full validation.
```

## 51.3 Semantics

```text
Exchange mark is never overwritten by reconstructed mark.
```

```text
Observed, reconstructed and proxy fields are distinguishable.
```

```text
No default IV without source.
```

```text
No independent per-instrument BTC index forward-fill.
```

```text
No pricing after expiry.
```

## 51.4 Universe

```text
Permanent 5m candidate tape <=64 rows/timestamp.
```

```text
Permanent 1m full-history dataset is forbidden in V1.
```

```text
Held positions use overlay rather than relying solely on candidate membership.
```

## 51.5 Backtest

```text
No fill from event before signal.
```

```text
No future data in execution calibration.
```

```text
Block/combo/liquidation excluded from default regular execution model.
```

## 51.6 Disk

```text
Post-cleanup permanent storage <=10 GiB.
```

```text
Publish stops when projected canonical storage exceeds hard budget.
```

---

# 52. Các giới hạn đã chấp nhận của V1

V1 không bảo đảm:

- Exact historical bid/ask.
- Exact quote size.
- Exact order-book depth.
- Exact queue fill.
- Exact exchange Greeks giữa trades.
- Exact forward curve.
- Deep-tail option coverage.
- Full long-dated calendar coverage.
- Perfect 0DTE 1m history.
- Perfect valuation khi anchor quá stale.
- Exact settlement nếu chưa bổ sung delivery-price source.

V1 phù hợp với:

- Research/backtest phổ biến.
- Relative comparison giữa strategy variants.
- Conservative execution approximation.
- Liquidity-focused option packages.
- QuantBT engine validation.

V1 không nên được mô tả là:

```text
historical order-book replay
```

hoặc:

```text
execution-grade tick simulator
```

---

# 53. Lộ trình V2 và các phiên bản sau

## V1.1

- Tune activity thresholds.
- Better disk packing.
- Strategy coverage reports.
- Incremental snapshot rebuild.

## V2

- Wider DTE 180.
- 5/10-delta wings.
- ETH options.
- Historical BTC futures basis.
- Better forward curve.
- Better IV interpolation.
- Store delivery prices.
- Better settlement.
- Optional full trade ID archive.

## V3

- Merge historical stream-recorded L1.
- Real bid/ask for newer dates.
- Execution model transition:
  - old dates: proxy.
  - new dates: observed L1.
- Paper/live adapter convergence.

## Version coexistence

```text
v1 = compact liquid trades-only
v2 = extended universe
v3 = hybrid historical trades + recorded quotes
```

---

# 54. Kết luận

Kế hoạch V1 chính thức là:

```text
Deribit historical instruments
        ↓
lazy trade_seq downloader
        ↓
bounded API and write queues
        ↓
filter broad relevant events
        ↓
direct immutable Parquet staging
        ↓
coverage ledger + SQLite checkpoint
        ↓
daily external-memory compaction
        ↓
canonical compact trade events
        ↓
5m candidate tape:
    ≤7 expiries
    ≤64 contracts/timestamp
        ↓
QuantBT ragged adapter
        +
held-position on-demand overlay
        +
execution proxy from trade-vs-mark
```

Các quyết định trọng tâm:

1. Học `trade_seq`, lazy scheduling, SQLite và disk-before-checkpoint từ repository RiveChen.
2. Không bê nguyên JSONL-per-instrument và giant Parquet.
3. Không lưu full Deribit archive.
4. Broad ingestion tối đa 120 DTE, moneyness 0.50–2.00, stateful activation.
5. Permanent 5m candidate tape chỉ giữ 0/7/14/30/60/90 DTE và optional 120D calendar.
6. DTE càng xa, delta/strike universe càng hẹp.
7. Hard cap 64 rows/timestamp.
8. Static metadata tách khỏi fact tables.
9. Full Greeks và proxy bid/ask tính on load.
10. 1m chỉ on-demand cho DTE ≤2 gần ATM.
11. Held contracts được phục vụ qua overlay, không ép permanent tape phải giữ mọi contract.
12. Temporary/staging/JSONL được xóa transactionally sau validation.
13. Permanent storage target 6–9 GiB và không vượt 10 GiB.
14. V1 độc lập, có thể mở rộng sạch sang V2 mà không phá dữ liệu cũ.

---

# 55. Nguồn tham khảo

## Repository tham khảo

- RiveChen — Deribit Historical Data
  <https://github.com/RiveChen/deribit-historical-data>

- Design Decisions
  <https://github.com/RiveChen/deribit-historical-data/blob/main/docs/design-decisions.md>

- Deribit Historical API Notes
  <https://github.com/RiveChen/deribit-historical-data/blob/main/docs/deribit-api.md>

- Fetcher Engine
  <https://github.com/RiveChen/deribit-historical-data/blob/main/src/deribit_fetcher/engine.py>

- SQLite Progress Repository
  <https://github.com/RiveChen/deribit-historical-data/blob/main/src/deribit_fetcher/progress.py>

## Repository đích

- BobbyAxerol — trading-historical-data
  <https://github.com/BobbyAxerol/trading-historical-data>

- Common storage implementation
  <https://github.com/BobbyAxerol/trading-historical-data/blob/main/collectors/common/storage.py>

- Data loader
  <https://github.com/BobbyAxerol/trading-historical-data/blob/main/data_loader.py>

- Existing options config
  <https://github.com/BobbyAxerol/trading-historical-data/blob/main/configs/options.yml>

## QuantBT Option Engine

- QuantBT branch `feat/option-engine`
  <https://github.com/BobbyAxerol/quantbt/tree/feat/option-engine>

- Option package templates
  <https://github.com/BobbyAxerol/quantbt/blob/feat/option-engine/options/templates/packages.py>

- Contract selectors
  <https://github.com/BobbyAxerol/quantbt/blob/feat/option-engine/options/selectors.py>

- Option data structures
  <https://github.com/BobbyAxerol/quantbt/blob/feat/option-engine/options/data.py>

- Option execution model
  <https://github.com/BobbyAxerol/quantbt/blob/feat/option-engine/options/execution.py>

## Deribit

- Public get instruments
  <https://docs.deribit.com/api-reference/market-data/public-get_instruments>

- Public get trades by instrument
  <https://docs.deribit.com/api-reference/market-data/public-get_last_trades_by_instrument>

- Rate limits
  <https://docs.deribit.com/articles/rate-limits>

- Inverse options
  <https://support.deribit.com/hc/en-us/articles/31424939096093-Inverse-Options>

---

## Final note

Các threshold về activity, bytes/row và projected storage là **V1 planning defaults**. Chúng phải được xác nhận bằng pilot ba market regimes trước full-history build. Kiến trúc, versioning, checkpoint ordering, disk-first constraints và 10 GiB post-cleanup budget là các quyết định chính thức.


## TRẢ LỜI

Các điểm bắt buộc verify bằng API probe trước khi build sâu
1. count=5000/10000

Tài liệu API Deribit hiện tại của public/get_last_trades_by_instrument công bố:

default count = 10
maximum count = 1000

Vì vậy không được mặc định production API hỗ trợ 5.000 hoặc 10.000. Repository RiveChen/deribit-historical-data báo cáo History API riêng từng chấp nhận page lớn và chạy tới khoảng 20 RPS, nhưng đây là hành vi thực nghiệm của history.deribit.com, không phải contract chính thức trong API docs.

Probe lần lượt:

count = 1000
count = 5000
count = 10000

Kiểm tra:

HTTP status
JSON-RPC error
số rows thực trả
response size
latency
memory peak

Policy:

Nếu 10000 hợp lệ → benchmark 5000 và 10000.
Nếu 5000 hợp lệ nhưng 10000 lỗi → dùng 5000.
Nếu cả hai không hợp lệ → fallback 1000.

Không hard-code 10000 trước khi probe.

2. Sorting và trade_seq boundary/overlap

Endpoint hỗ trợ:

sorting = asc | desc | default

Trong đó default chỉ có nghĩa là thứ tự dữ liệu rời database, không bảo đảm phù hợp với processing state. start_seq và end_seq đều được hỗ trợ nhưng tài liệu không mô tả rõ tính inclusive/exclusive của boundary.

Probe bằng một instrument có nhiều trades:

Request A: start_seq=100, end_seq=110, sorting=asc
Request B: start_seq=110, end_seq=120, sorting=asc
Request C: start_seq=100, end_seq=110, sorting=desc

Xác minh:

seq=110 có xuất hiện trong cả A và B không?
asc có thực sự tăng theo trade_seq không?
desc có thực sự giảm theo trade_seq không?
response có thể chứa seq ngoài requested range không?

Production policy:

Luôn sort client-side theo trade_seq.
Luôn deduplicate bằng (instrument_name, trade_seq).
Không phụ thuộc hoàn toàn vào response order.
Next cursor lấy từ max_trade_seq đã xác nhận + 1.
3. Expired instruments availability

Official get_instruments(expired=true) chỉ cam kết trả các instrument recently expired, không cam kết trả toàn bộ instrument lịch sử từ khi Deribit bắt đầu hoạt động.

Probe cần kiểm tra:

Số expired BTC options trả về.
Expiry nhỏ nhất.
Creation timestamp nhỏ nhất.
Có các contract từ 2017/2018 hay chỉ contract gần đây.
Một instrument cũ biết trước có query trade trực tiếp được không.

Nếu expired=true không trả toàn bộ lịch sử:

Dùng instrument list từ repository/reference manifest nếu đáng tin cậy;
hoặc discovery thêm từ currency-wide historical trades;
hoặc lưu instrument names phát hiện được trong quá trình backfill.

Không được coi expired=true là complete historical instrument master trước khi probe.

4. has_more, empty và unknown

has_more là field bắt buộc trong response, nhưng official docs không định nghĩa đủ rõ nó có nghĩa:

Còn dữ liệu trong requested sequence range.
Còn dữ liệu sau page hiện tại.
Hay còn dữ liệu toàn instrument.

Probe:

Range nhỏ hơn count.
Range lớn hơn count.
Range hoàn toàn không có trade.
Range nằm sau latest trade_seq.
Range có đúng count rows.

Production không nên dùng has_more làm tín hiệu duy nhất.

Phân loại response:

EMPTY_CONFIRMED:
    HTTP/JSON-RPC success
    result tồn tại
    trades == []
    không có error

SUCCESS_WITH_DATA:
    result.trades có rows hợp lệ

UNKNOWN:
    timeout
    connection reset
    HTTP non-2xx
    JSON-RPC error
    malformed JSON
    thiếu result/trades
    schema không hợp lệ

Chỉ EMPTY_CONFIRMED mới được advance/complete instrument. UNKNOWN phải retry hoặc dead-letter.

5. Rate limit và Retry-After

Official docs ghi riêng get_instruments có sustained limit khoảng 1 request/second. Repository RiveChen báo cáo History API có thể chạy khoảng 20 RPS, nhưng đây là behavior thực nghiệm và có thể khác theo IP, endpoint hoặc thời điểm.

Probe riêng từng nhóm:

get_instruments
get_last_trades_by_instrument
history.deribit.com
www.deribit.com nếu có fallback

Ramp test:

1 → 2 → 5 → 10 → 15 → 20 RPS

Theo dõi:

HTTP 429
JSON-RPC rate-limit error
Retry-After header
latency p95
timeout rate
successful RPS

Production policy:

get_instruments: tối đa 1 RPS mặc định.
trade history: khởi đầu 5 RPS.
Chỉ tăng lên 10–18 RPS sau probe.
Nếu có Retry-After: ưu tiên tuyệt đối.
Nếu không có: exponential backoff + jitter.
Giảm concurrency khi 429 liên tiếp.
6. Schema fields thực tế

Theo official schema, các field sau được đánh dấu required cho trade:

trade_id
trade_seq
instrument_name
timestamp
direction
tick_direction
index_price
price
amount
mark_price

Với option, iv được cung cấp cho option trades. contracts có thể vắng mặt trong historical trades. Các field liquidation, block, combo và Starbase đều optional.

Probe phải thống kê theo:

active vs expired instruments
old history vs recent history
regular vs block/combo trades
nhiều năm khác nhau

Báo cáo:

field_presence_rate
null_rate
invalid_type_rate
min/max timestamp
unknown_fields
schema variations by year

Quality policy:

trade_seq thiếu        → reject/quarantine
timestamp thiếu        → reject/quarantine
price thiếu            → reject/quarantine
amount thiếu           → reject/quarantine

mark_price thiếu       → giữ trade nếu cần audit,
                         nhưng không dùng làm reconstruction/execution anchor

index_price thiếu      → giữ trade nếu cần audit,
                         nhưng không tính moneyness/premium USD

iv thiếu hoặc <= 0     → giữ observed trade,
                         nhưng không reconstruct mark/Greeks

contracts thiếu        → dùng amount_base;
                         không tự suy ra nếu contract convention chưa verify

block/combo/liquidation thiếu
                       → coi là regular chỉ khi không có bằng chứng ngược lại,
                         đồng thời lưu FLAG_METADATA_ABSENT cho old schema

Nên có các flags:

MISSING_MARK_PRICE
MISSING_INDEX_PRICE
MISSING_IV
MISSING_CONTRACTS
INVALID_IV
INVALID_INDEX
IS_BLOCK
IS_COMBO
IS_LIQUIDATION
SCHEMA_LEGACY
SCHEMA_UNKNOWN_FIELD
Kết luận probe

Trước khi full backfill, agent phải xuất:

state/deribit_options/version=v1/api_probe_report.json

với kết luận đã xác minh:

selected_page_size
verified_sorting
sequence_boundary_semantics
expired_instrument_coverage
has_more_semantics
safe_trade_rps
get_instruments_rps
retry_after_behavior
field_presence_statistics
oldest_accessible_trade

Nếu chưa có report này, các giá trị count=5000, count=10000, 20 RPS và “expired instruments đầy đủ” chỉ được xem là assumptions, không phải production guarantees.

--------------------------------------------------------------------------------

# IMPLEMENTATION PLAN FOR `_get_data`

> Phần trên là **guide/spec chính thức V1**.
> Phần dưới đây là **implementation plan nội bộ cho repo** `/root/bobby/pool_alpha/alphas_storage/_get_data`.
> Không trộn semantics guide với log triển khai. Khi implementation thay đổi, chỉ update phần dưới hoặc append log mới.

## 0. Implementation Summary

Mục tiêu triển khai là thêm một subsystem Deribit BTC options historical vào `_get_data`, dùng cùng convention repo hiện tại nhưng không ép nó vào `PartitionedParquetStore.append()` vì Deribit có volume lớn và cần append-only staging.

Quyết định triển khai:

- Storage chính vẫn là **Parquet**.
- Query/compaction chính dùng **DuckDB external-memory**.
- Checkpoint/coverage dùng **SQLite**.
- Public endpoint vẫn đi qua `data_loader.py` / `load_data(...)`.
- Core loader logic mới đặt trong package OOP `loaders/`, nhưng không phá endpoint hiện tại.
- Collector facade là `collectors.deribit_option_trades`.
- Các module lõi đặt dưới `collectors/deribit/`.
- Docker vẫn dùng image chung `get_data-collectors:latest`.
- Không full-history Pandas concat.
- Không permanent JSONL mặc định.
- Không full dense option chain.
- Không full 1m permanent history.

## 1. Architecture Adaptation Với `_get_data`

### 1.1 Repo Integration

Thêm cấu trúc:

```text
collectors/
├── deribit_option_trades.py
└── deribit/
    ├── __init__.py
    ├── config.py
    ├── client.py
    ├── rate_limit.py
    ├── instruments.py
    ├── checkpoints.py
    ├── tasks.py
    ├── engine.py
    ├── normalize.py
    ├── filters.py
    ├── schema.py
    ├── staging.py
    ├── parquet_parts.py
    ├── compact.py
    ├── validate.py
    ├── repair.py
    ├── snapshot_5m.py
    ├── snapshot_1m.py
    ├── pricing.py
    ├── execution_proxy.py
    ├── cleanup.py
    ├── metrics.py
    └── reports.py

loaders/
├── __init__.py
└── deribit_options.py

configs/
└── deribit_historical_v1.yml
```

`data_loader.py` vẫn là public API. Nó import/wrap các class từ `loaders.deribit_options`.

### 1.2 Storage Layout

Giữ đúng layout V1:

```text
storage/
├── _staging/options/deribit/version=v1/currency=BTC/shard=XX/run_id=.../
├── _tmp/duckdb/
├── _tmp/compaction/
├── _cache/deribit_options/snapshot_1m/
├── _cache/deribit_options/held_overlay/
└── options/deribit/
    ├── instruments/version=v1/instruments.parquet
    ├── trades/version=v1/currency=BTC/year=YYYY/month=MM/day=DD/part-*.parquet
    ├── snapshot_5m/version=v1/currency=BTC/year=YYYY/month=MM/day=DD/part-*.parquet
    ├── execution_proxy/version=v1/
    └── manifests/version=v1/

state/
└── deribit_options/version=v1/
    ├── BTC.sqlite
    ├── api_probe_report.json
    ├── validation_summary.json
    ├── unresolved_ranges.parquet
    ├── storage_report.json
    └── locks/
```

### 1.3 Không Dùng Store Append-Rewrite Hiện Tại

`PartitionedParquetStore.append()` hiện tại phù hợp với datasets nhỏ/trung bình. Nó có pattern:

```text
read existing partition
concat new data
dedupe
rewrite partition
```

Deribit không dùng pattern này cho ingestion chính. Thay vào đó:

```text
API chunk
→ immutable staging part
→ SQLite coverage commit
→ daily compactor bằng DuckDB
→ canonical daily parts
```

### 1.4 Dependencies

`requirements.txt` cần có:

```text
httpx
aiolimiter
aiosqlite
tenacity
orjson
pyarrow
duckdb
pandas
PyYAML
xxhash
psutil
```

Nếu muốn giảm dependency mới:

- Có thể dùng `requests` + thread workers ở Phase 1 probe.
- Nhưng Phase 3 downloader nên dùng `httpx.AsyncClient`, `aiolimiter`, `aiosqlite`.
- `xxhash` có thể fallback sang SHA256 nếu dependency policy không cho thêm.

### 1.5 Public Loader Endpoints

Giữ endpoint cũ, thêm endpoint mới:

```python
from data_loader import DeribitOptionTrades, DeribitOptionSnapshots5m, load_data

trades = DeribitOptionTrades().load(
    start_date="2024-01-01",
    end_date="2024-02-01",
    currency="BTC",
    option_type="call",
    dte_min=0,
    dte_max=45,
    columns=None,
    version="v1",
)

snapshots = DeribitOptionSnapshots5m().load(
    start_date="2024-01-01",
    end_date="2024-02-01",
    currency="BTC",
    entry_eligible_only=False,
    version="v1",
)

snapshots = load_data(
    "deribit_option_snapshots_5m",
    start_date="2024-01-01",
    end_date="2024-02-01",
    currency="BTC",
)
```

Router names:

```text
deribit_option_trades
deribit_btc_option_trades
deribit_options_trades_v1
deribit_option_snapshots_5m
deribit_btc_option_snapshots_5m
deribit_options_5m
```

Không đổi behavior các endpoint Binance/VN hiện có.

### 1.6 Loader OOP Boundary

`loaders/deribit_options.py` chịu trách nhiệm:

- DuckDB read Parquet với hive partition pruning.
- Column projection.
- Date/time filtering.
- Join instrument dimension khi cần derived fields.
- Optional DTE/option_type filters.
- Return Pandas DataFrame ổn định.
- Không build full dense chain.

`data_loader.py` chịu trách nhiệm:

- Public class aliases.
- Router `load_data(...)`.
- Backward-compatible docs.

## 2. Versioning Và Config Freeze

### 2.1 Version IDs

Freeze V1:

```yaml
dataset_version: deribit_btc_options_v1
universe_version: compact_liquid_v1
schema_version: trade_schema_v1
snapshot_version: compact_5m_v1
pricing_version: anchored_iv_v1
execution_proxy_version: trade_mark_v1
```

Version ID phải nằm trong:

- Config.
- SQLite state.
- Parquet metadata.
- Manifest/report.
- Loader default args.

### 2.2 Config Hash

Mỗi run ghi `config_hash`. Hash tính từ YAML normalized, bỏ các field runtime không ảnh hưởng semantics nếu cần.

Checkpoint không dùng chung nếu `dataset_version` hoặc `universe_version` khác.

## 3. Phase Plan Chi Tiết

### Phase 0 — Freeze Interfaces

Mục tiêu:

- Chốt config schema.
- Chốt directory layout.
- Chốt SQLite schema.
- Chốt staging/canonical/snapshot schemas.
- Chốt CLI contracts.
- Chốt loader public API.

Deliverables:

- `configs/deribit_historical_v1.yml`
- `collectors/deribit/config.py`
- `collectors/deribit/schema.py`
- `collectors/deribit/checkpoints.py`
- `collectors/deribit_option_trades.py` CLI skeleton
- `loaders/deribit_options.py` loader skeleton
- README endpoint section draft
- Unit tests schema/config/checkpoint creation

Exit criteria:

- Config loads and validates.
- SQLite DB initializes idempotently.
- CLI help works for every subcommand.
- No network calls needed.

### Phase 1 — API Probe

Mục tiêu:

- Xác minh Deribit History API behavior trước khi build sâu.
- Không assume `count=5000/10000` hay `20 RPS`.

Deliverables:

- `collectors/deribit/client.py`
- `collectors/deribit/rate_limit.py`
- Probe command:

```bash
python -m collectors.deribit_option_trades probe --version v1
```

- Report:

```text
state/deribit_options/version=v1/api_probe_report.json
```

Probe phải verify:

- `count=1000/5000/10000`.
- Sorting `asc/desc/default`.
- Inclusive/exclusive sequence boundary.
- `has_more` semantics.
- Empty vs unknown.
- Expired instrument coverage.
- Oldest accessible BTC option trade.
- Safe RPS for `get_instruments`.
- Safe RPS for `get_last_trades_by_instrument`.
- `Retry-After` behavior.
- Schema field presence/null/type statistics.

Exit criteria:

- `api_probe_report.json` exists.
- `selected_page_size` chosen.
- `safe_trade_rps` chosen.
- `get_instruments_rps` chosen.
- `oldest_accessible_trade` recorded.
- If probe cannot verify a guarantee, config marks it as assumption and production backfill remains blocked.

### Phase 2 — Instrument Discovery + Checkpoint

Mục tiêu:

- Discover active/expired instruments.
- Parse metadata robustly.
- Build instrument dimension.
- Initialize SQLite state per instrument.

Deliverables:

- `collectors/deribit/instruments.py`
- `collectors/deribit/checkpoints.py`
- `collectors/deribit/tasks.py`
- Command:

```bash
python -m collectors.deribit_option_trades discover --version v1
```

Outputs:

```text
storage/options/deribit/instruments/version=v1/instruments.parquet
state/deribit_options/version=v1/BTC.sqlite
```

Exit criteria:

- Instrument dimension is deterministic.
- `instrument_id` stable across reruns.
- Invalid metadata uses `parse_status`, `metadata_source`, `quality_flags`; no silent zero.
- Active instruments not marked complete permanently.
- Expired instrument coverage limitation is recorded from probe.

### Phase 3 — Disk-First Downloader

Mục tiêu:

- Download historical trades by lazy `trade_seq` tasks.
- Keep bounded memory.
- Write immutable staging parts before checkpoint advance.

Deliverables:

- `collectors/deribit/engine.py`
- `collectors/deribit/normalize.py`
- `collectors/deribit/filters.py`
- `collectors/deribit/staging.py`
- `collectors/deribit/parquet_parts.py`
- Commands:

```bash
python -m collectors.deribit_option_trades backfill --version v1
python -m collectors.deribit_option_trades sync-once --version v1
```

Hot path:

```text
HTTP response
→ validate JSON-RPC
→ sort client-side by trade_seq
→ normalize minimal fields
→ broad filter + stateful activation
→ Arrow Table
→ write temp parquet
→ fsync/atomic rename
→ coverage commit
→ checkpoint advance
→ release memory
```

Exit criteria:

- No checkpoint before durable file.
- Empty retained chunks commit coverage without writing empty Parquet.
- UNKNOWN never advances checkpoint.
- Queue sizes bounded.
- Graceful stop leaves restartable state.
- Peak RSS target measured.

### Phase 4 — Compactor + Canonical Events

Mục tiêu:

- Compact immutable staging into canonical daily Parquet.
- Deduplicate `(instrument_id, trade_seq)`.
- Quarantine conflicts.
- Validate canonical partitions.

Deliverables:

- `collectors/deribit/compact.py`
- `collectors/deribit/validate.py`
- `collectors/deribit/repair.py`
- `collectors/deribit/cleanup.py`
- Commands:

```bash
python -m collectors.deribit_option_trades compact --version v1
python -m collectors.deribit_option_trades validate --version v1
python -m collectors.deribit_option_trades repair --version v1 --only-unresolved
```

Canonical path:

```text
storage/options/deribit/trades/version=v1/currency=BTC/year=YYYY/month=MM/day=DD/part-*.parquet
```

Exit criteria:

- DuckDB memory limit and temp directory enforced.
- No full-history Pandas concat.
- Daily partitions publish atomically.
- Duplicate key count zero after compaction.
- Conflicts recorded, not silently swallowed.
- Staging cleanup only after validation pass.

### Phase 5 — Pilot Benchmark

Mục tiêu:

- Không full backfill trước pilot.
- Chạy ba 30-day windows: low/normal/high volatility.
- Freeze real parameters from measured data.

Deliverables:

- Command:

```bash
python -m collectors.deribit_option_trades pilot --version v1
```

- Reports:

```text
state/deribit_options/version=v1/pilot_report_low.json
state/deribit_options/version=v1/pilot_report_normal.json
state/deribit_options/version=v1/pilot_report_high.json
state/deribit_options/version=v1/pilot_summary.json
```

Metrics:

- API requests.
- Response rows.
- Retained rows.
- Retention ratio.
- Bytes/trade.
- Snapshot rows.
- Bytes/snapshot row.
- Peak RSS.
- Disk projection.
- Strategy package coverage proxy.

Exit criteria:

- Projected permanent storage <= 9 GiB.
- Ingestion RSS <= 750 MiB.
- Snapshot RSS <= 1400 MiB.
- Unresolved coverage ranges = 0.
- Duplicate conflicts = 0.
- Strategy package coverage target >= 95%, or documented config adjustment.

### Phase 6 — Full Historical Backfill

Mục tiêu:

- Run full Deribit BTC historical ingestion after probe/pilot pass.
- Compact incrementally.
- Validate and repair.
- Cleanup staging transactionally.

Workflow:

```text
discover
backfill
compact
validate
repair if needed
compact repaired ranges
validate again
cleanup
storage-report
```

Exit criteria:

- No unresolved coverage ranges.
- No unknown completed instruments.
- Permanent storage <= budget.
- Cleanup report says staging removed only for validated partitions.
- `storage_report.json` records final bytes.

### Phase 7 — 5m Candidate Tape

Mục tiêu:

- Build permanent compact-liquid 5m snapshot tape.
- Implement reconstruction, expiry/strike/delta/activity selection, hard cap.

Deliverables:

- `collectors/deribit/snapshot_5m.py`
- `collectors/deribit/pricing.py`
- `collectors/deribit/metrics.py`
- Command:

```bash
python -m collectors.deribit_option_trades build-snapshot-5m --version v1
```

Output path:

```text
storage/options/deribit/snapshot_5m/version=v1/currency=BTC/year=YYYY/month=MM/day=DD/part-*.parquet
```

Exit criteria:

- Rows per timestamp <= 64.
- Expiries per timestamp <= 7.
- No post-expiry rows.
- No default IV.
- Observed/reconstructed/unavailable sources marked.
- Entry eligibility respects activity windows and TTL.
- Static fields not repeated unless intentionally projected by loader.

### Phase 8 — QuantBT Adapter + Loader

Mục tiêu:

- Expose stable endpoints for QuantBT.
- Keep long/ragged representation.
- Add held-position overlay.
- Add execution proxy on load.

Deliverables:

- `loaders/deribit_options.py`
- `data_loader.py` public classes/router.
- `collectors/deribit/execution_proxy.py`
- Held overlay cache manager.
- README loader docs.

Public classes:

```python
DeribitOptionTrades
DeribitOptionSnapshots5m
DeribitOptionOverlay
DeribitOptionExecutionProxy
```

Exit criteria:

- Loader date pruning works.
- Column projection works.
- DTE/option_type filters work.
- Candidate tape loads as long/ragged rows.
- Held contract does not disappear after leaving candidate tape.
- Proxy fields are clearly non-observed.
- Existing loader endpoints still pass all tests.

### Phase 9 — Optional 1m On-Demand Cache

Mục tiêu:

- Build near-ATM 1m cache only for experiments.
- Do not create permanent full-history 1m dataset.

Deliverables:

- `collectors/deribit/snapshot_1m.py`
- Cache lifecycle in `collectors/deribit/cleanup.py`.
- Command:

```bash
python -m collectors.deribit_option_trades build-snapshot-1m \
  --version v1 \
  --start YYYY-MM-DD \
  --end YYYY-MM-DD
```

Exit criteria:

- Cache max size and TTL enforced.
- No permanent 1m full history path.
- Cache cleanup idempotent.

### Phase 10 — Operations

Mục tiêu:

- Add Docker services/one-shot jobs.
- Add daily workflow docs.
- Add observability/storage reports.

Docker services:

```yaml
deribit-option-backfill:
  profiles: ["bootstrap"]
  restart: "no"

deribit-option-sync:
  restart: unless-stopped
```

Daily workflow:

```text
01:00 UTC discover + sync-once
02:00 UTC compact
02:30 UTC validate/repair
03:00 UTC build latest 5m partitions
03:30 UTC fit/update execution stats
04:00 UTC cleanup + storage report
```

Exit criteria:

- Docker image builds.
- One-shot commands work.
- Scheduled service does not run full backfill accidentally.
- README has endpoint and operations section.

## 4. Implementation Logs Format

Append logs only under this section. Newest log at bottom.

Template:

```markdown
### YYYY-MM-DD HH:MM UTC — Phase X: Short Title

Status: planned | in_progress | completed | blocked

Changed:
- ...

Commands:
- `...`

Validation:
- ...

Metrics:
- rows: ...
- files: ...
- bytes: ...
- peak_rss_mb: ...
- api_rps: ...

Decisions:
- ...

Open issues:
- ...

Commit:
- `abcdef0 message`
```

Initial log:

### 2026-07-24 UTC — Planning Handoff

Status: planned

Changed:
- Appended implementation plan below official V1 guide.
- No code changes yet.

Decisions:
- Use Parquet + DuckDB, consistent with current `_get_data` architecture.
- Add `loaders/` package for Deribit OOP loader internals while preserving `data_loader.py` endpoints.
- Deribit ingestion must use append-only staging + compaction, not current append-rewrite partition store.

Open issues:
- Need API probe before any production backfill assumptions.
- Need confirm dependency additions before Phase 1/3.

### 2026-07-24 UTC — Phase 0: Freeze Interfaces

Status: completed

Changed:
- Created branch `feat/option-ingestion` from `dev`.
- Added official V1 config at `configs/deribit_historical_v1.yml`.
- Added Deribit config loader/validator with stable `config_hash`.
- Added Arrow schemas for instrument dimension, staging trades, canonical trades, and 5m snapshots.
- Added SQLite checkpoint initializer for `instrument_state`, `download_ranges`, and metadata.
- Added CLI facade `collectors.deribit_option_trades` with Phase 0 `init` plus reserved future commands.
- Added OOP loader package `loaders/deribit_options.py` and public wrappers in `data_loader.py`.
- Added README endpoint/storage draft for Deribit V1.
- Added Phase 0 unit tests.

Commands:
- `python -m unittest tests.test_deribit_phase0`
- `python -m unittest discover tests`
- `python -m compileall collectors/deribit collectors/deribit_option_trades.py loaders data_loader.py tests/test_deribit_phase0.py`

Validation:
- Config loads and validates.
- SQLite checkpoint init is idempotent.
- CLI `init` creates checkpoint DB.
- Reserved future subcommands block instead of pretending implementation exists.
- Loader/router return empty schema-compatible DataFrames when Deribit data is not built yet.
- Existing `_get_data` test suite remains expected to pass after Phase 0.

Metrics:
- Network calls: 0
- Data backfill rows: 0
- Storage writes: SQLite checkpoint only during test/init

Decisions:
- Phase 0 does not add async/network dependencies yet.
- Phase 0 does not add Docker services yet; services belong to operations phases after probe/backfill commands are real.
- Deribit loader internals live in `loaders/`, while `data_loader.py` remains the public endpoint surface.

Open issues:
- Need API probe before `count`, RPS, expired coverage, and schema assumptions become production guarantees.
- Need choose whether Phase 1 uses sync `requests` first or adds `httpx`/`aiolimiter` immediately.

Commit:
- Included in Phase 0 commit `Add Deribit option ingestion phase 0`.

### 2026-07-24 UTC — Phase 1: API Probe

Status: completed

Changed:
- Added synchronous Deribit JSON-RPC client wrapper with rate limiting, retry/backoff, `Retry-After` parsing, and explicit result classification.
- Added probe runner that writes `state/deribit_options/version=v1/api_probe_report.json`.
- Added CLI command `python -m collectors.deribit_option_trades probe --version v1`.
- Probe report records `selected_page_size`, `verified_sorting`, `sequence_boundary_semantics`, `expired_instrument_coverage`, `has_more_semantics`, `safe_trade_rps`, `get_instruments_rps`, `retry_after_behavior`, `field_presence_statistics`, and `oldest_accessible_trade`.
- Added Phase 1 unit tests for client success/error/malformed payload behavior and probe report guardrails.

Commands:
- `python -m unittest tests.test_deribit_phase0 tests.test_deribit_phase1`
- `python -m unittest discover tests`
- `python -m compileall collectors/deribit collectors/deribit_option_trades.py loaders data_loader.py tests/test_deribit_phase1.py`
- Optional live probe: `python -m collectors.deribit_option_trades probe --version v1 --rate-ramp --max-rps 2 --requests-per-rps 1 --json`

Validation:
- `EMPTY_CONFIRMED` only means HTTP/JSON-RPC success with `result.trades == []`.
- HTTP/JSON-RPC/network/decode failures remain `UNKNOWN`, not empty data.
- `probe` without `--rate-ramp` writes a diagnostic report but keeps `production_backfill_allowed=false`.
- Production backfill is allowed only when mandatory fields exist and rate ramp is actually verified.

Metrics:
- Unit tests use fake clients only.
- No data backfill rows.
- Storage writes: probe report only when command/test runs.

Decisions:
- Phase 1 stays on `requests` to avoid adding async dependencies before API behavior is known.
- Conservative default RPS can be used for diagnostics but is not a production guarantee.
- Sequence boundaries are recorded as observed semantics; downloader phases must still use overlap + dedupe.

Open issues:
- Live Deribit probe should be rerun before Phase 2/3 on the production host/network.
- Expired instrument coverage remains observed coverage, not a complete-history guarantee.

Commit:
- Included in Phase 1 commit `Add Deribit API probe`.

### 2026-07-24 UTC — Phase 2: Instrument Discovery + Checkpoint

Status: completed

Changed:
- Added `collectors/deribit/instruments.py` for deterministic instrument discovery, Deribit option-name parsing, metadata fallback, stable `instrument_id`, and atomic dimension Parquet write.
- Added `quality_flags` to instrument dimension schema so invalid/fallback metadata is explicit.
- Added SQLite checkpoint upsert helper for `instrument_state` that preserves cursors/status across reruns.
- Added `collectors/deribit/tasks.py` for initial sequence task planning from checkpoint state.
- Enabled CLI command `python -m collectors.deribit_option_trades discover --version v1`.
- Updated README operational commands and Deribit notes.
- Added Phase 2 unit tests.

Commands:
- `python -m unittest tests.test_deribit_phase0 tests.test_deribit_phase1 tests.test_deribit_phase2`
- `python -m unittest discover tests`
- `python -m compileall collectors/deribit collectors/deribit_option_trades.py loaders data_loader.py tests/test_deribit_phase2.py`
- Optional live discovery: `python -m collectors.deribit_option_trades discover --version v1 --json`

Validation:
- `instrument_id` is hash-based from `instrument_name`, so adding future contracts does not renumber existing IDs.
- Instrument dimension writes to `storage/options/deribit/instruments/version=v1/instruments.parquet`.
- Missing API expiry/strike/type can be filled from instrument name, with `quality_flags` marking the fallback.
- Invalid metadata uses `parse_status=INVALID` and quality flags; no silent zero.
- Re-running discovery does not reset `last_processed_seq`, failure count, or non-terminal status.
- Active instruments start as resumable `NEW`/existing non-terminal state, never permanent complete.
- If `history.deribit.com` returns future contracts through `expired=true`, Phase 2 treats them as active/resumable by comparing expiry timestamp to current UTC.

Metrics:
- Unit tests use fake Deribit client only.
- Live discovery reads instrument master only; it does not download trades.
- Data backfill rows: 0.

Decisions:
- Store physical instrument Parquet under `version=v1`; code reads it via `ParquetFile(...).read()` to avoid PyArrow auto-adding hive partition column `version`.
- Phase 2 task planning creates first sequence ranges only; actual downloader/coverage commit remains Phase 3.

Open issues:
- Full expired instrument coverage is still only as complete as Deribit `get_instruments(expired=true)` returns.
- Phase 3 must use `api_probe_report.json` before deciding page size/RPS for downloader workers.

Commit:
- Included in Phase 2 commit `Add Deribit instrument discovery`.

### 2026-07-24 UTC — Phase 3: Disk-First Downloader

Status: completed

Changed:
- Added `collectors/deribit/engine.py` for bounded sync `backfill`/`sync-once` staging downloads.
- Added `collectors/deribit/normalize.py` to sort by `trade_seq`, encode minimal staging fields, and apply broad activation/retention.
- Added `collectors/deribit/filters.py` for V1 broad DTE/moneyness policy helpers.
- Added `collectors/deribit/staging.py` and `collectors/deribit/parquet_parts.py` for immutable staging Parquet writes with temp file, atomic rename, fsync, and checksum.
- Extended SQLite checkpoint schema to version 2 with `activated_at_ms` and `activation_seq`; migration is idempotent for Phase 2 DBs.
- Added checkpoint transition helpers for retryable failure and success coverage commits.
- Enabled CLI commands `backfill` and `sync-once`; default `--max-tasks=1` keeps first runs/smokes bounded.
- Added Phase 3 unit tests.

Commands:
- `python -m unittest tests.test_deribit_phase0 tests.test_deribit_phase1 tests.test_deribit_phase2 tests.test_deribit_phase3`
- `python -m unittest discover tests`
- `python -m compileall collectors/deribit collectors/deribit_option_trades.py loaders data_loader.py tests/test_deribit_phase3.py`
- Optional live smoke after probe/discover: `python -m collectors.deribit_option_trades backfill --version v1 --symbols BTC-25JUN27-160000-C --max-tasks 1 --json`

Validation:
- No checkpoint advance happens before durable staging file write when retained rows exist.
- Empty retained chunks with response rows commit coverage without writing empty Parquet.
- UNKNOWN/API failure records retryable state and never inserts `download_ranges` or advances `last_processed_seq`.
- Active instruments become `CAUGHT_UP_ACTIVE`, not permanently complete, when API confirms no more current rows.
- Expired empty instruments become `EMPTY_CONFIRMED`; expired exhausted instruments become `COMPLETE_EXPIRED`.
- Activation state is persisted in SQLite so subsequent chunks retain post-activation trades until expiry.
- Memory cleanup is called after each attempted task.

Metrics:
- Unit tests use fake Deribit client only.
- Default CLI task count: 1.
- Data writes are staging-only under `storage/_staging/options/deribit/version=v1/...`.
- Live smoke `BTC-25SEP26-115000-C`, `--max-tasks 1`: response rows `1,854`, retained rows `333`, discarded rows `1,521`, files written `1`, peak RSS `339.96 MB`, cursor advanced to `trade_seq=1,854`.

Decisions:
- Phase 3 is synchronous and bounded; worker pool/RSS pilot belongs to Phase 5.
- Page size and RPS are read from `api_probe_report.json` when production probe is available.
- `--allow-unprobed` is test/manual-only; normal CLI blocks before discovery/download if probe report is missing or not production-allowed.
- `sync-once` refreshes instrument discovery before task planning; `backfill` refreshes only with `--discover-first`.
- Checksum uses `blake2b_128` from stdlib for now; xxhash can be added later if dependency is approved.

Open issues:
- Phase 4 must compact staging into canonical daily Parquet and dedupe `(instrument_id, trade_seq)`.
- Phase 5 should benchmark worker counts and enforce RSS targets with real market-regime samples.

Commit:
- Included in Phase 3 commit `Add Deribit disk-first downloader`.

### 2026-07-24 UTC — Phase 4: Compactor + Canonical Events

Status: completed

Changed:
- Added `collectors/deribit/compact.py` using DuckDB with configured memory limit and temp directory.
- Added canonical daily publish to `storage/options/deribit/trades/version=v1/currency=BTC/year=YYYY/month=MM/day=DD/part-00000.parquet`.
- Added deterministic dedupe by `(instrument_id, trade_seq)` ordered by `source_priority DESC, ingested_at DESC`.
- Added conflict detection for duplicate keys with payload variants; conflict samples are written under `state/deribit_options/version=v1/conflicts/`.
- Added `collectors/deribit/validate.py` for acquisition ledger checks, checksum verification, canonical schema, duplicate key, basic financial-field checks, and instrument dimension FK checks.
- Added `collectors/deribit/repair.py` as a non-destructive repair planner for unresolved/retryable state.
- Added `collectors/deribit/cleanup.py`; cleanup is dry-run by default and requires validation pass plus `--confirm` to delete staging.
- Enabled CLI commands `compact`, `validate`, `repair`, and `cleanup`.
- Added Phase 4 unit tests.

Commands:
- `python -m unittest tests.test_deribit_phase4`
- `python -m unittest tests.test_deribit_phase0 tests.test_deribit_phase1 tests.test_deribit_phase2 tests.test_deribit_phase3 tests.test_deribit_phase4`
- `python -m unittest discover tests`
- `python -m compileall collectors/deribit collectors/deribit_option_trades.py loaders data_loader.py tests/test_deribit_phase4.py`
- Live smoke: `python -m collectors.deribit_option_trades compact --version v1 --max-days 1 --json`
- Live smoke: `python -m collectors.deribit_option_trades validate --version v1 --json`
- Live smoke: `python -m collectors.deribit_option_trades cleanup --version v1 --json`

Validation:
- DuckDB memory limit and temp directory are set before compaction.
- No Pandas full-history concat is used.
- Daily partitions publish through temp file then atomic replace/fsync.
- Duplicate canonical keys are removed by compaction and checked by validator.
- Payload conflicts are recorded under state and reported as `status=warning`.
- Cleanup does not delete staging unless validator returns `ok` and `--confirm` is passed.

Metrics:
- Unit tests use synthetic staging files only.
- Live compact smoke from Phase 3 staging: staging files `1`, days compacted `1`, canonical output files `1`, output rows `9`, conflict groups `0`.
- Live validate smoke: canonical files `1`, canonical rows `9`, duplicate keys `0`, status `ok`.
- Live cleanup dry-run: staging files seen `1`, bytes seen `18,891`, files deleted `0`.
- Post-test/live cleanup: `gc.collect()` collected `10`, PyArrow memory pool `0` bytes allocated.

Decisions:
- Repair in Phase 4 is a planner/report, not a mutating auto-repair; exact refetch execution belongs with downloader/repair expansion.
- Cleanup is intentionally conservative and dry-run-first.
- Canonical compaction currently writes one output file per day; multi-file target sizing can be tuned after pilot.

Open issues:
- Phase 5 must benchmark compaction/day sizing on larger pilot windows.
- Full conflict quarantine payload can be expanded if pilot finds real conflicts.

Commit:
- Included in Phase 4 commit `Add Deribit canonical compactor`.

### 2026-07-24 UTC — Phase 5: Pilot Benchmark

Status: completed

Changed:
- Added `collectors/deribit/pilot.py` for deterministic three-regime pilot reports.
- Enabled CLI command `python -m collectors.deribit_option_trades pilot --version v1`.
- Pilot writes:
  - `state/deribit_options/version=v1/pilot_report_low.json`
  - `state/deribit_options/version=v1/pilot_report_normal.json`
  - `state/deribit_options/version=v1/pilot_report_high.json`
  - `state/deribit_options/version=v1/pilot_summary.json`
- Pilot measures canonical rows/bytes, bytes per trade, trade-day coverage proxy, contracts, validation status, repair/unresolved state, duplicate conflicts, RSS, and projected permanent size when representative samples exist.
- Added Phase 5 unit tests.

Commands:
- `python -m unittest tests.test_deribit_phase5`
- `python -m unittest tests.test_deribit_phase0 tests.test_deribit_phase1 tests.test_deribit_phase2 tests.test_deribit_phase3 tests.test_deribit_phase4 tests.test_deribit_phase5`
- `python -m unittest discover tests`
- `python -m compileall collectors/deribit collectors/deribit_option_trades.py loaders data_loader.py tests/test_deribit_phase5.py`
- Live smoke: `python -m collectors.deribit_option_trades pilot --version v1 --json`

Validation:
- Pilot windows are deterministic and non-overlapping.
- Pilot does not run full history or mutate ingestion/checkpoint state.
- Status remains `blocked` until all three windows have representative samples and acceptance gates pass.
- Full historical Phase 6 must not treat a blocked pilot as approval.

Metrics:
- Unit tests use synthetic canonical files for all three windows and a blocked/no-sample case.
- Live pilot status on current partial smoke data: `blocked`.
- Live current canonical rows: `9`, canonical files: `1`, duplicate keys: `0`, validation status: `ok`.
- Live three pilot windows have `0` rows each because only 2026 smoke data exists so far.
- Live ingestion RSS observed by pilot: `158.28 MB`.
- Post-test/live cleanup: `gc.collect()` collected `10`, PyArrow memory pool `0` bytes allocated.

Decisions:
- Strategy package coverage is a proxy in Phase 5 based on trade-day coverage until Phase 7 snapshot tape exists.
- Permanent size projection is `null` when pilot windows have no measured rows; this intentionally fails acceptance.
- Pilot exit code is non-zero when summary status is `blocked`, so automation cannot proceed to Phase 6 accidentally.
- `backfill` and `sync-once` now require `pilot_summary.json` status `ok` for normal broad runs. Blocked-pilot override is restricted to explicit symbols and `max_tasks<=20` for targeted pilot sampling only.

Open issues:
- Need targeted pilot data for low/normal/high windows before Phase 6 full historical backfill.
- Phase 7 will replace the temporary strategy coverage proxy with snapshot-package coverage.

Commit:
- Included in Phase 5 commit `Add Deribit pilot benchmark`.

### 2026-07-24 UTC — Pre-Phase 6 Readiness Audit

Status: blocked

Checked:
- API probe report exists and has `status=ok`, `production_backfill_allowed=true`, `selected_page_size=10000`, `safe_trade_rps=2.0`.
- Phase 4 validation returns `status=ok`, `canonical_files=1`, `canonical_rows=9`, `duplicate_keys=0`.
- Repair planner returns `status=ok`, `retryable_instruments=0`, `missing_output_ranges=0`.
- Pilot summary exists but returns `status=blocked`.

Blocking reason:
- Phase 5 pilot has not passed because low/normal/high deterministic windows currently have no representative canonical rows.
- Permanent size projection is `null`; strategy package coverage proxy is `0.0%`.

Added guard:
- Normal `backfill`/`sync-once` now blocks when `pilot_summary.json` is missing or not `ok`.
- Targeted pilot sampling is still possible with `--allow-blocked-pilot`, but only with explicit symbols and `max_tasks<=20`.

Decision:
- Do not start Phase 6 full historical backfill until targeted pilot windows have enough samples and `pilot` returns `status=ok`.

### 2026-07-28 UTC — Pre-Phase 6 Readiness Audit Refresh

Status: blocked

Checked:
- Branch: `feat/option-ingestion`.
- Working tree was clean before this audit note.
- Phase 0-5 unit suite passed: `37 tests`.
- Compile check passed for Deribit collectors, loader package, `data_loader.py`, and Phase 0-5 tests.
- Live/current `validate` returned `status=ok`, `canonical_files=1`, `canonical_rows=9`, `duplicate_keys=0`.
- Live/current `repair --only-unresolved` returned `status=ok`, `retryable_instruments=0`, `missing_output_ranges=0`.
- `api_probe_report.json` still has `status=ok`, `production_backfill_allowed=true`, `selected_page_size=10000`, `safe_trade_rps=2.0`, `get_instruments_rps=1.0`.
- Loader smoke passed for `DeribitOptionTrades().load(...)` and `load_data("deribit_option_trades", ...)` against the tiny canonical sample.
- Phase 7+ commands such as `build-snapshot-5m` remain explicitly reserved/blocked.
- Normal `backfill --max-tasks 1` returned `status=blocked` because `pilot_summary.json` is not `ok`.
- Blocked-pilot override guard works: `--allow-blocked-pilot` is rejected unless symbols are explicit and `max_tasks<=20`.
- Post-test cleanup: `gc.collect()` collected `5`, PyArrow memory pool `0` bytes allocated.

Blocking reason:
- Phase 5 implementation is complete, but Phase 5 acceptance is not complete.
- Current `pilot` output is still `status=blocked`.
- Low/normal/high deterministic windows still have `0` canonical rows each:
  - low: `2022-09-01` to `2022-10-01`;
  - normal: `2024-04-01` to `2024-05-01`;
  - high: `2021-05-01` to `2021-05-31`.
- `permanent_size_projection_gib` is `null`.
- `strategy_package_coverage_pct` is `0.0`.

Decision:
- Do not start Phase 6 full historical backfill yet.
- The next safe step is targeted pilot sampling only: run bounded `backfill` with explicit pilot-window symbols and `--allow-blocked-pilot`, then `compact`, `validate`, `repair`, and `pilot` until `pilot_summary.json` returns `status=ok`.
- Treat any broad `backfill`/`sync-once` before an `ok` pilot as a bug or manual override violation.

### 2026-07-28 UTC — Targeted Pilot Sampling Before Phase 6

Status: completed

Changed:
- Ran bounded targeted pilot sampling for explicit BTC option symbols across the three deterministic Phase 5 windows.
- Confirmed sandboxed network calls can hang/retry silently; Deribit History API sampling was run with approved network escalation.
- Compacted targeted staging into canonical daily Parquet.
- Added cleanup manifest support so `cleanup --confirm` can remove staging while future `validate` still accepts ledger rows whose deleted staging files are recorded with matching checksum.
- Fixed Phase 5 pilot notes so `status=ok` reports that the Phase 6 gate is open instead of saying the run remains blocked.

Targeted symbols:
- high window `2021-05-01` to `2021-05-31`: `BTC-25JUN21-40000-P`, `BTC-25JUN21-40000-C`, `BTC-25JUN21-50000-P`, `BTC-25JUN21-50000-C`, `BTC-25JUN21-56000-P`, `BTC-25JUN21-56000-C`.
- low window `2022-09-01` to `2022-10-01`: `BTC-30SEP22-19000-P`, `BTC-30SEP22-19000-C`, `BTC-30SEP22-20000-P`, `BTC-30SEP22-20000-C`, `BTC-30SEP22-21000-P`, `BTC-30SEP22-21000-C`.
- normal window `2024-04-01` to `2024-05-01`: `BTC-31MAY24-65000-P`, `BTC-31MAY24-65000-C`, `BTC-31MAY24-70000-P`, `BTC-31MAY24-70000-C`, `BTC-31MAY24-75000-P`, `BTC-31MAY24-75000-C`.

Commands:
- `python -m collectors.deribit_option_trades backfill --version v1 --symbols BTC-31MAY24-70000-C --max-tasks 1 --allow-blocked-pilot --json`
- `python -m collectors.deribit_option_trades backfill --version v1 --symbols <17 explicit pilot symbols> --max-tasks 17 --allow-blocked-pilot --json`
- `python -m collectors.deribit_option_trades compact --version v1 --json`
- `python -m collectors.deribit_option_trades validate --version v1 --json`
- `python -m collectors.deribit_option_trades repair --version v1 --only-unresolved --json`
- `python -m collectors.deribit_option_trades pilot --version v1 --json`
- `python -m collectors.deribit_option_trades cleanup --version v1 --confirm --json`

Validation:
- Phase 0-5 unit suite passed: `37 tests`.
- Phase 4/5 focused suite passed: `9 tests`.
- Compile check passed for Deribit collectors, loader package, `data_loader.py`, and touched tests.
- Post-cleanup `validate` returned `status=ok`, `canonical_files=378`, `canonical_rows=82025`, `duplicate_keys=0`.
- Post-cleanup `repair --only-unresolved` returned `status=ok`, `retryable_instruments=0`, `missing_output_ranges=0`.
- Post-cleanup `pilot` returned `status=ok`.
- Staging cleanup deleted `19` files / `3,001,172` bytes; remaining staging parquet files: `0`.
- Post-test cleanup: `gc.collect()` collected `5`, PyArrow memory pool `0` bytes allocated.

Metrics:
- Targeted API sampling wrote `18` staging files.
- First single-symbol smoke: `response_rows=7017`, `retained_rows=7017`, `peak_rss_mb=370.98`.
- Remaining batch: `response_rows=81217`, `retained_rows=74675`, `discarded_rows=6542`, `peak_rss_mb=369.48`.
- Compaction: `days_compacted=378`, `output_files=378`, `output_rows=82025`, `conflict_groups=0`.
- Pilot measured rows: `33779`.
- Pilot measured bytes: `1102574`.
- Pilot bytes per trade: `32.64081233902721`.
- Pilot total canonical bytes: `3064679`.
- Pilot ingestion RSS: about `201 MB`, below `750 MB` hard limit.
- Pilot permanent size projection: `0.000033 GiB`, below `9 GiB` gate. This is a pilot proxy, not the final full-history storage estimate.

Pilot windows:
- low: `2022-09-01` to `2022-10-01`, `canonical_rows=16151`, `contracts=5`, `trade_days=30/30`, coverage `100%`.
- normal: `2024-04-01` to `2024-05-01`, `canonical_rows=4401`, `contracts=6`, `trade_days=30/30`, coverage `100%`.
- high: `2021-05-01` to `2021-05-31`, `canonical_rows=13227`, `contracts=6`, `trade_days=30/30`, coverage `100%`.

Decisions:
- Phase 6 full historical backfill gate is now open because `pilot_summary.json` is `status=ok`.
- Do not interpret the tiny pilot permanent-size projection as the final full-history size estimate; Phase 6 must still emit final storage report after full compact/cleanup.
- Keep broad Phase 6 backfill separate from this targeted pilot sampling commit/run.

### 2026-07-28 UTC — Phase 6 Runtime Correction

Status: corrected_runtime_ready

Changed:
- Stopped direct long-running shell Phase 6 backfill because it did not expose per-task progress logs and did not match the Docker service architecture.
- Added Deribit backfill progress logging for Docker logs:
  - `deribit_backfill_start`;
  - `deribit_task_start`;
  - `deribit_task_done`;
  - `deribit_task_failed`;
  - `deribit_backfill_done`.
- Added CLI expiry filters for controlled Phase 6 slices:
  - `--expiry-start`;
  - `--expiry-end`;
  - `--progress-every`.
- Added Docker Compose one-shot jobs:
  - `deribit-option-backfill-2022`;
  - `deribit-option-backfill-full`;
  - `deribit-option-cycle-2022`;
  - `deribit-option-cycle-full`;
  - `deribit-option-compact`;
  - `deribit-option-validate`;
  - `deribit-option-repair`;
  - `deribit-option-cleanup`.
- Fixed compactor to merge existing canonical daily Parquet with new staging rows before publishing. This prevents data loss when staging was cleaned after a previous compact and a later batch adds more rows for the same day.
- Fixed checkpoint storage references to be portable across host CLI and Docker:
  - new `download_ranges.output_file` values are stored as `storage/...`;
  - validator resolves legacy `/root/.../storage/...` paths to the active `DATA_ROOT`;
  - cleanup manifest keys are portable and remain valid after staging files are deleted.
- Added regression coverage for legacy absolute checkpoint paths.

Partial direct-run state before correction:
- Interrupted raw command had advanced checkpoint safely to `download_ranges=4014`.
- Status counts at interruption:
  - `CAUGHT_UP_ACTIVE=1`;
  - `COMPLETE_EXPIRED=3025`;
  - `EMPTY_CONFIRMED=988`;
  - `NEW=115815`.
- The direct-run partial staging must be compacted/validated/cleaned through Docker jobs before continuing.

Docker correction results:
- Rebuilt shared Docker image `get_data-collectors:latest`.
- Compacted direct-run staging through Docker:
  - `staging_files=2628`;
  - `days_compacted=538`;
  - `output_rows=527629`;
  - `conflict_groups=0`.
- Docker validate before cleanup:
  - `status=ok`;
  - `canonical_files=899`;
  - `canonical_rows=639076`;
  - `duplicate_keys=0`.
- Docker cleanup deleted `2628` validated staging files and wrote cleanup manifest.
- Docker validate after cleanup remained `status=ok`.

Controlled 2022 Docker pilot slice:
- Command shape: `DERIBIT_BACKFILL_MAX_TASKS=100 DERIBIT_PROGRESS_EVERY=10 docker compose --profile deribit run --rm deribit-option-backfill-2022`.
- Backfill result:
  - `tasks_attempted=100`;
  - `tasks_succeeded=100`;
  - `files_written=68`;
  - `retained_rows=5314`;
  - `discarded_rows=38`;
  - `peak_rss_mb=351.92`.
- Docker compact after pilot:
  - `staging_files=68`;
  - `days_compacted=9`;
  - `output_files=9`;
  - `output_rows=10148`;
  - `conflict_groups=0`.
- Docker validate after pilot compact:
  - `status=ok`;
  - `canonical_files=901`;
  - `canonical_rows=644390`;
  - `duplicate_keys=0`.
- Docker cleanup after pilot deleted `68` staging files.
- Final Docker validate after cleanup remained `status=ok`.
- Final staging parquet count: `0`.
- Disk check after pilot:
  - `storage/options/deribit=26M`;
  - `state/deribit_options=23M`;
  - staging parquet files: `0`;
  - pilot rows are already merged into canonical storage, not left in staging.
- Checkpoint summary after pilot:
  - `download_ranges=4131`;
  - `CAUGHT_UP_ACTIVE=1`;
  - `COMPLETE_EXPIRED=3104`;
  - `EMPTY_CONFIRMED=1026`;
  - `NEW=115698`.

Decision:
- Phase 6 continuation must run through Docker Compose jobs, not a raw long shell command.
- First controlled slice is expiry up to `2022-12-31` via `deribit-option-backfill-2022`.
- The first 2022 pilot slice validates cleanly. Do not start full historical in chat; operator should inspect this slice first, then continue with Docker batches.
- After the 2022 slice is accepted, continue remaining history with `deribit-option-backfill-full` in checkpointed batches.
- Preferred background command is `docker compose --profile deribit-full up -d deribit-option-cycle-full` because it runs `backfill -> compact -> validate -> cleanup -> validate` and then exits the container. Re-running the same command later resumes from SQLite checkpoint.

### 2026-07-29 UTC — Phase 6 Contracts Field Repair

Status: complete

Reason:
- Deribit historical trade responses can omit optional `contracts`.
- Current canonical sample had `contracts` null for a large subset, while instrument dimension has `contract_size=1.0` for all discovered BTC option instruments.
- `amount` is the option trade amount in underlying base currency, so `contracts = amount_base / contract_size` is the correct derived fill when source `contracts` is missing.

Changed:
- Future ingestion derives `contracts` from `amount_base / contract_size` if Deribit omits source `contracts`.
- `MISSING_CONTRACTS` flag remains set so downstream users can distinguish source-native contracts from derived contracts.
- Added `repair-contracts` CLI command for idempotent canonical Parquet repair.
- Added `repair-contracts` into both Docker cycle jobs before validate/cleanup.

Repair result on existing canonical data:
- Dry run:
  - `files_seen=946`;
  - `files_repaired=610`;
  - `rows_seen=752072`;
  - `rows_repaired=378218`.
- Confirmed repair:
  - `status=ok`;
  - `files_repaired=610`;
  - `rows_repaired=378218`.
- Deep validation after repair:
  - `contracts_null=0`;
  - `contracts_non_positive=0`;
  - `amount_non_positive=0`;
  - `duplicate_key_rows=0`;
  - Deribit validator `status=ok`.

## 5. Test Plan Theo Phase

### Phase 0 Tests

- Config schema validates required keys and defaults.
- Version IDs are present and stable.
- SQLite schema creates idempotently.
- CLI help returns zero.
- Loader skeleton imports without side effects.

### Phase 1 Tests

- Client handles JSON-RPC success/error.
- Client handles timeout/invalid JSON/missing result.
- Retry honors `Retry-After` when present.
- Probe distinguishes `EMPTY_CONFIRMED` from `UNKNOWN`.
- Probe report includes all mandatory fields.
- Network smoke must be explicit, not part of default unit suite.

### Phase 2 Tests

- Instrument name parser handles BTC option names.
- Invalid metadata gets flags, not silent zero.
- Instrument IDs stable across reruns.
- Active instruments stay resumable.
- Expired empty instruments can become `EMPTY_CONFIRMED`.
- SQLite state transitions valid.

### Phase 3 Tests

- Downloader writes temp before final.
- Simulated crash before checkpoint causes safe duplicate refetch.
- Checkpoint never advances before durable file.
- Empty retained response commits coverage without file.
- Unknown response retries/dead-letters.
- Queue hard caps enforced.
- Memory release called after chunks.

### Phase 4 Tests

- DuckDB compactor dedupes duplicate staging rows.
- Conflict payload is quarantined.
- Daily partition pruning works.
- Atomic publish leaves old canonical intact on failure.
- Validation catches duplicate key, missing dimension FK, invalid price/IV.
- Cleanup does not delete staging on validation fail.

### Phase 5 Tests

- Pilot windows are non-overlapping and deterministic.
- Pilot report contains API/storage/RSS/coverage metrics.
- Projection rejects config above hard budget.
- Pilot can be rerun idempotently.

### Phase 6 Tests

- Full run can resume after interruption.
- Coverage ranges have no unresolved gaps.
- Repair exact ranges does not rollback cursor.
- Storage report bytes match filesystem scan.

### Phase 7 Tests

- 5m bar timing is `(t-5m, t]`.
- Expired instruments removed.
- Anchor equality at observed trade.
- No default IV reconstruction.
- Hard cap 64 enforced.
- Package completeness prioritizes contiguous strikes.
- Activity filters exclude block/combo/liquidation by default.

### Phase 8 Tests

- Loader date pruning and column projection.
- DTE/option_type filters.
- Candidate snapshot read with DuckDB.
- Held overlay includes pinned instruments.
- Execution proxy uses only past data.
- Existing `_get_data` test suite remains green.

### Phase 9 Tests

- 1m cache respects DTE/ATM profile.
- Cache size cap and TTL cleanup.
- No permanent full-history 1m artifact.

### Phase 10 Tests

- Docker build.
- One-shot commands.
- Service env roots.
- Logs and reports written under `logs/` and `state/`.
- Cleanup/prune does not delete canonical data.

## 6. Technical Debt / Accepted Limitations

Accepted V1 limitations:

- No historical bid/ask.
- No historical order book.
- No exact exchange Greeks between trades.
- No dense full chain.
- No full ETH options.
- No permanent 1m history.
- Index is derived from option trade observations unless V2 adds official index.
- Execution model is proxy, not execution-grade simulator.
- Settlement may need V2 delivery-price enrichment.

Technical debt to track:

- `loaders/` package is new; current repo mostly uses `data_loader.py` monolith.
- Async dependencies introduce more test surface than current sync collectors.
- SQLite state requires migration discipline for future schema versions.
- DuckDB compactor needs careful temp disk management.
- Strategy coverage metric requires QuantBT fixture or adapter mock.
- Full-history storage estimate must be recalibrated after pilot.
- Root Poetry env dependency drift should be kept separate from `_get_data` Docker requirements.

## 7. Notes / Decisions / Open Questions

### Decisions

- Deribit V1 must not start full backfill before `api_probe_report.json` exists.
- Page size and RPS are probe outputs, not constants.
- Expired instrument discovery is not trusted as complete until probe verifies coverage.
- Unknown API response is never equivalent to empty.
- Canonical sequence is allowed to be non-dense because broad filter discards intentional pre-activation trades.
- Correctness is coverage-ledger based, not canonical dense sequence based.
- Observed exchange mark and reconstructed mark remain separate fields/provenance.
- Block/combo/liquidation trades are excluded from regular liquidity/execution calibration by default.
- After test/compile/live smoke runs that touch Parquet or large loaders, agent must run best-effort memory cleanup with `gc.collect()` and `pyarrow.default_memory_pool().release_unused()`, then report collected object count and PyArrow bytes allocated when relevant.

### Open Questions For Probe/Pilot

- What is the oldest BTC option trade accessible from History API today?
- Does `expired=true` return complete historical instrument master or only recent expired?
- What is the real max `count` accepted by `history.deribit.com`?
- Are `start_seq` and `end_seq` inclusive on both sides?
- Does response ever contain sequence outside requested range?
- Does `has_more` mean within range, after page, or whole instrument?
- What is stable safe RPS on this VPS/IP?
- How frequent are missing `mark_price`, `index_price`, and `iv` in older years?
- Does `amount` represent BTC amount consistently for inverse options across years?
- Are legacy block/combo/liquidation fields absent or explicit false?

## 8. Documentation Updates Required During Implementation

Update `README.md` when endpoints become real:

- Supported Data Sources table: Deribit BTC options trades/snapshot.
- Storage Layout: Deribit paths.
- Data Contracts: canonical trades, instrument dimension, snapshot 5m.
- Loader Endpoints table.
- One-shot commands.
- Docker services.
- Integrity rules.

Update this markdown after every completed phase:

- Add implementation log.
- Record deviations from guide.
- Record measured API behavior.
- Record pilot-chosen parameters.
- Record final storage/RSS metrics.

## 9. Commit Policy

Commit after each phase or meaningful safe milestone.

Suggested messages:

```text
Add Deribit V1 config and schemas
Add Deribit API probe
Add Deribit instrument checkpoint store
Add Deribit disk-first staging downloader
Add Deribit daily compactor
Add Deribit pilot benchmark reports
Add Deribit snapshot 5m builder
Add Deribit options loaders
Add Deribit operations workflow
```

Never mix:

- Root Poetry dependency changes with `_get_data` collector changes.
- Generated full data artifacts with code commits.
- Unrelated dirty docs such as existing orderbook markdown changes.

## 10. Done Definition For The Whole Job

The Deribit V1 job is complete only when:

- API probe report exists and is referenced by config.
- Pilot passes or thresholds are adjusted with documented reason.
- Full historical backfill completes without unresolved ranges.
- Canonical trades have no duplicate `(instrument_id, trade_seq)`.
- Instrument dimension FK validation passes.
- 5m snapshot tape exists and respects cap/expiry rules.
- Loader endpoints work through `data_loader.py`.
- QuantBT adapter can build target package fixtures.
- Held overlay prevents opened contracts from disappearing.
- Execution proxy is fitted without future leakage.
- Cleanup removes staging only after validation.
- Permanent post-cleanup storage <= 10 GiB.
- Docker operations are documented.
- Full `_get_data` test suite passes.
