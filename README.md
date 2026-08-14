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
| Binance USD-M Quarterly concrete contracts | `1m` | Configured BTC/ETH quarterly historical + active contracts | Binance Vision monthly/daily + REST active tail | Định kỳ | `CryptoBinanceQuarterly1m`, `load_data("binance_usdm_quarterly_1m")` |
| Binance Spot BTCUSDT | `1m` | `BTCUSDT` từ `2018-01-01` | Binance Vision + REST tail; gap được fill bằng USD-M futures proxy khi đã duyệt | Định kỳ/live tail | `CryptoBinanceSpot1m`, `load_data("binance_spot_1m")` |
| Binance Daily Matrix | `1d` | Top/liquid USD-M perpetual symbols, policy chỉ thêm trong universe hợp lệ | Backfill từ `2020-01-01` | Hằng ngày `00:05 UTC` | `CryptoDailyMatrix`, `load_data("binance_daily_matrix", feature=...)` |
| Binance Futures Metrics | `5m` | `BTCUSDT`, `ETHUSDT`, relation symbols, active BTC/ETH quarterlies | Binance Vision `daily/metrics`, scan full coverage | Định kỳ cuối ngày/REST tail perpetual | `BinanceFuturesMetrics5m`, `load_data("binance_futures_metrics_5m")` |
| Binance Order Book Snapshot | `1h` | `BTCUSDT` perpetual + active BTCUSDT quarterlies | Rolling 30 ngày từ Vision `bookDepth` + REST current snapshot | Mỗi giờ | `BinanceOrderBookSnapshot1h`, `load_data("binance_orderbook_snapshot_1h")` |
| Binance Options Snapshot | `5m` | Options Binance theo cấu hình hiện tại | Snapshot incremental, append theo ngày | Mỗi 5 phút | `BinanceOptions5m`, `load_data("options_5m")` |
| Deribit BTC Options V1 | trades + `5m` snapshots | BTC inverse options compact-liquid universe | Deribit History API trades-only, gated by probe/pilot | Planned phased build | `DeribitOptionTrades`, `DeribitOptionSnapshots5m`, `load_data("deribit_option_trades")` |
| VN Equity Daily raw | `1d` | Universe VN curated khoảng 300 symbols | Provider VN daily, lưu partition theo symbol/year | Hằng ngày `16:30 Asia/Ho_Chi_Minh` | `VnStockDaily`, `load_data("vn_stock_daily")` |
| VN Daily Matrix | `1d` | VN equity universe + auxiliary `VN30F1M` benchmark column | Build từ canonical raw Parquet | Chạy builder khi cần sau raw daily update | `VNDailyMatrix`, `load_data("vn_daily_matrix", feature=...)` |
| VN Equity Intraday | `1m` | VN stock symbols trong config | Provider VN intraday | Hằng ngày `16:30 Asia/Ho_Chi_Minh` | `VnStock1m`, `load_data("vn_stock_1m")` |
| VN Futures Intraday | `1m` | `VN30F1M` và symbols futures configured | DNSE/VN provider | Hằng ngày `16:30 Asia/Ho_Chi_Minh` | `VnFutures1m`, `load_data("vn_futures_1m")` |
| VN30 Futures Contracts | `1m`, `1d` | Concrete contracts `VN30FYYMM` | KBS/DNSE source proof first; no canonical Phase E contract publish until that proof is accepted | Not enabled yet | `VnDerivativesContracts1m`, `VnDerivativesContractsDaily`, `load_data("vn_derivatives_contracts_1m")` |
| VN30 Futures Continuous Alias | `1m`, `1d` | `VN30F1M` | VNDIRECT DChart provider continuous alias; 1m has a separate Phase E source proof/backfill gate | Daily tail + 1m tail after its gate passes | `VnDerivativesContinuous1m`, `VnDerivativesContinuousDaily`, `load_data("vn30f1m_continuous_1m")` |

### Controlled Phase E expansion

Phase E is a non-Deribit, exact-command expansion. Its policy lives in
[`configs/primus_hmd_phase_e.yml`](configs/primus_hmd_phase_e.yml): configured
core USD-M perpetuals, the retained Binance order-book archive horizon, the
configured VN daily universe plus matrix, VNDIRECT `VN30F1M` continuous alias
at 1m, and a **non-publishing** KBS/DNSE contract source proof.

Historical jobs are never started through a generic collector command. After
the reviewed image has been built, use the allow-listed runner only:

```bash
tools/run_phase_e_service.sh phase-e-vn30f1m-vndirect-1m
```

It accepts only a named service in the policy and refuses concurrent Phase E
historical jobs. Matching live tails use `tools/run_phase_e_tail.sh`. Concrete
VN contract backfill and any contract-derived replacement of the alias remain
blocked until the source-proof artifact has positive usable coverage and a new
exact gate is recorded. Deribit is not part of Phase E.

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
│   └── futures/
│       ├── 1m/symbol=VN30F1M/year=YYYY/month=MM/part.parquet
│       ├── contracts/1m/symbol=VN30FYYMM/year=YYYY/month=MM/part.parquet
│       ├── contracts/1d/symbol=VN30FYYMM/year=YYYY/part.parquet
│       ├── continuous/1m/symbol=VN30F1M/version=v1/year=YYYY/month=MM/part.parquet
│       ├── continuous/1d/symbol=VN30F1M/version=v1/year=YYYY/part.parquet
│       └── rolls/version=v1/rolls.parquet
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

`VNDailyMatrix` có thể chứa auxiliary column `VN30F1M` để làm benchmark/regime/hedge. Từ Phase 3, matrix ưu tiên nguồn rebuilt continuous:

```text
storage/vn/futures/continuous/1d/symbol=VN30F1M/version=v1
```

Nếu continuous chưa có, builder mới fallback về legacy `storage/vn/futures/1d` hoặc aggregate từ `1m`. Metadata `state/vn_daily_matrix_symbols.json` ghi `auxiliary_sources` để service khác biết `VN30F1M` đang lấy từ đâu. `state/vn_daily_universe_report.csv.gz` và `state/vn_daily_matrix_symbols.json` tách `equity_symbols` khỏi `auxiliary_symbols`; không dùng `VN30F1M` trong cross-sectional equity ranking.

### VN30 Futures Derivatives V1

Contract-level storage dùng identity thật của từng hợp đồng `VN30FYYMM`, không dùng rolling alias làm key lịch sử. Schema canonical:

```text
time, instrument_id, open, high, low, close, volume, source, quality_flags, ingested_at
```

Continuous storage dùng một roll table chung cho cả `1m` và `1d`:

```text
storage/vn/futures/rolls/version=v1/rolls.parquet
```

Roll table schema:

```text
trading_date, series, old_instrument_id, new_instrument_id,
roll_reason, decision_date, old_close, new_close, roll_gap, roll_ratio
```

Quy ước series:

- `VN30F1M`: calendar front-month, giữ hợp đồng gần nhất đến hết phiên đáo hạn; phiên giao dịch kế tiếp chuyển sang hợp đồng tháng sau.
- `VN30F1M_TRADE`: liquidity-aware tradable series; chỉ dùng volume của các phiên đã đóng để quyết định roll, không dùng volume cùng ngày.
- `VN30F1M_PROVIDER`: không phải output canonical mới. Alias DNSE cũ chỉ còn vai trò validation/parity nếu dữ liệu legacy tồn tại.

VNDIRECT `VN30F1M` `1m`/`1d` là continuous alias trực tiếp từ provider,
được nhận diện bằng `source=vndirect_dchart` và
`quality_flags=CONTINUOUS_ALIAS`. Nó không mang `active_instrument_id` hoặc
roll metadata của một series xây lại từ concrete contracts. Consumer cần chọn
theo mục đích: alias dùng cho benchmark/regime; contract-aware accounting chỉ
được dùng khi contract source-proof và gate riêng đã pass.

Continuous schema có thêm metadata:

```text
time, symbol, open, high, low, close, volume,
active_instrument_id, roll_flag, roll_gap, roll_ratio,
source, quality_flags, ingested_at
```

Loader mặc định vẫn chỉ trả OHLCV để tiết kiệm RAM. Truyền `columns="full"` nếu cần `active_instrument_id`, `roll_flag`, `roll_gap`, `quality_flags`.

## Docker Services

Các service chạy bằng `docker compose` và có `restart: unless-stopped`.

| Service | Collector | Lịch/cơ chế |
| :--- | :--- | :--- |
| `crypto-1m-live` | `collectors.crypto_1m_live` | Cập nhật futures 1m liên tục |
| `binance-usdm-quarterly-1m` | `collectors.binance_usdm_quarterly_1m` | Sync quarterly historical/current |
| `binance-spot-1m` | `collectors.binance_spot_1m` | Sync BTCUSDT spot historical/current |
| `binance-daily-matrix` | `collectors.binance_daily_matrix` | Daily matrix lúc `00:05 UTC` |
| `binance-futures-metrics-5m` | `collectors.binance_futures_metrics_5m` | Metrics 5m theo lịch |
| `binance-orderbook-snapshot-1h` | `collectors.binance_orderbook_snapshot_1h` | BTC-only B0 REST snapshot mỗi giờ; preserves the shared 2,500-day orderbook retention horizon |
| `options-binance-5m` | `collectors.options_binance_5m` | Options snapshot mỗi 5 phút |
| `vn-daily` | `collectors.vn_daily` | VN daily raw lúc `16:30 Asia/Ho_Chi_Minh`; sau mỗi lượt update sẽ build universe report và rebuild daily matrix |
| `vn-intraday-stocks` | `collectors.vn_intraday_vnstock` | VN stock 1m lúc `16:30 Asia/Ho_Chi_Minh` |
| `vn30f1m-dnse` | `collectors.vn_intraday_dnse` | Legacy alias service; disabled khỏi default compose, chỉ chạy khi bật profile `legacy-vn30f1m-dnse` |
| `vn30f1m-vndirect-probe` | `collectors.vn_derivatives` | Hard-gated VNDIRECT DChart VN30F1M source proof; bootstrap/profile only, no publish |
| `vn30f1m-vndirect` | `collectors.vn_derivatives` | VNDIRECT DChart VN30F1M continuous daily sync lúc `16:30 Asia/Ho_Chi_Minh` |
| `phase-e-vn30f1m-vndirect-1m` | `collectors.vn_derivatives` | One-shot source-proof + bounded VNDIRECT 1m backfill; profile `phase-e`, never starts by default |
| `vn30f1m-vndirect-1m` | `collectors.vn_derivatives` | Explicit 1m VNDIRECT tail, only after the Phase E one-shot audit passes |
| `phase-e-vn-daily-universe-1d` | `collectors.vn_daily` | One-shot configured VN raw daily historical rebuild, then universe report + matrix; an interrupted approved run resumes only symbols with a recorded post-run success checkpoint. Explicit VCI no-data responses are retained as source evidence, while HTTP/schema/rate-limit errors still fail the gate. |
| `phase-e-vn-daily-matrix-rebuild` | `collectors.vn_daily_matrix` | Derived-only, exact-gated rebuild from canonical VN raw Parquet and continuous-first `VN30F1M`; it makes no provider call and does not alter raw history. |
| `phase-e-binance-usdm-core-perpetual-1m` | `collectors.binance_usdm_perpetual_1m` | One-shot ETH/SOL/BNB/DOGE USD-M archive rebuild with source-listing-aware audit |
| `phase-e-binance-orderbook-history-1h` | `collectors.binance_orderbook_snapshot_1h` | One-shot retained-horizon Vision + REST history for BTC perpetual/current/next quarterlies |
| `phase-e-vn30-contract-source-probe` | `collectors.vn_derivatives` | Non-publishing KBS/DNSE representative proof; it is not a contract backfill approval |
| `crypto-1m-core-live`, `binance-usdm-quarterly-next-1m`, `binance-orderbook-expanded-1h` | scoped collectors | Phase E tails, each independently approved and never a default-universe expansion; the expanded orderbook tail retains the approved 2,500-day archive horizon while using REST only |
| `vn-derivatives-probe` | `collectors.vn_derivatives` | Probe KBS/DNSE individual VN30 futures contracts; bootstrap/profile only |
| `vn-derivatives-source-probe` | `collectors.vn_derivatives` | Historical V2 multi-source proof; superseded for VN30F1M by VNDIRECT DChart |
| `vn-derivatives-bootstrap` | `collectors.vn_derivatives` | Backfill individual VN30 futures contracts; bootstrap/profile only |
| `vn-derivatives-validate` | `collectors.vn_derivatives` | Validate contract-level VN30 futures storage |
| `vn-derivatives` | `collectors.vn_derivatives` | Daily sync contracts, validate, rebuild continuous, compare provider alias, update VN matrix |

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

# Deribit trades staging downloader. Broad backfill/sync bị chặn nếu Phase 5 pilot chưa pass.
PYTHONPATH=. python -m collectors.deribit_option_trades backfill --version v1 --max-tasks 1 --json
PYTHONPATH=. python -m collectors.deribit_option_trades sync-once --version v1 --max-tasks 1 --json

# Targeted pilot sampling khi pilot đang blocked: bắt buộc explicit symbols và max-tasks<=20.
PYTHONPATH=. python -m collectors.deribit_option_trades backfill --version v1 --symbols BTC-25SEP26-115000-C --max-tasks 1 --allow-blocked-pilot --json

# Deribit staging -> canonical daily Parquet, validation, repair planning, cleanup guard
PYTHONPATH=. python -m collectors.deribit_option_trades compact --version v1 --max-days 1 --json
PYTHONPATH=. python -m collectors.deribit_option_trades validate --version v1 --json
PYTHONPATH=. python -m collectors.deribit_option_trades repair --version v1 --only-unresolved --json
PYTHONPATH=. python -m collectors.deribit_option_trades cleanup --version v1 --json

# Deribit pilot benchmark acceptance gate trước full historical backfill
PYTHONPATH=. python -m collectors.deribit_option_trades pilot --version v1 --json

# VN daily matrix rebuild thủ công để debug. Production đi qua service vn-daily live schedule.
PYTHONPATH=. python -m collectors.vn_daily_matrix --start-date 2016-01-01

# VN30 futures derivatives V1 probe. Không publish canonical bars; dùng để xác nhận coverage trước backfill.
PYTHONPATH=. python -m collectors.vn_derivatives discover --json
PYTHONPATH=. python -m collectors.vn_derivatives probe --json

# VN30F1M VNDIRECT DChart source proof. Không publish canonical bars.
# Command fail nếu recent 1m hoặc daily không có positive rows thật.
PYTHONPATH=. python -m collectors.vn_derivatives probe-vndirect --json

# VN30F1M VNDIRECT DChart daily sync. Ghi daily continuous alias.
PYTHONPATH=. python -m collectors.vn_derivatives sync-vndirect --resolution 1d --mode once --update-matrix --json

# VNDIRECT 1m history must be run through the Phase E exact-service runner,
# not this low-level module command. It writes source-proof and audit evidence.
tools/run_phase_e_service.sh phase-e-vn30f1m-vndirect-1m

# Historical multi-source proof, superseded by VNDIRECT DChart for the active VN30F1M task.
PYTHONPATH=. python -m collectors.vn_derivatives probe-free-sources --json

# VN30 futures individual contracts V1.
PYTHONPATH=. python -m collectors.vn_derivatives backfill --start 2017-08-10 --resolutions 1m,1d --max-contracts 1 --max-windows 2 --json
PYTHONPATH=. python -m collectors.vn_derivatives validate --json

# VN30 futures continuous + matrix integration.
PYTHONPATH=. python -m collectors.vn_derivatives build-continuous --start 2017-08-10 --resolutions 1m,1d --json
PYTHONPATH=. python -m collectors.vn_derivatives validate-continuous --json
PYTHONPATH=. python -m collectors.vn_derivatives compare-provider --json
PYTHONPATH=. python -m collectors.vn_derivatives update-matrix --json
PYTHONPATH=. python -m collectors.vn_derivatives sync-once --json
```

Luồng production cho VN daily là container `vn-daily`, không phải chạy host command rời. Service này tự chạy cuối ngày, append/dedupe raw daily, ghi `state/vn_daily_universe_report.csv.gz`, đọc auxiliary `VN30F1M` theo continuous-first policy, rồi rebuild `VNDailyMatrix`.

Luồng VN derivatives production là `vn-derivatives`. Daily workflow:

```text
sync recent concrete contracts
→ validate contract storage
→ merge/update shared roll table
→ rebuild affected continuous partitions
→ validate continuous storage
→ compare rebuilt VN30F1M với legacy/provider alias nếu có overlap
→ rebuild VN Daily Matrix
```

`vn-derivatives-bootstrap` dùng cho warmup/backfill dài. `vn30f1m-dnse` đã chuyển sang profile legacy để tránh hai process cùng ghi alias `VN30F1M`.

`vn-derivatives-bootstrap` giữ strict mode: provider error không có usable rows sẽ fail-fast và không mark completed. `vn-derivatives live/sync-once` chạy best-effort: window lỗi provider hoặc empty 0 rows được ghi vào manifest `last_error`, không ghi `completed_windows`, service tiếp tục validate/build phần dữ liệu có sẵn và trả status `warning` để lần sau tự retry.

V2 source proof ghi:

```text
state/vn_derivatives/source_probe_v2.parquet
state/vn_derivatives/source_probe_v2.json
state/vn_derivatives/source_status.json
```

Hard gate V2: HTTP 400/403/429/5xx không bao giờ được coi là `empty_confirmed`; provider chỉ được promote khi có `status=success` và `row_count > 0`. `xnoapi` không nằm trong default V2 probe vì package/repo quant yêu cầu API key; Phase 1 ưu tiên public/free web sources như Vietstock và TradingView.

Vietstock Phase 1 có 2 lớp proof: public search `/search/{query}/3` để xác nhận symbol phái sinh tồn tại, rồi public page/table check để tìm OHLC. Search hit không phải OHLC data, nên không được promote provider nếu `row_count=0`.

Deribit Phase 6 backfill vận hành bằng Docker one-shot jobs, không chạy raw shell dài trong chat. Job backfill có progress log dạng `deribit_task_start`, `deribit_task_done`, `deribit_backfill_done`, đọc được bằng `docker compose logs -f deribit-option-backfill-2022`.

```bash
# Chạy nền một batch đến expiry 2022, xong tự compact/validate/cleanup rồi exit.
DERIBIT_BACKFILL_MAX_TASKS=500 DERIBIT_PROGRESS_EVERY=25 \
docker compose --profile deribit up -d deribit-option-cycle-2022

# Xem tiến trình nền.
docker compose --profile deribit logs -f deribit-option-cycle-2022

# Backfill thử đoạn history có expiry đến hết 2022, rồi dừng theo batch checkpoint.
DERIBIT_BACKFILL_MAX_TASKS=500 DERIBIT_PROGRESS_EVERY=25 \
docker compose --profile deribit run --rm deribit-option-backfill-2022

# Sau mỗi backfill batch: compact -> validate -> repair -> cleanup staging đã validate.
docker compose --profile deribit run --rm deribit-option-compact
docker compose --profile deribit run --rm deribit-option-validate
docker compose --profile deribit run --rm deribit-option-repair
docker compose --profile deribit run --rm deribit-option-cleanup

# Chỉ dùng sau khi đoạn 2022 ổn định.
DERIBIT_BACKFILL_MAX_TASKS=500 DERIBIT_PROGRESS_EVERY=25 \
docker compose --profile deribit-full run --rm deribit-option-backfill-full

# Chạy nền full-history một batch, xong tự compact/validate/cleanup rồi exit.
DERIBIT_BACKFILL_MAX_TASKS=500 DERIBIT_PROGRESS_EVERY=25 \
docker compose --profile deribit-full up -d deribit-option-cycle-full

docker compose --profile deribit-full logs -f deribit-option-cycle-full
```

`deribit-option-backfill-2022` filter theo `--expiry-end 2022-12-31`; nó không đoán bằng số task. Nếu container bị dừng, SQLite checkpoint resume từ `last_processed_seq`; nếu API treo ở một request, Docker logs vẫn cho biết đang ở `instrument/start_seq/end_seq` nào.

Checkpoint Deribit lưu staging path dạng portable `storage/...`, không phụ thuộc host path `/root/...` hay container path `/app/...`. Sau mỗi batch backfill phải chạy đủ `compact -> validate -> cleanup`; cleanup chỉ xóa staging khi validate `status=ok` và ghi `staging_cleanup_manifest.json`, vì vậy validate sau cleanup vẫn audit được checkpoint. Các service `deribit-option-cycle-*` đã tự chạy đủ chain này rồi exit; dùng lại cùng command sau này sẽ tiếp tục từ SQLite checkpoint, không backfill lại từ đầu.

Deribit historical trades có thể thiếu source field `contracts`; loader chuẩn hóa `amount` thành `amount_base`, và ingestion/repair sẽ derive `contracts = amount_base / contract_size` khi source thiếu. Flag `MISSING_CONTRACTS` vẫn được giữ để audit rằng giá trị này là derived, không phải source-native.

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
    VnDerivativesContracts1m,
    VnDerivativesContractsDaily,
    VnDerivativesContinuous1m,
    VnDerivativesContinuousDaily,
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
| `VnDerivativesContracts1m` | `vn_derivatives_contracts_1m`, `vn30_contracts_1m` | Long OHLCV concrete VN30 futures contracts |
| `VnDerivativesContractsDaily` | `vn_derivatives_contracts_1d`, `vn30_contracts_1d` | Long OHLCV concrete VN30 futures contracts |
| `VnDerivativesContinuous1m` | `vn_derivatives_continuous_1m`, `vn30_continuous_1m`, `vn30f1m_continuous_1m` | Long OHLCV rebuilt continuous VN30 futures |
| `VnDerivativesContinuousDaily` | `vn_derivatives_continuous_1d`, `vn30_continuous_1d`, `vn30f1m_continuous_1d` | Long OHLCV rebuilt continuous VN30 futures |
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

- `symbols`: string, list string, hoặc `None` để dùng default discovery đúng
  semantic của từng loader. Riêng shared Binance USD-M 1m xem contract bên dưới.
- `start_date`, `end_date`: inclusive datetime filter.
- `limit`: giới hạn số dòng sau khi sort.
- `check_val`: mặc định `True`; không tự tắt validation trong service downstream.
- `columns`: với OHLCV loaders, mặc định chỉ đọc `time/symbol/open/high/low/close/volume` để giảm RAM. Truyền `columns="full"` nếu cần toàn bộ schema như `source`, `ingested_at`, `quote_volume`.

### Binance USD-M 1m default-discovery contract

Perpetual và concrete quarterly cùng dùng physical root
`storage/crypto/binance_futures/1m`. Vì vậy từ compatibility patch này:

- `CryptoBinance1m().load()` hoặc `load_data("crypto_1m")` khi không truyền
  `symbols` chỉ discover perpetual symbols (không có suffix contract).
- `CryptoBinanceQuarterly1m().load()` hoặc
  `load_data("binance_usdm_quarterly_1m")` khi không truyền `symbols` chỉ
  discover concrete contract symbols có dạng `PAIR_YYMMDD`.
- `symbols=` explicit vẫn được giữ nguyên pass-through để không làm vỡ các
  consumer cũ. Tuy vậy service/backtest production nên luôn truyền danh sách
  symbol cụ thể để kết quả reproducible, đặc biệt khi contract mới được thêm.

Không có schema, router name, storage path, hay user/service endpoint nào đổi
trong patch này; chỉ default discovery được tách đúng theo semantic loader.

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

Phase 3 `backfill`/`sync-once` tải trade chunks vào immutable staging Parquet, chưa compact sang canonical trades. Sau Phase 5, broad backfill/sync yêu cầu `pilot_summary.json` status `ok`; khi pilot đang blocked, chỉ cho phép targeted sampling với `--allow-blocked-pilot`, explicit `--symbols`, và `--max-tasks<=20`. `sync-once` refresh discovery trước khi lập task; `backfill` chỉ refresh khi truyền `--discover-first`. Mỗi chunk đi theo thứ tự: API success -> normalize/filter -> write temp parquet -> atomic rename/fsync -> commit `download_ranges` -> advance `instrument_state`. API error/unknown chỉ tăng retry state, không advance cursor.

Phase 4 `compact` dùng DuckDB memory/temp limits để publish canonical daily Parquet. `validate` kiểm coverage ledger, checksum staging, canonical schema, duplicate key, basic finance fields, và FK sang instrument dimension. `cleanup` mặc định dry-run; chỉ xoá staging khi validate pass và có `--confirm`.

Phase 5 `pilot` ghi ba report deterministic cho low/normal/high volatility windows và `pilot_summary.json`. Command này không chạy full history; nếu ba window chưa có representative samples hoặc acceptance chưa pass thì status là `blocked` để chặn Phase 6.
- `timeframe`: khi gọi qua `load_data`, truyền `timeframe="5min"`/`"15min"`/`"1h"` để dùng endpoint resample-on-read.
- `feature`: bắt buộc khi dùng router cho matrix datasets.

Raw `1m` vẫn là canonical storage. Endpoint resample không tạo thêm timeframe file mặc định; nó query Parquet 1m bằng DuckDB, chỉ đọc OHLCV columns, aggregate trước rồi mới trả về Pandas. Nếu DuckDB không khả dụng, loader fallback sang Pandas chunk theo partition.

### Common Examples

```python
from data_loader import CryptoBinance1m, CryptoDailyMatrix, VNDailyMatrix, VnDerivativesContinuousDaily, load_data

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

# VN daily OHLCV data_dict. VN30F1M là auxiliary, chỉ dùng làm benchmark/hedge.
vn_daily = VNDailyMatrix().load_ohlcv(
    symbols=["FPT", "VCB", "HPG", "VN30F1M"],
    start_date="2018-01-01",
    check_val=True,
)

# VN daily close matrix qua router.
vn_close = load_data(
    "vn_daily_matrix",
    feature="close",
    symbols=["FPT", "VCB", "VN30F1M"],
    start_date="2018-01-01",
    check_val=True,
)

# VN30 futures concrete contract, không phải rolling alias.
vn30_contract = load_data(
    "vn_derivatives_contracts_1m",
    symbols="VN30F2508",
    start_date="2025-08-01",
    check_val=True,
)

# VN30 rebuilt continuous daily. Mặc định chỉ đọc OHLCV.
vn30_continuous = VnDerivativesContinuousDaily().load(
    symbols="VN30F1M",
    start_date="2018-01-01",
    check_val=True,
)

# Khi cần kiểm tra roll/provenance.
vn30_roll_meta = load_data(
    "vn_derivatives_continuous_1d",
    symbols="VN30F1M",
    columns="full",
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
- [implementation_plan.md](implementation_plan.md): consolidated job/phase tracker, including VN Daily Universe upgrade.
