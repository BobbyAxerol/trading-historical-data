# VN30 FUTURES FREE DATA UPGRADE PLAN V2

> **Repository:** `BobbyAxerol/trading-historical-data`, branch `dev`  
> **Mục tiêu:** mở rộng lịch sử VN30 futures bằng các nguồn miễn phí, ưu tiên daily từ năm 2017 và thử kéo continuous 1m dài nhất có thể.  
> **Nguyên tắc:** không coi một provider là usable chỉ vì SDK hoặc website có hỗ trợ phái sinh; chỉ publish khi probe trả bars thật, có `row_count`, `first_bar`, `last_bar` và validation pass.

---

## 1. Kết luận kiến trúc

Tách bài toán thành hai dataset độc lập:

### 1.1 Daily contract-aware dataset

Mục tiêu:

```text
individual VN30 futures contracts
→ validate
→ roll table
→ continuous daily
```

Nguồn ưu tiên:

```text
Vietstock public web/XHR    primary candidate
XNO continuous daily       validation/fill candidate
TradingView continuous     validation/fill candidate
KBS                        opportunistic fallback
DNSE                       opportunistic fallback
```

Daily là deliverable bắt buộc. Mục tiêu coverage:

```text
2017-08-10 → hiện tại
```

### 1.2 Continuous 1m dataset

Không yêu cầu individual-contract 1m nếu nguồn free không có.

Nguồn ưu tiên:

```text
XNO VN30F1M 1m             primary candidate
existing DNSE/provider 1m  canonical tail hiện có từ 2024
TradingView/VNDIRECT       optional public-chart fallback
KBS/DNSE concrete          chỉ dùng nếu positive probe
```

1m là best-effort:

```text
earliest positive bar thực tế → hiện tại
```

Không để thiếu 1m chặn việc hoàn thành daily.

---

## 2. Bằng chứng hiện có và mức độ tin cậy

### 2.1 Vietstock

Trang công khai của expired contract như `VN30F1709` vẫn tồn tại và có:

- Ngày giao dịch đầu/cuối.
- Daily open/close.
- Volume.
- OI.
- Trang thống kê giao dịch.

Điều này chứng minh Vietstock còn biết individual expired contracts. Tuy nhiên phải probe endpoint/pagination để xác nhận có thể lấy **toàn bộ daily history**, không chỉ vài rows hiển thị.

Status:

```text
INDIVIDUAL DAILY: STRONG CANDIDATE
FULL EXTRACTION:  MUST PROBE
INDIVIDUAL 1M:    UNPROVEN
```

### 2.2 XNO API

Tài liệu công khai của `xnoapi`:

```python
get_derivatives_hist("VN30F1M", "1m")
```

hỗ trợ:

```text
1m, 5m, 15m, 30m, 1H, 1D
```

Ví dụ công khai hiển thị các bars `VN30F1M` 1m bắt đầu từ:

```text
2018-08-13 09:01:00
```

Đây là bằng chứng mạnh hơn KBS/DNSE, nhưng vẫn phải chạy probe bằng version cài trên VPS để xác nhận:

- Coverage hiện tại có còn đầy đủ không.
- Hàm có trả toàn dataset hay sample giới hạn.
- Timezone và session.
- Dữ liệu là continuous alias hay individual contract.
- Volume có đúng đơn vị hợp đồng không.

Status:

```text
CONTINUOUS 1M: STRONGEST FREE CANDIDATE
CONTINUOUS 1D: STRONG CANDIDATE
INDIVIDUAL CONTRACTS: NOT EXPECTED
```

### 2.3 TradingView

TradingView công khai:

```text
HNX:VN301!
```

là continuous VN30 index futures và có trang danh sách individual contracts.

TradingView phù hợp cho:

- Daily continuous validation.
- Roll-date comparison.
- Optional public chart endpoint probe.
- Bổ sung continuous history nếu endpoint công khai trả đủ.

Không coi TradingView là nguồn production mặc định trước khi chứng minh:

- Có endpoint truy cập ổn định.
- Có historical pagination.
- Có thể lấy dữ liệu không cần bypass login/CAPTCHA.
- Terms/rate limits cho phép cách sử dụng.

Status:

```text
CONTINUOUS DAILY: GOOD VALIDATION CANDIDATE
CONTINUOUS 1M:    MAYBE
INDIVIDUAL DAILY: MAYBE
```

### 2.4 KBS và DNSE

Giữ code hiện tại nhưng hạ vai trò:

```text
KBS  = fallback only after positive bars
DNSE = tail/current validation and fallback
```

Không tiếp tục full backfill concrete contracts bằng KBS/DNSE khi probe chưa có row thật.

---

## 3. Dataset outputs

Không overwrite series hiện tại ngay.

### 3.1 Contract daily

```text
storage/vn/futures/contracts/1d/
└── symbol=VN30F1709/
    └── year=2017/
        └── part.parquet
```

### 3.2 Free continuous daily

Nhanh chóng build từ longest continuous source:

```text
storage/vn/futures/continuous/1d/
└── symbol=VN30F1M_FREE/
    └── version=v1/
```

Nguồn có thể là:

```text
XNO
→ TradingView fill
→ existing provider fill
```

Dataset này dùng cho:

- Regime.
- Benchmark.
- Portfolio hedge proxy.
- Cross-sectional feature.

Không dùng để khẳng định exact roll execution.

### 3.3 Contract-rebuilt continuous daily

```text
storage/vn/futures/continuous/1d/
└── symbol=VN30F1M_CONTRACT/
    └── version=v1/
```

Được dựng từ individual contracts và roll table.

Dataset này dùng cho:

- Futures accounting.
- Explicit roll cost.
- Short execution backtest.
- Contract-aware P&L.

### 3.4 Continuous 1m

```text
storage/vn/futures/continuous/1m/
└── symbol=VN30F1M/
    └── version=v2_free/
```

Merge longest free sources nhưng giữ provenance từng row.

### 3.5 Existing provider series

Giữ nguyên để validation:

```text
VN30F1M_PROVIDER
2024-05-02 → hiện tại
```

Sau khi validation pass mới đổi loader alias `VN30F1M`.

---

# PHASE 1 — SOURCE PROOF VÀ FREE PROVIDER INTEGRATION

## 4. Bắt buộc sửa probe contract

`collectors/vn_derivatives/probe.py` hiện chỉ hỗ trợ KBS/DNSE và coi DataFrame empty sau success là `empty_confirmed`.

Cần chuyển sang typed result:

```python
from dataclasses import dataclass
from typing import Literal

ProviderStatus = Literal[
    "success",
    "empty_confirmed",
    "unsupported_symbol",
    "invalid_request",
    "auth_error",
    "rate_limited",
    "blocked",
    "schema_error",
    "unknown_error",
]

@dataclass(frozen=True)
class ProviderFetchResult:
    provider: str
    canonical_symbol: str
    requested_symbol: str
    resolved_symbol: str | None
    resolution: str
    status: ProviderStatus
    rows: object
    http_status: int | None
    first_bar: object | None
    last_bar: object | None
    error: str | None
```

Không được convert:

```text
HTTP 400 → empty confirmed
```

`EMPTY_CONFIRMED` chỉ khi:

```text
HTTP/request success
schema hợp lệ
bars=[]
```

## 5. Provider registry mới

```text
collectors/providers/
├── xno_derivatives.py
├── vietstock_derivatives.py
├── tradingview_derivatives.py
├── kbs_derivatives.py
└── dnse_derivatives.py
```

Registry:

```python
PROVIDERS = {
    "xno": XnoDerivativesProvider(),
    "vietstock": VietstockDerivativesProvider(),
    "tradingview": TradingViewDerivativesProvider(),
    "kbs": KbsDerivativesProvider(),
    "dnse": DnseDerivativesProvider(),
}
```

Mỗi provider phải trả cùng `ProviderFetchResult`.

---

## 6. XNO probe

### 6.1 Smoke tests

```python
from xnoapi.vn.data import get_derivatives_hist

for resolution in ["1m", "5m", "1H", "1D"]:
    df = get_derivatives_hist("VN30F1M", resolution)
```

Báo cáo:

```text
installed_version
resolution
row_count
first_bar
last_bar
columns
timezone
duplicate_rows
invalid_ohlc_rows
session_outside_rows
median_bar_interval
volume_min
volume_max
```

### 6.2 Acceptance

`XNO_1M_POSITIVE` khi:

```text
row_count > 1000
first_bar < 2024-05-02
last_bar gần hiện tại
OHLC hợp lệ
session/timezone xác định được
không chỉ trả sample ngắn
```

`XNO_1D_POSITIVE` khi:

```text
row_count > 500
first_bar <= 2018-08-13 hoặc sớm hơn
daily timestamps unique
```

### 6.3 Không giả định date filters

Nếu API trả full dataset không có `start/end`:

- Call một lần.
- Ghi disk ngay.
- Không gọi lại từng window.
- Dùng checksum/version để phát hiện update.

Nếu API có giới hạn rows:

- Inspect source package.
- Xác định pagination/cursor.
- Probe exact maximum coverage.

### 6.4 XNO storage

```text
storage/_staging/vn/futures/xno/
└── VN30F1M/
    ├── resolution=1m/
    └── resolution=1d/
```

Sau validation:

```text
storage/vn/futures/provider_series/
└── provider=xno/
    └── symbol=VN30F1M/
```

---

## 7. Vietstock daily extraction probe

### 7.1 Contract samples

```text
VN30F1709    early market
VN30F2003    middle history
VN30F2406    pre-KRX
VN30F2508    post-KRX example
current active contract
```

### 7.2 Discovery order

1. GET contract overview page.
2. Parse:
   - First trading date.
   - Last trading date.
   - Expiry date.
   - Summary statistics.
3. Open trading-statistics page.
4. Inspect public XHR/fetch or HTML pagination.
5. Replay public request bằng `httpx`.
6. Chỉ dùng Playwright nếu browser bootstrap thực sự cần.

### 7.3 Scraping constraints

Cho phép:

- Public page.
- Public JSON/XHR.
- Session cookie nhận từ public GET.
- Normal pagination.
- Rate limiting.
- Local response cache.

Không làm:

- Bypass CAPTCHA.
- Bypass authentication/paywall.
- Giả mạo account.
- Né rate limit.
- Reverse-engineer private credential.

Nếu bị block:

```text
status = BLOCKED
```

và chuyển sang source khác.

### 7.4 Probe report

```text
state/vn_derivatives/vietstock_probe_v1.parquet
```

Fields:

```text
canonical_symbol
page_url
endpoint_type
resolved_request
http_status
row_count
first_bar
last_bar
expected_first_date
expected_last_date
coverage_ratio
requires_browser
requires_cookie
blocked
error
```

### 7.5 Positive proof

Vietstock được promote thành daily primary chỉ khi:

```text
ít nhất 4/5 sample contracts có full daily rows
coverage_ratio >= 0.95
first/last dates hợp lý
OHLCV schema ổn định
```

---

## 8. TradingView probe

### 8.1 Symbols

```text
HNX:VN301!    continuous
individual contracts từ contracts page
```

### 8.2 Probe mục tiêu

Thử:

```text
daily:
    2017
    2020
    2024
    current

1m:
    2018
    2020
    2022
    2024
    current
```

### 8.3 Chỉ tích hợp khi public-accessible

Production adapter chỉ được dùng khi data có thể truy xuất bằng:

- Public chart response.
- Public WebSocket/session không cần bypass controls.
- Public downloadable data.

Nếu cần logged-in session hoặc phá giới hạn giao diện, không đưa vào automated service.

### 8.4 Vai trò

Ngay cả khi probe pass:

```text
TradingView = continuous validation/fill
```

Không ưu tiên hơn Vietstock individual contracts cho roll accounting.

---

## 9. KBS/DNSE probe tối thiểu

Không bỏ hẳn nhưng không scan toàn lịch sử.

Chỉ test:

```text
one old expired contract
one pre-KRX recent contract
one post-KRX contract
one active contract
```

Cả legacy và KRX symbol.

Nếu không có row thật:

```text
provider_status = DISABLED_FOR_BACKFILL
```

Service vẫn có thể dùng alias/current tail nếu positive.

---

## 10. Probe hard gates

Mỗi run phải ghi:

```text
expected_request_count
actual_request_count
positive_request_count
blocked_request_count
error_request_count
```

Fail exit code nếu:

```text
actual_request_count != expected_request_count
```

Không cho build sâu nếu:

```text
positive_request_count == 0
```

Provider status:

```text
UNVERIFIED
POSITIVE_PARTIAL
VALIDATED
DISABLED
```

---

# PHASE 2 — BUILD DAILY, 1M VÀ CONTINUOUS SERIES

## 11. Daily quick path: free continuous

Nếu XNO daily hoặc TradingView daily positive:

```text
download longest continuous daily
→ normalize
→ validate
→ merge existing provider tail
→ publish VN30F1M_FREE
```

Row priority:

```text
XNO primary nếu coverage dài và parity tốt
TradingView fill exact missing dates
existing provider fill recent tail
```

Không average OHLC.

Mỗi row giữ:

```text
source
source_symbol
quality_flags
```

`VN30F1M_FREE` có thể đưa vào Daily Matrix ngay sau validation với:

```text
asset_type = future
eligible_for_equity_ranking = false
execution_quality = research_proxy
```

---

## 12. Individual daily backfill

Khi Vietstock positive:

### 12.1 Canonical identity

Luôn dùng:

```text
VN30FYYMM
```

KRX/legacy chỉ là provider mapping.

Instrument dimension tối thiểu:

```text
instrument_id
canonical_symbol
legacy_symbol
krx_symbol
expiry_date
first_trading_date
last_trading_date
vietstock_symbol
source_status
```

### 12.2 Storage

```text
storage/vn/futures/contracts/1d/
└── symbol=VN30FYYMM/
    └── year=YYYY/
        └── part.parquet
```

Schema:

```text
time
instrument_id
open
high
low
close
volume
open_interest
settlement_price
source
quality_flags
ingested_at
```

Fields không có thì null, không tự tạo giả.

### 12.3 Backfill behavior

- Process từng contract.
- Ghi atomically.
- Resume theo contract.
- Cache raw HTML/JSON trong staging chỉ tới khi validation pass.
- Xóa raw response sau publish nếu không cần audit dài hạn.
- Không giữ browser screenshots.

### 12.4 Validation

```text
unique trading date
high >= open/close/low
low <= open/close/high
volume >= 0
no row after final trading date
first/last date consistent with metadata
```

Không tính ngày trước listing là missing.

---

## 13. Build roll table

```text
storage/vn/futures/rolls/version=v1/rolls.parquet
```

Hai policies:

### 13.1 Calendar F1M

```text
giữ front contract đến hết phiên đáo hạn
chuyển từ phiên tiếp theo
```

Dùng để parity với alias F1M.

### 13.2 Tradable F1M

Roll khi:

```text
next volume > current volume
trong hai phiên đã đóng liên tiếp
```

Hard roll:

```text
không muộn hơn phiên trước expiry
```

Decision cho ngày `t` chỉ dùng data đến `t-1`.

Roll schema:

```text
trading_date
series
old_instrument_id
new_instrument_id
decision_date
roll_reason
old_close
new_close
roll_gap
roll_ratio
```

---

## 14. Contract-rebuilt daily continuous

```text
VN30F1M_CONTRACT
VN30F1M_TRADE
```

Persistent columns:

```text
time
open
high
low
close
volume
active_instrument_id
roll_flag
roll_gap
source
quality_flags
```

Back-adjusted prices tính on load.

Không dùng back-adjusted price làm fill.

### 14.1 Promotion rule

Chỉ promote loader alias:

```text
VN30F1M → VN30F1M_CONTRACT
```

khi:

```text
coverage bắt đầu gần 2017-08-10
contract gaps được report
overlap parity với free/provider continuous pass
roll table pass look-ahead tests
```

Trước đó:

```text
VN30F1M_FREE = research default
VN30F1M_PROVIDER = old validation series
```

---

## 15. Continuous 1m merge

### 15.1 Source priority

Sau probe, không hard-code trước.

Ví dụ nếu XNO pass:

```text
XNO                 primary historical
existing DNSE       primary recent tail hoặc validation
TradingView/VNDIRECT fill exact gaps nếu public endpoint pass
```

### 15.2 Merge rules

Key:

```text
time
```

Rules:

- Không average prices.
- Primary source thắng khi cả hai có row.
- Secondary chỉ fill exact missing timestamps.
- Không forward-fill missing minutes.
- Lưu provenance per row.
- Giữ conflict report.

### 15.3 Canonical timezone/session

```text
Asia/Ho_Chi_Minh
```

Loại:

- Weekends.
- Exchange holidays nếu có calendar.
- Out-of-session rows.
- Duplicate timestamp.

Không assume session cũ và KRX session giống nhau tuyệt đối; validate theo từng regime/date.

### 15.4 1m acceptance

```text
first_bar xác nhận
last_bar gần hiện tại
OHLC valid
duplicate = 0
no unexplained timezone shift
daily aggregation parity hợp lý
```

Nếu XNO thực sự bắt đầu `2018-08-13`, publish đúng ngày đó; không fabricate phần `2017–2018`.

---

## 16. Daily aggregation từ 1m

Chỉ dùng như fallback:

```text
open   = first valid open
high   = max high
low    = min low
close  = last valid close
volume = sum
```

Daily row chỉ publish nếu session completeness đạt threshold.

Không dùng incomplete intraday session để overwrite direct daily data.

---

## 17. VN Daily Matrix integration

Matrix columns đề xuất:

```text
VN30F1M_FREE
VN30F1M_CONTRACT
VN30F1M_PROVIDER
```

Sau promotion có thể expose convenience alias:

```text
VN30F1M
```

Metadata tối thiểu:

```text
symbol
asset_type
source_dataset
series_type
first_valid_date
last_valid_date
eligible_for_equity_ranking
quality_status
```

Rules:

```text
eligible_for_equity_ranking = false
```

cho mọi futures series.

Portfolio strategy có thể dùng:

- `VN30F1M_FREE`: regime/proxy.
- `VN30F1M_CONTRACT`: shortable accounting.
- `VN30F1M_PROVIDER`: parity/debug.

---

## 18. Adapt với code hiện tại

### 18.1 `probe.py`

- Provider registry thay cho hard-code KBS/DNSE.
- Typed statuses.
- Expected request count.
- Positive proof gate.
- Không đổi HTTP 400 thành empty.

### 18.2 `contracts.py`

Provider order cho daily:

```text
vietstock
→ kbs positive only
→ dnse positive only
→ aggregate 1m
```

Không gọi provider đã status `DISABLED`.

### 18.3 Module mới

```text
collectors/vn_derivatives/
├── alias_series.py
├── provider_registry.py
├── source_gates.py
├── web_cache.py
└── parity.py
```

`alias_series.py` xử lý XNO/TradingView continuous, tách khỏi individual contract pipeline.

### 18.4 State

```text
state/vn_derivatives/
├── source_probe_v2.parquet
├── source_probe_v2.json
├── source_status.json
├── vietstock_contracts.json
├── xno_alias.json
├── tradingview_alias.json
├── continuous_1d_v2.json
├── continuous_1m_v2.json
└── parity_report.json
```

---

## 19. Services/container flow

Không cho experimental scraper chạy trong main service trước probe.

### 19.1 One-shot source proof

```yaml
vn-derivatives-source-probe:
  profiles: ["bootstrap"]
  restart: "no"
  command:
    - python
    - -m
    - collectors.vn_derivatives
    - probe-free-sources
```

### 19.2 Daily historical bootstrap

```yaml
vn-derivatives-daily-backfill:
  profiles: ["bootstrap"]
  restart: "no"
  command:
    - python
    - -m
    - collectors.vn_derivatives
    - backfill-daily-free
```

### 19.3 Continuous alias 1m bootstrap

```yaml
vn-derivatives-1m-backfill:
  profiles: ["bootstrap"]
  restart: "no"
  command:
    - python
    - -m
    - collectors.vn_derivatives
    - backfill-alias-1m
```

### 19.4 Main incremental service

Sau source validation:

```yaml
vn-derivatives:
  command:
    - python
    - -m
    - collectors.vn_derivatives
    - live
```

Daily flow:

```text
sync XNO/continuous source
→ scrape/update active Vietstock contract daily
→ optional KBS/DNSE tail
→ validate
→ rebuild affected continuous tail
→ update daily matrix
→ write parity/storage report
```

---

## 20. Rate limit và web-cache policy

Default:

```yaml
web:
  max_requests_per_second: 0.5
  concurrency: 1
  retry_attempts: 5
  cache_raw_responses: true
  cache_ttl_days: 7
  user_agent_identifies_project: true
```

Backfill vài trăm daily contracts không cần chạy nhanh.

Ưu tiên:

```text
ổn định
→ không bị block
→ resume được
→ có checksum
```

hơn concurrency cao.

---

## 21. Cleanup

Sau canonical publish và validation pass:

```text
delete temporary HTML
delete temporary JSON/XHR responses
delete Playwright artifacts
delete screenshots
delete .tmp Parquet
```

Giữ:

```text
canonical Parquet
instrument dimension
source status
probe report
coverage/parity report
manifest
```

Nếu validation fail, giữ raw response cho repair.

---

## 22. Test plan

### Provider unit tests

- XNO normalization.
- Vietstock HTML/XHR parsing.
- TradingView response normalization.
- HTTP status classification.
- Empty versus error.
- Rate-limit behavior.
- Schema drift.

### Daily integration

- Old expired contract.
- Pre-KRX contract.
- Post-KRX contract.
- Current contract.
- Resume.
- Duplicate backfill.
- Missing page.
- Contract date boundaries.

### 1m integration

- Earliest bar.
- Timezone.
- Session filter.
- Duplicate rows.
- Overlap source conflict.
- Daily aggregation parity.

### Continuous

- Calendar roll.
- Volume roll uses `t-1`.
- No same-day look-ahead.
- No adjusted fill prices.
- 1m and daily active contract agreement where contract-aware data exists.

### Matrix

- Futures excluded from equity ranking.
- No all-null column.
- Correct first-valid date.
- Existing equity matrix unaffected.

---

## 23. Acceptance gates

### Gate A — Free continuous daily ready

```text
at least one free continuous provider positive
first_bar materially earlier than 2024
daily validation pass
```

### Gate B — Individual daily ready

```text
Vietstock full extraction proven on 4/5 sample contracts
contract backfill has positive rows
coverage and OHLC validation pass
```

### Gate C — Continuous 1m ready

```text
at least one free source returns >1000 real bars
first_bar earlier than existing 2024 coverage
timezone/session validated
```

### Gate D — Contract-rebuilt continuous ready

```text
roll table complete
individual contract chain usable
overlap parity pass
no look-ahead
```

### Gate E — Matrix promotion

```text
quality_status = PASS
source provenance present
non-null rows > existing series
```

---

## 24. Expected outcomes

### Best case

```text
daily contract-aware: 2017-08-10 → present
continuous 1m:        2018-08-13 → present
```

### Acceptable case

```text
daily contract-aware: 2017 → present
continuous 1m:        2020/2022 → present
```

### Minimum success

```text
free continuous daily extended substantially before 2024
contract daily progressively rebuilt
1m remains 2024 → present
```

Không coi minimum case là failure; daily đã đủ cho các chiến lược portfolio, hedge và asset allocation.

---

## 25. Implementation order

```text
1. Patch typed provider result and probe gates.
2. Integrate XNO and run actual 1m/1D probe.
3. Discover Vietstock full daily extraction.
4. Probe TradingView as validation/fill source.
5. Publish VN30F1M_FREE daily.
6. Backfill Vietstock individual daily contracts.
7. Build roll table and VN30F1M_CONTRACT.
8. Merge longest positive continuous 1m sources.
9. Parity-test against existing 2024 provider series.
10. Update VN Daily Matrix and enable main service.
```

---

## 26. Non-negotiable invariants

```text
No provider promoted without positive bars.
No HTTP error interpreted as confirmed empty.
No full backfill before probe gate.
No private-access bypass for web scraping.
No forward-fill missing 1m bars.
No averaging conflicting OHLC.
No future volume in roll decision.
No adjusted price used for execution.
No futures included in equity ranking.
No silent overwrite of existing VN30F1M series.
```

---

## 27. References

### Repository

- `trading-historical-data`, branch `dev`  
  <https://github.com/BobbyAxerol/trading-historical-data/tree/dev>

- Existing provider probe  
  <https://github.com/BobbyAxerol/trading-historical-data/blob/dev/collectors/vn_derivatives/probe.py>

- Existing KBS adapter  
  <https://github.com/BobbyAxerol/trading-historical-data/blob/dev/collectors/providers/kbs_derivatives.py>

- Existing DNSE adapter  
  <https://github.com/BobbyAxerol/trading-historical-data/blob/dev/collectors/providers/dnse_derivatives.py>

### Free sources

- XNO API package  
  <https://pypi.org/project/xnoapi/>

- Vietstock futures overview  
  <https://finance.vietstock.vn/chung-khoan-phai-sinh/hop-dong-tuong-lai.htm>

- Vietstock VN30F1M  
  <https://finance.vietstock.vn/chung-khoan-phai-sinh/VN30F1M/hop-dong-tuong-lai.htm>

- TradingView VN30 continuous futures  
  <https://vn.tradingview.com/symbols/HNX-VN301%21/>

- TradingView contract list  
  <https://vn.tradingview.com/symbols/HNX-VN301%21/contracts/>

---

## Final decision

Official free-first strategy:

```text
XNO:
    first probe for longest continuous 1m and daily

Vietstock:
    primary path for individual daily contracts

TradingView:
    validation and optional public-source fill

KBS/DNSE:
    fallback only after positive proof
```

Daily contract-aware history is the primary production target. Continuous 1m is attempted aggressively, especially through XNO, but its earliest date is determined only by actual returned bars.
