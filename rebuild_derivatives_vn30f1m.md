# UPGRADE VN30 FUTURES HISTORICAL DATA

## KBS Primary, DNSE Fallback, Individual Contracts và Continuous Series V1

> **Repository:** `BobbyAxerol/trading-historical-data`
> **Mục tiêu:** kéo lịch sử dài nhất có thể của từng hợp đồng VN30 futures, ưu tiên KBS, dùng DNSE fallback; sau đó dựng lại `VN30F1M` continuous cho cả `1m` và `1d`.
> **Ngày bắt đầu thị trường:** `2017-08-10`. Bốn hợp đồng trong phiên khai trương là `VN30F1708`, `VN30F1709`, `VN30F1712`, `VN30F1803`.

---

# 1. Quyết định kiến trúc

## Provider priority

```text
KBS / vnstock
    → nguồn chính cho 1m và 1d

DNSE
    → fallback cho range KBS bị thiếu hoặc lỗi
    → validation chéo trên range overlap

Aggregate 1m → 1d
    → fallback cuối cùng nếu cả KBS và DNSE không có daily trực tiếp
```

Không tiếp tục dùng alias `VN30F1M` làm nguồn lịch sử chính.

Pipeline mới phải:

```text
individual contracts
    → validate
    → canonical contract storage
    → daily roll map
    → continuous 1m
    → continuous 1d
```

---

# 2. Quy ước mã hợp đồng cũ và KRX

## 2.1 Canonical internal symbol

Trong storage và backtest, luôn dùng mã dễ đọc:

```text
VN30FYYMM
```

Ví dụ:

```text
VN30F1709
VN30F2508
VN30F2607
```

Không dùng `VN30F1M`, `VN30F2M`, `VN30F1Q`, `VN30F2Q` làm identity của hợp đồng vì các alias này thay đổi theo thời gian.

## 2.2 Mã KRX mới

HNX quy định mã phái sinh mới gồm chín ký tự:

```text
41 + underlying + year_code + month_code + 000
```

Đối với VN30:

```text
underlying = I1
```

Ví dụ chính thức:

```text
VN30F2508 → 41I1F8000
```

HNX cũng nêu rõ các hợp đồng đã niêm yết tại thời điểm chuyển sang KRX tiếp tục giữ mã cũ đến đáo hạn; các sản phẩm niêm yết mới sau thời điểm vận hành KRX dùng mã mới. Vì vậy giai đoạn chuyển tiếp có thể tồn tại đồng thời mã legacy và mã KRX.

## 2.3 Hàm chuyển đổi

```python
YEAR_CODES = {
    2010: "0", 2011: "1", 2012: "2", 2013: "3",
    2014: "4", 2015: "5", 2016: "6", 2017: "7",
    2018: "8", 2019: "9", 2020: "A", 2021: "B",
    2022: "C", 2023: "D", 2024: "E", 2025: "F",
    2026: "G", 2027: "H", 2028: "J", 2029: "K",
}

MONTH_CODES = {
    1: "1", 2: "2", 3: "3", 4: "4",
    5: "5", 6: "6", 7: "7", 8: "8",
    9: "9", 10: "A", 11: "B", 12: "C",
}

def legacy_to_krx(year: int, month: int) -> str:
    return f"41I1{YEAR_CODES[year]}{MONTH_CODES[month]}000"
```

Implementation nên mở rộng theo chu kỳ 30 năm giống quy tắc HNX thay vì hard-code đến năm 2029.

## 2.4 Symbol dimension

Tạo:

```text
storage/vn/futures/instruments/version=v1/instruments.parquet
```

Schema:

```text
instrument_id
canonical_symbol
legacy_symbol
krx_symbol
expiry_date
listing_start
listing_end
exchange_symbol_at_listing
kbs_symbol_resolved
dnse_symbol_resolved
kbs_available_1m
kbs_available_1d
dnse_available_1m
dnse_available_1d
first_1m
last_1m
first_1d
last_1d
```

---

# 3. Lưu ý quan trọng với KBS/vnstock

KBS source trong vnstock:

* Hỗ trợ `1m` qua endpoint suffix `1P`.
* Hỗ trợ daily qua suffix `day`.
* Dùng endpoint dạng `/stocks/{symbol}/data_{interval}`.
* Tự nhận diện derivatives.
* Tự động chuyển mã legacy `VN30FYYMM` sang mã KRX trước khi request.

Vấn đề:

```python
Quote("VN30F1709")
```

có thể tự chuyển thành:

```text
41I179000
```

Điều này tiện nhưng không cho phép kiểm tra KBS có lưu lịch sử dưới mã legacy hay mã KRX.

## Yêu cầu implementation

Tạo low-level KBS adapter riêng:

```text
collectors/providers/kbs_derivatives.py
```

Adapter hỗ trợ:

```python
fetch_ohlc(
    provider_symbol: str,
    start: datetime,
    end: datetime,
    resolution: Literal["1m", "1d"],
    auto_convert: bool = False,
)
```

Resolver thử:

```text
1. Mã từ KBS instrument discovery.
2. Mã KRX.
3. Mã legacy.
```

Không phụ thuộc duy nhất vào auto-conversion của vnstock.

---

# 4. Lưu ý quan trọng với DNSE

DNSE OHLC hỗ trợ explicit market type:

```text
type = DERIVATIVE
```

và resolution minute. Tài liệu DNSE mô tả topic OHLC derivative riêng, với `1` là dữ liệu một phút.

Collector hiện tại của repo chỉ coi bốn alias sau là derivative:

```python
DERIVATIVE_SYMBOLS = {
    "VN30F1M",
    "VN30F2M",
    "VN30F1Q",
    "VN30F2Q",
}
```

Do đó hợp đồng riêng như `VN30F2503` hoặc mã KRX sẽ bị route nhầm thành `STOCK`.

## Bắt buộc sửa

Không suy luận asset type bằng set alias.

Thay API thành:

```python
fetch_ohlc(
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    resolution: str,
    asset_type: Literal["stock", "derivative"],
)
```

Request:

```python
bar_type = "DERIVATIVE"
```

cho mọi contract futures.

Resolver DNSE:

```text
1. Query instrument/security definition nếu API trả được.
2. Thử exact mã provider discovery.
3. Thử legacy symbol.
4. Thử KRX symbol.
5. Empty success khác với request error.
```

---

# PHASE 1 — PROBE VÀ BACKFILL INDIVIDUAL CONTRACTS

# 5. Contract calendar

Sinh monthly canonical contracts từ:

```text
2017-08 → hiện tại + 6 tháng
```

Canonical format:

```text
VN30FYYMM
```

Ngày đáo hạn lý thuyết:

```text
thứ Năm thứ ba của tháng đáo hạn
```

Nếu trùng ngày nghỉ, dùng ngày giao dịch liền trước theo exchange calendar.

Không tự giả định listing date. `listing_start` là bar đầu tiên thực tế tìm thấy từ provider.

---

# 6. Probe bắt buộc trước full backfill

Chưa hard-code rằng KBS hoặc DNSE có 1m từ năm 2017.

Môi trường triển khai phải test ít nhất:

```text
VN30F1708
VN30F1709
VN30F1712
VN30F1803

VN30F2003
VN30F2206
VN30F2406

VN30F2504      # trước/chuyển tiếp KRX
VN30F2505
VN30F2506
VN30F2508      # ví dụ chính thức KRX

một số contract hiện tại
```

Với mỗi contract, thử:

```text
KBS legacy symbol
KBS KRX symbol
DNSE legacy symbol
DNSE KRX symbol
```

Cho cả:

```text
resolution = 1m
resolution = 1d
```

## Probe output

```text
state/vn_derivatives/provider_probe_v1.parquet
state/vn_derivatives/provider_probe_v1.json
```

Fields:

```text
canonical_symbol
provider
provider_symbol
resolution
request_success
empty_confirmed
first_bar
last_bar
row_count
columns
timezone
price_scale
volume_scale
max_safe_request_days
error
```

## Acceptance của probe

Phải kết luận được:

```text
earliest KBS 1m
earliest DNSE 1m
earliest KBS daily
earliest DNSE daily

KBS dùng legacy hay KRX cho lịch sử cũ
DNSE dùng legacy hay KRX cho lịch sử cũ

daily resolution DNSE là D, 1D hay giá trị khác
maximum date window an toàn cho 1m
```

Không ghi trong documentation rằng 1m bắt đầu từ năm 2017 nếu probe chưa chứng minh.

---

# 7. Backfill 1m dài nhất có thể

## Request windows

Vì provider có thể giới hạn số rows dù không báo rõ:

```yaml
kbs_1m_window_calendar_days: 7
dnse_1m_window_calendar_days: 5
```

Nếu response có dấu hiệu truncate:

* Chia đôi window.
* Fetch lại.
* Không advance manifest khi chưa xác định đầy đủ.

## Contract query range

Với mỗi contract:

```text
start = max(2017-08-10, expiry_date - 270 days)
end   = expiry_date + 1 day
```

270 ngày đủ rộng để tìm ngày niêm yết thực tế của quarterly contracts.

## Provider merge

```text
KBS complete bar
    → giữ KBS

KBS thiếu timestamp nhưng DNSE có
    → fill bằng DNSE

Cả hai có cùng timestamp
    → KBS primary
    → lưu parity statistics

Cả hai đều thiếu
    → giữ gap
    → không forward-fill
```

## Không synthesize 1m

Nếu provider chỉ có daily:

```text
không tạo 1m từ daily
```

Continuous 1m phải bắt đầu tại ngày sớm nhất có contract-level 1m thật.

---

# 8. Backfill daily dài nhất có thể

Provider order:

```text
1. KBS daily trực tiếp.
2. DNSE daily trực tiếp.
3. Aggregate canonical 1m nếu session đủ.
```

Aggregate:

```text
open   = first
high   = max
low    = min
close  = last
volume = sum
```

Daily row phải có:

```text
source = kbs
source = dnse
source = aggregated_1m
```

Không aggregate session thiếu nhiều bars mà vẫn coi là daily complete.

---

# 9. Contract-level storage

## 1m

```text
storage/vn/futures/contracts/1m/
└── symbol=VN30F1709/
    └── year=2017/
        └── month=09/
            └── part.parquet
```

## Daily

```text
storage/vn/futures/contracts/1d/
└── symbol=VN30F1709/
    └── year=2017/
        └── part.parquet
```

## Fact schema

```text
time
instrument_id
open
high
low
close
volume
source
quality_flags
ingested_at
```

Static symbols, expiry và provider mapping nằm trong instrument dimension.

## Source priority

```python
SOURCE_KBS = 1
SOURCE_DNSE = 2
SOURCE_AGGREGATED_1M = 3
```

---

# 10. Validation contract data

## OHLC invariants

```text
high >= open
high >= close
high >= low

low <= open
low <= close

volume >= 0
```

## Timestamp

* Canonical timezone: `Asia/Ho_Chi_Minh`.
* Không lưu naive timestamp nếu không có metadata timezone.
* Không duplicate `(instrument_id, time)`.
* Không có bars sau expiry session.

## Tick-size sanity

VN30 futures có tick size 0.1 index point. Giá lệch khỏi grid phải được flag, không tự round âm thầm.

## KBS–DNSE parity

Trên overlap:

```text
abs(close_kbs - close_dnse) <= 0.1:
    exact/acceptable

0.1 < difference <= 0.5:
    warning

difference > 0.5:
    conflict
```

Volume:

```text
exact match preferred
relative difference > 5% → warning
```

KBS vẫn là canonical primary, nhưng conflict lớn phải được lưu trong report.

---

# PHASE 2 — BUILD CONTINUOUS, MATRIX VÀ SERVICES

# 11. Hai continuous series

Để vừa parity với alias F1M vừa backtest thực tế, build hai series nhỏ.

## 11.1 `VN30F1M`

Calendar front-month series:

```text
Giữ hợp đồng gần nhất đến hết phiên đáo hạn.
Phiên giao dịch tiếp theo chuyển sang hợp đồng tháng kế tiếp.
```

Mục tiêu:

* Tương thích ý nghĩa F1M.
* Parity với provider alias hiện có.
* Dùng cho benchmark và signal.

## 11.2 `VN30F1M_TRADE`

Liquidity-aware tradable series:

Roll sang contract tiếp theo tại phiên kế tiếp nếu:

```text
next_volume > current_volume
```

trong hai phiên đã đóng liên tiếp.

Hard roll:

```text
không muộn hơn phiên trước expiry
```

Mọi quyết định roll dùng dữ liệu đến close ngày `t-1`, và áp dụng từ đầu ngày `t`.

Không dùng volume cùng ngày để đổi contract ngay trong ngày, tránh look-ahead.

---

# 12. Daily roll table

Tạo:

```text
storage/vn/futures/rolls/version=v1/rolls.parquet
```

Schema:

```text
trading_date
series
old_instrument_id
new_instrument_id
roll_reason
decision_date
old_close
new_close
roll_gap
roll_ratio
```

`1m` và `1d` phải dùng chung roll table.

Không được có logic roll khác nhau giữa hai timeframe.

---

# 13. Continuous 1m

Với mỗi trading date:

1. Đọc active contract từ roll table.
2. Load contract 1m của ngày đó.
3. Ghi sang continuous partition.
4. Không nối bars của hai contracts trong cùng phiên.
5. Không forward-fill missing minute.
6. Lưu `active_instrument_id`.

Path:

```text
storage/vn/futures/continuous/1m/
└── symbol=VN30F1M/
    └── version=v1/
        └── year=YYYY/
            └── month=MM/
                └── part.parquet
```

---

# 14. Continuous daily

Path:

```text
storage/vn/futures/continuous/1d/
└── symbol=VN30F1M/
    └── version=v1/
        └── year=YYYY/
            └── part.parquet
```

Schema:

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

## Raw và adjusted

Giữ:

```text
raw OHLC
roll_gap
roll_ratio
```

Tính on load:

```text
back_adjusted_close
continuous_return
```

Không dùng adjusted price làm execution price.

Khi tính return qua ngày roll:

* Không dùng trực tiếp `new_close / old_close`.
* Dùng roll-aware return hoặc explicit close/open contract transactions.

---

# 15. Overlap với series hiện tại

Series hiện tại:

```text
VN30F1M_PROVIDER
2024-05-02 → hiện tại
```

Không overwrite ngay.

Dùng làm validation:

```text
provider alias
vs
rebuilt VN30F1M
```

Report:

```text
overlap_start
overlap_end
daily_return_correlation
non_roll_price_difference
roll_date_difference
largest_mismatch_dates
volume_difference
```

Acceptance gợi ý:

```text
return correlation excluding roll dates >= 0.995
median non-roll close difference <= 0.1 point
```

Sau khi parity pass:

```text
VN30F1M
    = rebuilt canonical continuous từ earliest available

VN30F1M_PROVIDER
    = validation-only series
```

---

# 16. Tích hợp VNDaily Matrix

`storage/vn/equity/daily_matrix/close.parquet` tiếp tục có column:

```text
VN30F1M
```

Nhưng column này được lấy từ:

```text
storage/vn/futures/continuous/1d/symbol=VN30F1M/version=v1
```

Không lấy trực tiếp từ alias DNSE cũ.

Metadata:

```text
asset_type = future
eligible_for_equity_ranking = false
continuous_version = v1
roll_policy = calendar_front_month
```

Optional thêm:

```text
VN30F1M_TRADE
```

cho strategy thật sự muốn short và mô phỏng roll sớm theo thanh khoản.

---

# 17. Collector mới

Tạo:

```text
collectors/vn_derivatives.py
```

Modules:

```text
collectors/vn_derivatives/
├── symbols.py
├── calendar.py
├── kbs_provider.py
├── dnse_provider.py
├── probe.py
├── contracts.py
├── validate.py
├── roll.py
├── continuous.py
└── matrix_adapter.py
```

CLI:

```bash
python -m collectors.vn_derivatives probe

python -m collectors.vn_derivatives backfill \
  --start 2017-08-10 \
  --resolutions 1m,1d

python -m collectors.vn_derivatives sync-once

python -m collectors.vn_derivatives validate

python -m collectors.vn_derivatives build-continuous

python -m collectors.vn_derivatives update-matrix
```

---

# 18. Config

Tạo:

```text
configs/vn_derivatives.yml
```

```yaml
dataset_version: v1
underlying: VN30
backfill_start: "2017-08-10"

providers:
  primary: kbs
  fallback: dnse

symbols:
  canonical_format: "VN30FYYMM"
  support_legacy: true
  support_krx: true
  krx_underlying_code: I1

resolutions:
  - 1m
  - 1d

requests:
  kbs_1m_window_days: 7
  dnse_1m_window_days: 5
  daily_window_days: 365
  retry_attempts: 5

storage:
  format: parquet
  compression: zstd
  intraday_partition: month
  daily_partition: year

continuous:
  calendar_series: VN30F1M
  tradable_series: VN30F1M_TRADE
  volume_confirmation_days: 2
  hard_roll_sessions_before_expiry: 1

validation:
  price_tolerance_points: 0.1
  conflict_tolerance_points: 0.5
  volume_relative_warning: 0.05
```

---

# 19. Docker Compose

Service hiện tại chỉ chạy alias `VN30F1M` qua DNSE và hard-code bốn derivative aliases. Service này phải được deprecated để tránh hai process cùng ghi futures data.

## Bootstrap service

```yaml
vn-derivatives-bootstrap:
  <<: *get-data-service
  profiles: ["bootstrap"]
  restart: "no"
  command:
    - python
    - -m
    - collectors.vn_derivatives
    - backfill
    - --start
    - "2017-08-10"
    - --resolutions
    - "1m,1d"
```

## Main daily service

```yaml
vn-derivatives:
  <<: *get-data-service
  command:
    - python
    - -m
    - collectors.vn_derivatives
    - live
    - --schedule
    - "16:30"
  environment:
    TZ: Asia/Ho_Chi_Minh
    DATA_ROOT: /app/storage
    STATE_ROOT: /app/state
    LOG_ROOT: /app/logs
    VNSTOCK_API_KEY: ${VNSTOCK_API_KEY:-}
    DNSE_API_KEY: ${DNSE_API_KEY:-}
    DNSE_API_SECRET_KEY: ${DNSE_API_SECRET_KEY:-}
```

Daily workflow:

```text
discover new contracts
→ sync 1m tails
→ sync direct daily
→ fill KBS gaps with DNSE
→ validate
→ rebuild affected continuous partitions
→ update VN Daily Matrix
→ write report
```

---

# 20. Disk-first và OOM safety

Không giữ toàn bộ contracts trong RAM.

Process từng:

```text
contract
→ request window
→ validate
→ append partition
→ release DataFrame
```

Không dùng:

```python
all_contracts = []
pd.concat(all_contracts)
```

Continuous builder xử lý:

```text
một ngày hoặc một tháng mỗi lần
```

Daily builder xử lý:

```text
một contract mỗi lần
```

Manifest riêng:

```text
state/vn_derivatives/contracts_1m.json
state/vn_derivatives/contracts_1d.json
state/vn_derivatives/continuous_v1.json
```

---

# 21. Acceptance criteria

## Provider probe

* Xác định được earliest 1m thật của KBS.
* Xác định được earliest 1m thật của DNSE.
* Xác định được earliest daily của hai provider.
* Xác định mapping legacy/KRX thực tế.
* Không nhầm empty response với request failure.

## Contract data

* Không duplicate timestamp.
* Không bar sau expiry.
* Không OHLC invalid.
* Mỗi row có provenance.
* DNSE chỉ fill gap, không overwrite KBS tùy tiện.

## Continuous

* Daily bắt đầu sớm nhất có thể, mục tiêu `2017-08-10`.
* 1m bắt đầu tại earliest contract-level minute data thực tế.
* Không synthesize 1m từ daily.
* Roll table không look-ahead.
* 1m và daily dùng cùng roll map.
* Held contract/roll không bị nhầm do alias.

## Matrix

* `VN30F1M` được extend về earliest continuous daily date.
* Provider alias cũ được giữ để parity.
* `VN30F1M` không tham gia equity cross-sectional ranking.
* Daily matrix rebuild thành công.

## Service

* `vn-derivatives-bootstrap` chạy được one-shot.
* `vn-derivatives` chạy daily incremental.
* Service DNSE alias cũ bị disable hoặc migrate.
* Restart không tải lại toàn bộ lịch sử.

---

# 22. Kết quả cuối cần báo cáo

```text
KBS earliest 1m:
DNSE earliest 1m:
Canonical continuous 1m start:

KBS earliest daily:
DNSE earliest daily:
Canonical continuous daily start:

Contracts discovered:
Contracts with 1m:
Contracts with daily:
KBS-filled rows:
DNSE fallback rows:
Aggregated daily rows:

Unresolved gaps:
Provider conflicts:
Overlap correlation with VN30F1M_PROVIDER:
Final storage size:
```

## Quy tắc cuối cùng

```text
Canonical identity luôn là VN30FYYMM.
Provider symbol chỉ là mapping.

KBS là primary.
DNSE chỉ fallback/validation.

Continuous được dựng từ hợp đồng thật.
Alias không phải source of truth.

Không kéo 1m lùi xa hơn coverage thật.
Không forward-fill missing minute.
Không dùng future volume để quyết định roll.
Không dùng adjusted price làm execution price.
```
