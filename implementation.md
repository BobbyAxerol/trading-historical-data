# UPGRADE VN DAILY UNIVERSE — 2 PHASES

## Mục tiêu

Mở rộng `VNDaily Matrix` thêm khoảng 110–150 mã tiềm năng, ưu tiên cổ phiếu có lịch sử đủ dài, giao dịch tương đối đều và phù hợp cho chiến lược danh mục.

* Backfill request luôn bắt đầu từ `2016-01-01`.
* Nếu mã niêm yết sau năm 2016 hoặc vendor chỉ có dữ liệu từ ngày muộn hơn, chấp nhận `first_valid_date` thực tế; không coi giai đoạn trước ngày đó là missing data.
* Không tự động xóa mã chỉ vì lịch sử ngắn.
* Thêm `VN30F1M` daily vào matrix như một external series, nhưng không xếp hạng nó cùng cổ phiếu.
* Tổng dung lượng permanent tăng dự kiến chỉ khoảng 25–70 MB.

---

## PHASE 1 — Backfill, Validate và Score Universe

### 1. Mở rộng config

Bổ sung danh sách candidate đã đề xuất vào:

```text
configs/symbols.vn_daily.yml
```

Giữ:

```yaml
backfill_start: "2016-01-01"
```

Collector phải gọi từng mã từ năm 2016. Nếu response đầu tiên chỉ bắt đầu ở ngày niêm yết thực tế thì lưu từ ngày đó, không retry vô hạn cho giai đoạn trước listing.

### 2. Thu thập dữ liệu

Với mỗi symbol:

1. Call dữ liệu từ `2016-01-01` đến ngày đã đóng gần nhất.
2. Append/deduplicate theo `time, symbol`.
3. Lưu dưới:

```text
storage/vn/equity/1d/symbol={SYMBOL}/year={YYYY}/part.csv.gz
```

4. Không loại mã chỉ vì không có đủ dữ liệu từ năm 2016.
5. Chỉ đánh dấu lỗi khi:

   * Không có bất kỳ dữ liệu nào.
   * OHLC sai nghiêm trọng.
   * Có gap nội bộ lớn chưa giải thích.
   * Tail đã stale quá lâu.
   * Vendor trả schema không hợp lệ.

### 3. Thêm `VN30F1M` daily

Tạo:

```text
storage/vn/futures/1d/symbol=VN30F1M/year={YYYY}/part.csv.gz
```

Ưu tiên dùng daily history hiện có. Nếu cần aggregate từ 1m:

```text
open   = first
high   = max
low    = min
close  = last
volume = sum
```

theo ngày giao dịch Việt Nam.

### 4. Metadata tối thiểu

Chỉ tạo một file:

```text
state/vn_daily_universe_report.csv.gz
```

Schema:

```text
symbol
asset_type
first_valid_date
last_valid_date
row_count
coverage_ratio
max_internal_gap
median_turnover_60d
median_turnover_252d
score
tier
reasons
```

Không cần eligibility matrix hoặc metadata ngành phức tạp trong phase này.

### 5. Score không loại nhầm mã tiềm năng

Score:

```text
40% liquidity
30% continuity/coverage
20% history length
10% recent availability
```

Quy tắc:

```text
liquidity:
    median(close × volume) trong 60 và 252 phiên

continuity:
    tính từ first_valid_date, không tính từ 2016

history length:
    tăng dần nhưng cap tại 5 năm;
    không phạt quá mạnh mã mới niêm yết

recent availability:
    dữ liệu phải cập nhật gần hiện tại
```

Tier:

```text
core:
    score >= 70
    và dữ liệu đủ ổn định

extended:
    score 45–69
    hoặc lịch sử còn ngắn nhưng thanh khoản cao

review:
    score < 45 hoặc có quality warning
```

Không tự động xóa `extended` hoặc `review` khỏi raw storage. Score chỉ phục vụ chọn universe, không quyết định có lưu data hay không.

Đặc biệt:

```text
Mã mới nhưng thanh khoản top quartile:
    ít nhất phải được giữ ở extended

Mã có lịch sử dài nhưng thanh khoản thấp:
    giữ ở review hoặc extended

Mã chuyển sàn:
    nối series nếu vendor trả liên tục;
    không coi ngày chuyển sàn là listing mới
```

---

## PHASE 2 — Rebuild Matrix và Tích hợp Loader

### 1. Mở rộng matrix builder

`collectors/vn_daily_matrix.py` tiếp tục đọc equity từ:

```text
storage/vn/equity/1d
```

và đọc thêm external series:

```text
storage/vn/futures/1d/symbol=VN30F1M
```

Output vẫn giữ:

```text
storage/vn/equity/daily_matrix/
    open.csv.gz
    high.csv.gz
    low.csv.gz
    close.csv.gz
    volume.csv.gz
```

`VN30F1M` xuất hiện như một column bình thường trong matrix.

### 2. Không dùng `VN30F1M` như cổ phiếu

Trong universe report:

```text
symbol = VN30F1M
asset_type = future
tier = auxiliary
```

Loader hoặc strategy phải có khả năng:

```text
include VN30F1M làm benchmark/regime/hedge
exclude VN30F1M khỏi cross-sectional equity ranking
```

### 3. Rebuild và validation

Sau khi backfill:

1. Rebuild toàn bộ VN Daily Matrix từ `2016-01-01`.
2. Không tạo column hoàn toàn rỗng.
3. Không drop mã chỉ vì bắt đầu sau năm 2016.
4. Kiểm tra:

   * Index ngày tăng dần.
   * Không duplicate ngày.
   * `high >= open/close/low`.
   * `low <= open/close/high`.
   * Volume không âm.
   * Mỗi symbol bắt đầu đúng tại `first_valid_date`.
5. Cập nhật:

```text
state/vn_daily_matrix_symbols.json
state/vn_daily_universe_report.csv.gz
```

### 4. Cleanup

Sau khi matrix và validation pass:

```text
xóa file .tmp
xóa intermediate merge files
giữ canonical yearly files
giữ 5 matrix files
giữ universe report
```

### 5. Acceptance

```text
candidate symbols được probe đầy đủ
không loại mã mới chỉ vì history ngắn
không tính missing trước listing
VN30F1M daily có trong matrix
VN30F1M không nằm trong equity rank universe
matrix load được bằng VNDailyMatrix
dung lượng permanent tăng không quá khoảng 100 MB
```
