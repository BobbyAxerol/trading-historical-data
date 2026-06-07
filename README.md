# Automated Market Data Collectors

Hệ thống dịch vụ thu thập dữ liệu thị trường tự động (Crypto, VN Stocks, VN Futures, Options) cho Pool Alpha. Hệ thống được đóng gói dưới dạng các container Docker chạy liên tục, tự động lưu trữ phân vùng, chống trùng lặp (deduplication) và tự động lọc dữ liệu ngoài giờ giao dịch/ngày lễ.

---

## 1. Cấu trúc lưu trữ (Storage Layout / Data Lake)

Toàn bộ dữ liệu thu thập được lưu trữ tập trung tại thư mục `storage/` dưới dạng các tệp tin **CSV nén GZIP (`.csv.gz`)** được phân vùng (partitioned).

### 1.1 Sơ đồ phân vùng đường dẫn

```text
storage/
├── crypto/
│   ├── binance_futures/
│   │   └── 1m/
│   │       └── symbol=BTCUSDT/
│   │           └── year=2026/
│   │               └── month=06/
│   │                   └── part.csv.gz
│   └── binance_daily_matrix/
│       ├── open.csv.gz
│       ├── high.csv.gz
│       ├── low.csv.gz
│       ├── close.csv.gz
│       └── volume.csv.gz
├── vn/
│   ├── equity/
│   │   ├── 1d/
│   │   │   └── symbol=FPT/
│   │   │       └── year=2026/
│   │   │           └── part.csv.gz
│   │   └── 1m/
│   │       └── symbol=FPT/
│   │           └── year=2026/
│   │               └── month=06/
│   │                   └── part.csv.gz
│   └── futures/
│       └── 1m/
│           └── symbol=VN30F1M/
│               └── year=2026/
│                   └── month=06/
│                       └── part.csv.gz
└── options/
    └── binance/
        └── snapshot_5m/
            └── underlying=BTC/
                └── year=2026/
                    └── month=06/
                        └── part.csv.gz
```

### 1.2 Quy chuẩn Schema & Kiểu dữ liệu (VN Stocks & Futures 1m)

Tất cả các tệp tin trong `vn/equity/1m/` và `vn/futures/1m/` đều được đồng bộ hóa về cùng một cấu trúc cột và kiểu dữ liệu chuẩn:

| Tên cột | Kiểu dữ liệu (Pandas) | Mô tả |
| :--- | :--- | :--- |
| `time` | `datetime64[ns]` | Thời gian mở nến (naive, múi giờ Việt Nam `Asia/Ho_Chi_Minh`) |
| `symbol` | `str` | Mã chứng khoán/phái sinh (ví dụ: `FPT`, `VN30F1M`) |
| `open` | `float64` | Giá mở cửa |
| `high` | `float64` | Giá cao nhất |
| `low` | `float64` | Giá thấp nhất |
| `close` | `float64` | Giá đóng cửa |
| `volume` | `int64` | Khối lượng khớp lệnh |
| `source` | `str` | Nguồn dữ liệu (ví dụ: `vnstock_kbs`, `dnse`) |
| `ingested_at`| `str` | Thời gian ghi nhận vào hệ thống (ISO UTC timestamp) |

### 1.3 Cấu trúc Ma trận Dữ liệu Ngày Binance (Binance Daily Matrix)

Dữ liệu Binance Daily Matrix được lưu trữ tại `storage/crypto/binance_daily_matrix/` dưới dạng các ma trận xoay (pivoted) tương ứng cho 5 trường thuộc tính:
- `open.csv.gz`: Giá mở cửa.
- `high.csv.gz`: Giá cao nhất.
- `low.csv.gz`: Giá thấp nhất.
- `close.csv.gz`: Giá đóng cửa.
- `volume.csv.gz`: Khối lượng giao dịch (kiểu `int64`).

**Cấu trúc bảng ma trận:**
- **Index (Dòng)**: Cột chỉ số ngày (`time`, định dạng `YYYY-MM-DD`).
- **Columns (Cột)**: Danh sách các symbol futures của Binance (ví dụ: `BTCUSDT`, `ETHUSDT`, ...).
- **Trị số**: Giá trị số thực (`float64`) hoặc số nguyên (`int64` đối với volume).

Danh sách symbol được theo dõi lưu trong tệp trạng thái `state/binance_daily_matrix_symbols.json`, tự động cập nhật hàng tháng bằng cách lấy Top 400 symbol giao dịch nhiều nhất (quoteVolume) từ sàn Binance USD-M Futures, đảm bảo chỉ thêm mới và không loại bỏ các symbol cũ trừ khi symbol đó bị delist hoàn toàn khỏi sàn.

---

## 2. Quy tắc lọc giờ giao dịch & Ngày nghỉ lễ Việt Nam

Dữ liệu VN Intraday 1m (cả Cơ sở lẫn Phái sinh) được tự động đi qua bộ lọc **[`filter_trading_hours`](collectors/common/calendar_vn.py)** trước khi xử lý hoặc lưu trữ:
- **Ngày nghỉ lễ**: Tự động loại bỏ dữ liệu rơi vào danh sách ngày lễ của Việt Nam (`VN_HOLIDAYS` định nghĩa trong `calendar_vn.py`).
- **Giờ giao dịch (Stocks)**: Chỉ giữ lại dữ liệu trong khung giờ `09:00 - 11:30` và `13:00 - 14:45`.
- **Giờ giao dịch (Phái sinh - VN30F)**: Chỉ giữ lại dữ liệu trong khung giờ `08:45 - 11:30` và `13:00 - 14:45`.

*(Crypto chạy 24/7 và không áp dụng bộ lọc này).*

---

## 3. Quản lý trạng thái (State & Healthcheck)

Hệ thống ghi nhận trạng thái liên tục tại thư mục `state/` để phục vụ cơ chế tự resume (incremental update) và theo dõi sức khỏe:
- **`state/manifests/<dataset>.json`**: Lưu thông tin `latest_time` (mốc dữ liệu lớn nhất đã tải) của từng symbol. Khi khởi động lại, dịch vụ sẽ đọc manifest để tiếp tục fetch từ mốc này, chồng lấn một khoảng ngắn (overlap) để bắt kịp dữ liệu mới nhất.
- **`state/heartbeats/<service>.json`**: Ghi nhận trạng thái sống (heartbeat) mỗi vòng lặp. Dịch vụ giám sát có thể chạy lệnh sau để kiểm tra sức khỏe hệ thống:
  ```bash
  python -m collectors.healthcheck
  ```

---

## 4. Hướng dẫn vận hành & Lịch chạy (Schedules)

### 4.1 Cơ chế và Tần suất cập nhật (Scheduling Frequency)

Để tránh bị giới hạn hoặc khoá API Key/IP từ các nhà cung cấp dữ liệu, hệ thống được cấu hình chạy định kỳ cuối ngày thay vì cập nhật liên tục trong phiên giao dịch:
- **VN Intraday Stocks & Futures**: Chạy hàng ngày lúc **16:30** (Giờ Việt Nam `Asia/Ho_Chi_Minh`). Khi khởi động lại hoặc đến giờ chạy, hệ thống tự động kiểm tra và thực hiện tải bù dữ liệu thiếu theo ngày (incremental catch-up) cho mục đích lưu trữ lịch sử để backtest.
- **Binance Daily Matrix**: Chạy hàng ngày lúc **01:00** (Giờ UTC).
- **Crypto 1m Live**: Chạy cập nhật liên tục mỗi phút để thu thập nến crypto thời gian thực.
- **Options Binance 5m**: Chạy cập nhật Snapshot tùy chọn Binance mỗi 5 phút.

### 4.2 Khởi chạy bằng Docker Compose

Hệ thống sử dụng Docker Compose để cấu hình các dịch vụ chạy ngầm (`restart: unless-stopped`):

```bash
# Khởi dựng hình ảnh và chạy toàn bộ dịch vụ ngầm
docker compose up -d --build

# Xem log của các dịch vụ
docker compose logs -f binance-daily-matrix
docker compose logs -f vn-intraday-stocks
docker compose logs -f vn30f1m-dnse
```

### 4.3 Chạy thử lệnh One-shot (Ghi trực tiếp vào storage)

Bạn có thể chạy thử một chu kỳ tải dữ liệu bằng interpreter python local:

```bash
# Chạy một lần để lấy dữ liệu FPT (KBS) từ ngày 2026-05-23
PYTHONPATH=. python -m collectors.vn_intraday_vnstock --mode once --symbols FPT --backfill-start 2026-05-23

# Chạy một lần để lấy dữ liệu VN30F1M (DNSE) từ ngày 2026-06-01
PYTHONPATH=. python -m collectors.vn_intraday_dnse --mode once --symbols VN30F1M --backfill-start 2026-06-01

# Chạy một lần để cập nhật ma trận Binance Daily từ ngày 2026-06-01
PYTHONPATH=. python -m collectors.binance_daily_matrix --mode once --backfill-start 2026-06-01
```

---

## 5. Tổ chức mã nguồn

- **`collectors/`**: Chứa mã nguồn của các services chính.
- **`collectors/common/`**: Thư viện dùng chung quản lý lịch, phân vùng lưu trữ, ghi tệp tin an toàn (atomic write), lock chống tranh chấp, retry và manifest.
- **`configs/`**: Tệp tin cấu hình YAML chứa danh sách symbol cần chạy.
- **`tests/`**: Các script kiểm thử nguồn kết nối và định dạng lưu trữ.
- **`legacy/`**: Thư mục chứa các script và notebook kiểm thử cũ đã ngừng hoạt động (được cấu hình loại trừ trong `.gitignore`).
