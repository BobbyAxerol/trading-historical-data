# Automated Market Data Collectors

Hệ thống dịch vụ thu thập dữ liệu thị trường tự động (Crypto, VN Stocks, VN Futures, Options) cho Pool Alpha. Hệ thống được đóng gói dưới dạng các container Docker chạy liên tục, tự động lưu trữ phân vùng, chống trùng lặp (deduplication) và tự động lọc dữ liệu ngoài giờ giao dịch/ngày lễ.

---

## 1. Cấu trúc lưu trữ (Storage Layout / Data Lake)

Toàn bộ dữ liệu thu thập được lưu trữ tập trung tại thư mục `storage/` dưới dạng các tệp tin **CSV nén GZIP (`.csv.gz`)** được phân vùng (partitioned).

### 1.1 Sơ đồ phân vùng đường dẫn

```text
storage/
├── crypto/
│   └── binance_futures/
│       └── 1m/
│           └── symbol=BTCUSDT/
│               └── year=2026/
│                   └── month=06/
│                       └── part.csv.gz
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

## 4. Hướng dẫn vận hành

### 4.1 Khởi chạy bằng Docker Compose

Hệ thống sử dụng Docker Compose để cấu hình chạy ngầm (`restart: unless-stopped`):

```bash
# Khởi dựng hình ảnh và chạy toàn bộ dịch vụ ngầm
docker compose up -d --build

# Xem log của một dịch vụ cụ thể
docker compose logs -f vn30f1m-dnse
docker compose logs -f vn-intraday-stocks
```

### 4.2 Chạy thử lệnh One-shot (Ghi trực tiếp vào storage)

Bạn có thể chạy thử một chu kỳ tải dữ liệu bằng interpreter python local:

```bash
# Chạy một lần để lấy dữ liệu FPT (KBS) từ ngày 2026-05-23
PYTHONPATH=. python -m collectors.vn_intraday_vnstock --mode once --symbols FPT --backfill-start 2026-05-23

# Chạy một lần để lấy dữ liệu VN30F1M (DNSE) từ ngày 2026-06-01
PYTHONPATH=. python -m collectors.vn_intraday_dnse --mode once --symbols VN30F1M --backfill-start 2026-06-01
```

---

## 5. Tổ chức mã nguồn

- **`collectors/`**: Chứa mã nguồn của các services chính.
- **`collectors/common/`**: Thư viện dùng chung quản lý lịch, phân vùng lưu trữ, ghi tệp tin an toàn (atomic write), lock chống tranh chấp, retry và manifest.
- **`configs/`**: Tệp tin cấu hình YAML chứa danh sách symbol cần chạy.
- **`tests/`**: Các script kiểm thử nguồn kết nối và định dạng lưu trữ.
- **`legacy/`**: Thư mục chứa các script và notebook kiểm thử cũ đã ngừng hoạt động (được cấu hình loại trừ trong `.gitignore`).
