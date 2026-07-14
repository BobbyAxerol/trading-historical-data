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
│   │       └── symbol=BTCUSDT_240329/
│   │           └── year=2024/
│   │               └── month=03/
│   │                   └── part.csv.gz
│   ├── binance_spot/
│   │   └── 1m/
│   │       └── symbol=BTCUSDT/
│   │           └── year=2026/
│   │               └── month=06/
│   │                   └── part.csv.gz
│   ├── binance_orderbook_snapshot/
│   │   └── 1h/
│   │       └── symbol=BTCUSDT/
│   │           └── year=2026/
│   │               └── month=07/
│   │                   └── part.csv.gz
│   ├── binance_futures_metrics/
│   │   └── 5m/
│   │       └── symbol=BTCUSDT/
│   │           └── year=2026/
│   │               └── month=07/
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
│   │   ├── daily_matrix/
│   │   │   ├── open.csv.gz
│   │   │   ├── high.csv.gz
│   │   │   ├── low.csv.gz
│   │   │   ├── close.csv.gz
│   │   │   └── volume.csv.gz
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

Danh sách symbol được theo dõi lưu trong tệp trạng thái `state/binance_daily_matrix_symbols.json`, tự động cập nhật hàng tháng từ Binance USD-M Futures nhưng chỉ nhận **crypto coin perpetual** hợp lệ: `contractType=PERPETUAL`, `underlyingType=COIN`, `quoteAsset=USDT`, `marginAsset=USDT`, loại `Alpha/Index/TradFi`, và mặc định yêu cầu tối thiểu `365` ngày history. Thứ tự cột ưu tiên nhóm core big/liquid symbols (`BTCUSDT`, `ETHUSDT`, `BNBUSDT`, `SOLUSDT`, ...) trước, sau đó mới tới phần mở rộng xếp theo score: `50%` rank `24h quoteVolume`, `30%` rank tuổi listing, `20%` rank độ ổn định volume 180 ngày. Policy là **chỉ thêm, không bớt** trong universe hợp lệ; symbol sai schema như equity/pre-market/commodity/index sẽ bị reject khỏi daily crypto matrix.

### 1.3b Binance USD-M Quarterly 1m

Concrete USD-M quarterly contracts như `BTCUSDT_240329` và `ETHUSDT_260925` được lưu chung schema/path với Binance futures 1m:

```text
storage/crypto/binance_futures/1m/symbol=BTCUSDT_240329/year=2024/month=03/part.csv.gz
```

Collector [`collectors/binance_usdm_quarterly_1m.py`](collectors/binance_usdm_quarterly_1m.py) tự discover active `CURRENT_QUARTER`/`NEXT_QUARTER` từ Binance `/fapi/v1/exchangeInfo`, discover historical concrete contracts từ Binance Vision S3, rồi đồng bộ theo thứ tự: monthly ZIP -> daily ZIP -> REST active tail. `_get_data` chỉ lưu raw contract-level data, không tự build continuous contract. Chi tiết vận hành nằm tại [`BINANCE_USDM_QUARTERLY_1M.md`](BINANCE_USDM_QUARTERLY_1M.md).

### 1.3c Binance Spot 1m

Binance Spot 1m hiện được cấu hình mặc định cho `BTCUSDT` từ `2018-01-01`:

```text
storage/crypto/binance_spot/1m/symbol=BTCUSDT/year=YYYY/month=MM/part.csv.gz
```

Collector [`collectors/binance_spot_1m.py`](collectors/binance_spot_1m.py) sync theo thứ tự: Binance Vision spot monthly ZIP -> Vision spot daily ZIP đoạn gần hiện tại -> Binance Spot REST `/api/v3/klines` để bù tail tới candle đã đóng mới nhất. Append luôn dedupe theo `symbol,time`, đọc tail storage để resume, và audit continuity 1 phút khi validation chạy.

Với BTCUSDT spot, một số gap lịch sử official spot không có kline/trade/aggTrade. Theo policy backtest đã duyệt ngày `2026-07-11`, collector được phép fill các gap mà local Binance USD-M Futures cover đủ từng phút vào cùng raw spot storage với `source=binance_usdm_futures_proxy_gap_fill`. OHLC lấy từ futures cùng phút; volume/trade count được scale theo median tỷ lệ spot/futures quanh gap. Các gap không có futures coverage, chủ yếu 2018/2019, được giữ nguyên. Chi tiết vận hành nằm tại [`BINANCE_SPOT_1M.md`](BINANCE_SPOT_1M.md).

### 1.3d Binance USD-M Order Book Snapshot 1h

Order book snapshot 1h hiện được cấu hình cho `BTCUSDT` perpetual và active BTCUSDT quarterly contracts:

```text
storage/crypto/binance_orderbook_snapshot/1h/symbol=BTCUSDT/year=YYYY/month=MM/part.csv.gz
```

Collector [`collectors/binance_orderbook_snapshot_1h.py`](collectors/binance_orderbook_snapshot_1h.py) seed rolling `30 days` từ Binance Vision USD-M `daily/bookDepth`, downsample về bucket 1h, rồi gọi REST `/fapi/v1/depth` với `depth_limit=20` để append giờ hiện tại. Service live tự quét lại rolling window và tự bù các ngày Vision publish trễ; không cần chạy tay lại nếu container vẫn chạy và không bật `--no-vision`. Feature chính gồm `bid_depth_1pct`, `ask_depth_1pct`, `q_bid_depth_1pct`, `q_ask_depth_1pct`; trong đó `q_` nghĩa là quote-notional depth, không phải quarterly. Quarterly được xác định bằng `symbol`/`contract_type`. Các band đang bật là `0.2%`, `1%`, `2%`, `5%`, với `1%` là primary band. Chi tiết vận hành nằm tại [`BINANCE_ORDERBOOK_SNAPSHOT_1H.md`](BINANCE_ORDERBOOK_SNAPSHOT_1H.md).

### 1.3e Binance USD-M Futures Metrics 5m

Binance Futures Metrics 5m lưu open interest và long/short ratios từ cùng một file Binance Vision `daily/metrics`:

```text
storage/crypto/binance_futures_metrics/5m/symbol=BTCUSDT/year=YYYY/month=MM/part.csv.gz
```

Collector [`collectors/binance_futures_metrics_5m.py`](collectors/binance_futures_metrics_5m.py) chuẩn hoá dữ liệu legacy BTC/ETH nếu có, discover earliest/latest keys có thật trên Binance Vision `data/futures/um/daily/metrics/{SYMBOL}/`, rồi repair coverage toàn range cho `BTCUSDT`, `ETHUSDT`, nhóm relation symbols (`LINKUSDT`, `ARBUSDT`, `OPUSDT`, `POLUSDT`, `AAVEUSDT`) và active BTC/ETH quarterly contracts. Một canonical dataset chứa cả `sum_open_interest`, `sum_open_interest_value`, `count_toptrader_long_short_ratio`, `sum_toptrader_long_short_ratio`, `count_long_short_ratio`, `sum_taker_long_short_vol_ratio`; downstream có thể gọi helper loader để lấy riêng OI hoặc ratios. Perpetual symbols có thêm REST tail vài ngày cuối nếu Vision publish trễ. Chi tiết vận hành nằm tại [`BINANCE_FUTURES_METRICS_5M.md`](BINANCE_FUTURES_METRICS_5M.md).

### 1.4 Cấu trúc Ma trận Dữ liệu Ngày VN (VN Daily Matrix)

VN Daily Matrix được build từ canonical storage `storage/vn/equity/1d/` sang `storage/vn/equity/daily_matrix/`, gồm 5 ma trận `open/high/low/close/volume` cùng schema pivot: index là ngày giao dịch, columns là mã cổ phiếu.

Universe nằm tại `configs/symbols.vn_daily.yml`, hiện là curated large/liquid seed từ legacy HOSE/VN30/VN100/VN200-style universe. Thứ tự cột ưu tiên nhóm VN30/large-cap ở đầu. Matrix builder không tạo cột rỗng: mã nào vendor/storage chưa có dữ liệu sẽ được ghi vào `state/vn_daily_matrix_symbols.json` trong `missing_symbols`.

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
- **Binance Daily Matrix**: Chạy hàng ngày lúc **00:05** (Giờ UTC), chỉ ghi nến daily đã đóng hoàn toàn. Khi file hiện có bị thiếu phần đầu hoặc có gap nội bộ, service sẽ đọc matrix hiện tại, xác định điểm cần backfill theo từng symbol, fetch bù từ mốc thiếu rồi merge/dedupe vào storage.
- **Crypto 1m Live**: Chạy cập nhật liên tục mỗi phút để thu thập nến crypto thời gian thực.
- **Binance USD-M Quarterly 1m**: Chạy định kỳ, dùng Binance Vision để kéo lịch sử quarterly dài nhất có thể và REST để bù active tail.
- **Binance Order Book Snapshot 1h**: Chạy mỗi giờ, seed/lookback 30 ngày từ Binance Vision `bookDepth`, rồi REST append snapshot hiện tại.
- **Binance Futures Metrics 5m**: Chạy định kỳ, dùng Binance Vision `daily/metrics`; mỗi vòng đều scan coverage từ earliest available key để phát hiện/fix missing hoặc partial days, sau đó REST tail cho perpetual nếu Vision publish trễ.
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

# Chạy một lần để backfill/cập nhật ma trận Binance Daily từ 2020-01-01
PYTHONPATH=. python -m collectors.binance_daily_matrix --mode once --backfill-start 2020-01-01

# Chạy một lần để đồng bộ USD-M quarterly contracts lịch sử/current
PYTHONPATH=. python -m collectors.binance_usdm_quarterly_1m --mode once

# Chạy một lần để seed 30 ngày order book snapshot 1h và append REST snapshot hiện tại
PYTHONPATH=. python -m collectors.binance_orderbook_snapshot_1h --mode once

# Chạy một lần để chuẩn hoá futures metrics 5m
PYTHONPATH=. python -m collectors.binance_futures_metrics_5m --mode once
```

### 4.4 Audit continuity & repair crypto gaps

Với crypto 1m, audit phải nối toàn bộ partition của từng symbol rồi mới kiểm tra gap. Không được chỉ kiểm tra từng `part.csv.gz` riêng lẻ vì sẽ bỏ sót gap giữa các partition.

```bash
# Quét toàn bộ symbols crypto trong storage
PYTHONPATH=. python -m collectors.audit_continuity --dataset crypto --all-symbols

# Dry-run: chỉ in các đoạn thiếu, chưa gọi Binance
PYTHONPATH=. python -m collectors.fill_crypto_gaps --dry-run

# Repair: gọi Binance đúng các đoạn thiếu và append/dedupe vào storage
PYTHONPATH=. python -m collectors.fill_crypto_gaps
```

Chi tiết incident gap `2026-05-01 -> 2026-06-06` và kết quả repair được ghi tại [`CONTINUITY_REPAIR_2026-06-12.md`](CONTINUITY_REPAIR_2026-06-12.md).

Chi tiết incident Binance Daily Matrix chỉ có dữ liệu từ `2026-06-01` và kết quả backfill lại từ `2020-01-01` được ghi tại [`BINANCE_DAILY_MATRIX_REPAIR_2026-06-16.md`](BINANCE_DAILY_MATRIX_REPAIR_2026-06-16.md).

---

## 5. Tổ chức mã nguồn

- **`data_loader.py`**: Module bộ nạp dữ liệu thống nhất (Unified Data Loader) cho các Alpha hoặc hệ thống khác import và gọi nạp dữ liệu.
- **`collectors/`**: Chứa mã nguồn của các services chính.
- **`collectors/common/`**: Thư viện dùng chung quản lý lịch, phân vùng lưu trữ, ghi tệp tin an toàn (atomic write), lock chống tranh chấp, retry và manifest.
- **`configs/`**: Tệp tin cấu hình YAML chứa danh sách symbol cần chạy.
- **`tests/`**: Các script kiểm thử (bao gồm unit tests cho data loader và smoke tests cho nguồn dữ liệu).
- **`legacy/`**: Thư mục chứa các script và notebook kiểm thử cũ đã ngừng hoạt động (được cấu hình loại trừ trong `.gitignore`).

---

## 6. Bộ nạp dữ liệu SDK thống nhất (Unified Data Loader SDK)

Để thuận tiện cho các mô hình Alpha hoặc các dự án khác sử dụng dữ liệu trực tiếp mà không cần tự xử lý cấu trúc phân vùng phức tạp hoặc cấu trúc ma trận xoay của storage mới, hệ thống cung cấp một SDK nạp dữ liệu thống nhất **[`data_loader.py`](data_loader.py)** tại thư mục gốc.

Các lớp trong SDK tự động định tuyến (route) đường dẫn, ghép nối các tệp phân vùng, loại bỏ dữ liệu trùng, sắp xếp theo trình tự thời gian và thực hiện ép kiểu/chuẩn hoá timezone về naive datetime tương ứng.

---

### 6.1 Hướng dẫn Bind Mount & Import từ Thư mục Khác

Khi chạy mô hình Alpha ở một thư mục độc lập khác trên hệ thống (hoặc trong một container khác), bạn thực hiện cấu hình theo các bước sau:

#### Bước 1: Bind Mount Thư mục `_get_data`
Cấu hình trong `docker-compose.yml` của dự án Alpha của bạn hoặc chạy lệnh docker run để mount thư mục `/root/bobby/pool_alpha/alphas_storage/_get_data` vào một đường dẫn bên trong container (ví dụ: `/app/market_data_sdk`):

```yaml
# docker-compose.yml của dự án Alpha
services:
  my-alpha-model:
    image: my-alpha-image:latest
    volumes:
      - /root/bobby/pool_alpha/alphas_storage/_get_data:/app/market_data_sdk
      # (Gắn thêm các thư mục code hoặc data khác của Alpha...)
```

#### Bước 2: Import SDK trong Python
Bất kể bạn gọi file python từ đâu, bạn có thể nạp động SDK này vào `sys.path` hoặc sử dụng biến môi trường `PYTHONPATH`.

```python
import sys
from pathlib import Path

# Thêm đường dẫn mount của SDK vào đầu sys.path
sys.path.insert(0, "/app/market_data_sdk")

# Giờ bạn có thể import các Class Loader trực tiếp
from data_loader import VnStock1m, CryptoDailyMatrix, load_data
```

Vì SDK sử dụng `Path(__file__).parent` để tự động định vị các tệp tin lưu trữ vật lý, việc định vị file sẽ tự động hoạt động chính xác tại thư mục được mount.

---

### 6.2 Các Class Loader Endpoints & Cách Sử Dụng

SDK cung cấp các Class Reader chuyên biệt cho từng nguồn dữ liệu:

| Lớp (Reader Class) | Dataset Target | Múi giờ Timezone (Naive Datetime) |
| :--- | :--- | :--- |
| `VnStock1m` | Dữ liệu nến 1m cổ phiếu Việt Nam | Múi giờ Việt Nam `Asia/Ho_Chi_Minh` |
| `VnStockDaily` | Dữ liệu nến 1d (daily) cổ phiếu Việt Nam | Múi giờ Việt Nam `Asia/Ho_Chi_Minh` |
| `VNDailyMatrix` | Dữ liệu Matrix xoay ngày cổ phiếu Việt Nam | Múi giờ Việt Nam `Asia/Ho_Chi_Minh` |
| `VnFutures1m` | Dữ liệu nến 1m phái sinh Việt Nam | Múi giờ Việt Nam `Asia/Ho_Chi_Minh` |
| `CryptoBinance1m` | Dữ liệu nến 1m Binance Futures | Múi giờ quốc tế `UTC` |
| `CryptoBinanceQuarterly1m` | Alias đọc concrete USD-M quarterly contracts trong cùng Binance Futures storage | Múi giờ quốc tế `UTC` |
| `CryptoBinanceSpot1m` | Dữ liệu nến 1m Binance Spot, mặc định hiện có `BTCUSDT` từ `2018-01-01` | Múi giờ quốc tế `UTC` |
| `BinanceOrderBookSnapshot1h` | Feature snapshot order book USD-M Futures 1h (`bid_depth_1pct`, `q_bid_depth_1pct`, ...) | Múi giờ quốc tế `UTC` |
| `BinanceFuturesMetrics5m` | Futures metrics 5m: open interest, top/global long-short ratios, taker long-short volume ratio | Múi giờ quốc tế `UTC` |
| `CryptoDailyMatrix` | Dữ liệu Matrix xoay ngày Binance | Múi giờ quốc tế `UTC` |
| `BinanceOptions5m` | Dữ liệu snapshot option 5m Binance | Múi giờ quốc tế `UTC` |

#### Ví dụ gọi các Endpoint cụ thể:

```python
from data_loader import VnStock1m, VnStockDaily, VNDailyMatrix, CryptoDailyMatrix, CryptoBinanceQuarterly1m, CryptoBinanceSpot1m, BinanceOrderBookSnapshot1h, BinanceFuturesMetrics5m, load_data

# 1. Gọi dữ liệu nến 1m của FPT & ACB trong 1 khoảng thời gian, giới hạn 1000 dòng
df_1m = VnStock1m().load(
    symbols=["FPT", "ACB"],
    start_date="2026-06-01",
    end_date="2026-06-07",
    limit=1000,
    check_val=True
)

# 2. Gọi dữ liệu Daily Stock của tất cả các mã Việt Nam
df_daily_all = VnStockDaily().load(symbols=None)

# 3. Gọi dữ liệu Matrix xoay đóng cửa (Close Price) của Top 400 Crypto
# CryptoDailyMatrix trả về index=time, columns=symbols
df_matrix = CryptoDailyMatrix().load(
    feature="close",
    symbols=["BTCUSDT", "ETHUSDT"],
    start_date="2020-01-01",
    limit=100
)

# 4. Gọi thẳng format data_dict tương thích pipeline chiến lược cũ
# data_dict["BTCUSDT"] => DataFrame index=datetime, columns=open/high/low/close/volume
data_dict = CryptoDailyMatrix().load_ohlcv(
    symbols=None,
    start_date="2020-01-01",
    check_val=True,
)

# 5. Gọi VN daily matrix theo feature
vn_close = VNDailyMatrix().load(
    feature="close",
    symbols=["FPT", "VCB", "HPG"],
    start_date="2016-01-01",
)

# 6. Gọi VN daily theo format data_dict tương thích pipeline chiến lược
vn_data_dict = VNDailyMatrix().load_ohlcv(
    symbols=None,
    start_date="2016-01-01",
    check_val=True,
)

# 7. Gọi dữ liệu Binance USD-M quarterly concrete contracts
# Dữ liệu nằm chung storage với Binance futures 1m, symbol có dạng BTCUSDT_YYMMDD / ETHUSDT_YYMMDD
quarterly_df = CryptoBinanceQuarterly1m().load(
    symbols=["BTCUSDT_240329", "ETHUSDT_260925"],
    start_date="2024-01-01",
    end_date="2026-07-04 10:00:00",
    check_val=True,
)

# Có thể gọi qua router nếu service khác muốn truyền dataset name động
quarterly_df_2 = load_data(
    "binance_usdm_quarterly_1m",
    symbols="BTCUSDT_261225",
    start_date="2026-06-26",
    check_val=True,
)

# 8. Gọi dữ liệu Binance Spot BTCUSDT 1m
# Cột source phân biệt official spot và các dòng futures proxy được duyệt để fill gap backtest.
spot_df = CryptoBinanceSpot1m().load(
    symbols="BTCUSDT",
    start_date="2018-01-01",
    check_val=True,
)

# Alias router tương đương cho service khác
spot_df_2 = load_data(
    "binance_spot_1m",
    symbols="BTCUSDT",
    start_date="2018-01-01",
    check_val=True,
)

# 9. Gọi feature order book snapshot 1h
orderbook_df = BinanceOrderBookSnapshot1h().load_features(
    symbols=["BTCUSDT", "BTCUSDT_260925"],
    start_date="2026-06-13",
    check_val=True,
)

orderbook_df_2 = load_data(
    "binance_orderbook_snapshot_1h",
    symbols="BTCUSDT",
    start_date="2026-06-13",
    check_val=True,
)

# 10. Gọi futures metrics 5m
metrics_df = BinanceFuturesMetrics5m().load(
    symbols=["BTCUSDT", "ETHUSDT", "LINKUSDT"],
    check_val=True,
)
oi_df = BinanceFuturesMetrics5m().load_open_interest(symbols="BTCUSDT")
ratios_df = BinanceFuturesMetrics5m().load_long_short_ratios(symbols="BTCUSDT")
```

**Các tham số chung hỗ trợ:**
- `symbols`: Mã hoặc danh sách mã (ví dụ: `"FPT"`, `["FPT", "ACB"]`). Truyền `None` để tự động quét và tải toàn bộ mã hiện có.
- `start_date` / `end_date`: Lọc khoảng thời gian (định dạng `YYYY-MM-DD` hoặc `YYYY-MM-DD HH:MM:SS`).
- `limit`: Giới hạn tối đa số dòng trả về (lấy `N` dòng đầu tiên từ kết quả đã sắp xếp).
- `check_val`: Mặc định `True`, tự động chạy hàm kiểm tra tính hợp lệ và cảnh báo lỗi logic dữ liệu.

---

### 6.3 Hàm Kiểm Tra & Validate dữ liệu (`validate_data`)

Module cung cấp hàm `validate_data(df, dataset)` được tích hợp sẵn để kiểm tra tính toàn vẹn và hợp lệ:

```python
from data_loader import VnStock1m, validate_data

df = VnStock1m().load("FPT")
report = validate_data(df, "vn_stock_1m")

print("Valid:", report["valid"])
print("Tổng số dòng:", report["row_count"])
if not report["valid"]:
    print("Danh sách lỗi phát hiện:", report["errors"])
```

**Các kiểm tra được thực hiện:**
- Tính đầy đủ của các cột bắt buộc (`time`, `symbol`, `open`, `high`, `low`, `close`, `volume`).
- Phát hiện các giá trị âm hoặc null không hợp lệ.
- Kiểm tra tính logic của nến: `high >= open`, `high >= close`, `low <= open`, `low <= close`.
- Đảm bảo thời gian tăng dần liên tục (monotonic) và không bị trùng lặp thời gian trên mỗi symbol.
