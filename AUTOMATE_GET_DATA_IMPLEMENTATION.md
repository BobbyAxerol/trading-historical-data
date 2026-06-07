# Automate Get Data Implementation

## Muc tieu

Bien cac script lay data dang chay roi rac trong `_get_data` thanh mot cum services Docker doc lap, chay ben bang `restart: unless-stopped`, co storage chung, co state/manifest ro rang, append thong minh, retry/fallback cu the, va co lich update phu hop voi tung loai du lieu.

Pham vi da doc: toan bo thu muc `/root/bobby/pool_alpha/alphas_storage/_get_data`, bo qua `.ipynb`. Thu muc nay hien co cac script Python, data da luu san, logs, va DNSE OpenAPI SDK vendored.

## Hien trang code

| File | Vai tro hien tai | Van de chinh | Nen tai su dung |
| --- | --- | --- | --- |
| `get_crypto_1m.py` | One-shot download Binance Futures 1m cho `BNBUSDT`, `SOLUSDT`, `BTCUSDT`, `ETHUSDT`, `DOGEUSDT` tu 2020 | `DATA_DIR` khai bao nhung khong dung, filename luu theo cwd, khong incremental, khong retry/backfill window ro, khong chay continuous | Tai su dung logic Binance async + symbol list, viet lai thanh service backfill/live |
| `get_multiple_stock_1d.py` | Download VN daily universe bang `vnstock.explorer.vci.Quote`; co rate limit 18 req/min, concurrency 2, incremental theo last `time` | Duong dan default trong `main()` dang tro sang `/root/bobby/pool_alpha/alphas_storage/data_stock`, lech voi storage trong `_get_data/data_stock`; `_save()` moi lan merge full file | Nen giu lam service daily scheduler, sua path + state + atomic write |
| `get_stock_vnstock.py` | Intraday VN 1m collector bang vnstock source `KBS`, co trading-hour loop, validate OHLC, retry, log rotate, split stocks/futures | Path default cung lech ra ngoai `_get_data`; moi append doc full file CSV.GZ nen se nang khi 1m history lon; holiday hard-code | Nen giu trading calendar/retry/validate, tach thanh service intraday stocks; thay save bang partition/month + manifest |
| `dnse_intraday_collector.py` | DNSE OHLC historical collector tong quat, co HMAC qua SDK, rate limiter 900 req/hour va 9000 req/day, chunking, resume | `_get_existing_range()` doc gzip bang `readlines()` nen ton RAM khi file lon; save merge full file; chua co continuous loop | Nen giu DNSE auth/rate limiter/fetch; dung cho backfill hoac fallback stock/futures |
| `get_vn30f1m_dnse.py` | Service rieng cho `VN30F1M` 1m qua DNSE `/price/ohlc`, co HMAC, retry, trading calendar, loop 1 phut, save parquet zstd | Luu path ngoai `_get_data`; chi mot symbol; doc/ghi full parquet moi lan; holiday hard-code | Tot nhat de lam service futures live sau khi sua storage + partition/atomic write |
| `fetch_option_data_all.py` | Binance Options snapshot, filter BTC near expiries theo delta, save `options_full_history.csv.gz`, loop hourly | `DATA_DIR` khong dung, fixed expiry hard-code, chi BTC, hourly trong khi muc tieu can 5m, merge full file | Dung lam prototype; viet service `options-snapshot` moi |
| `get_stock_dnse.py` | Test max range DNSE cho vai symbol | Chi la test | Giu trong `experiments/` hoac `tests/manual/` |
| `test_all_endpoint_dnse.py` | Test REST + WebSocket DNSE endpoints | Chi la integration test thu cong | Giu lam manual smoke test |
| `openapi_sdk/` | DNSE SDK Python/Javascript, REST + websocket marketdata | Vendored SDK, co retry websocket co ban | Giu vendored hoac pin package neu co tren PyPI |

Storage hien tai:

- Daily VN: `_get_data/data_stock/*_1d_max.csv.gz`, khoang 16 MB.
- Intraday VN: `_get_data/data_stock/_intraday_storage/stocks/*_1m.csv.gz` va futures `VN30F1M_1m.csv.gz`/`VN30F1M_1m.parquet`.
- Crypto 1m: `_get_data/crypto_1m_data/*_perpetual_1m.csv.gz`, khoang 488 MB cho 4 symbols lon.
- Logs: `_get_data/logs/intraday_collector.log*`.

## Data products can build ngay

### 1. Crypto Binance Futures 1m

Muc tieu:

- Universe ban dau: `BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `BNBUSDT`, `DOGEUSDT`.
- Backfill toi da tu `2020-01-01` hoac tu ngay Binance co data thuc te.
- Non-FIFO: khong cat bot history cu.
- Live update moi phut hoac moi 60-75 giay de tranh candle chua dong.
- Source: Binance USD-M Futures kline (`/fapi/v1/klines`) hoac `python-binance` voi `HistoricalKlinesType.FUTURES`.

Service:

- `crypto-1m-backfill`: one-shot, chay theo profile/manual de keo lai history dai.
- `crypto-1m-live`: daemon, moi vong doc manifest latest timestamp, fetch tu `latest - overlap` den candle da dong, append va dedupe.

### 2. VN daily universe 1d

Muc tieu:

- Universe tu `HOSE_300_SYMBOLS` trong `get_multiple_stock_1d.py`.
- Update daily sau khi thi truong dong, tot nhat 16:30 Asia/Ho_Chi_Minh hoac 06:00 ngay hom sau de tranh vendor cham.
- Backfill max tu `2016-01-01`.
- Append non-FIFO, dedupe theo `time`.

Service:

- `vn-daily`: daemon scheduler chay 1 lan/ngay, co the chay catch-up khi container restart.
- Source primary: `vnstock.explorer.vci.Quote`.
- Fallback: neu VCI loi/rate-limit dai, bo qua symbol va retry lan sau; khong nen dung source khac tu dong cho 1d neu schema/adjustment khac nhau, tru khi co co `source` trong metadata.

### 3. VN intraday 1m universe

Muc tieu:

- Stocks universe ban dau: VN30/liquid list trong `get_stock_vnstock.py`.
- Futures: `VN30F1M`, sau nay mo rong `VN30F2M`, `VN30F1Q`, `VN30F2Q`.
- Backfill max theo kha nang vendor, khong FIFO.
- Trong gio giao dich VN, update lien tuc; ngoai gio thi sleep toi phien tiep theo.

Service:

- `vn-intraday-stocks`: dung vnstock/KBS cho stocks. Moi symbol fetch incremental theo chunk nho, chi append candle moi.
- `vn30f1m-dnse`: dung DNSE REST `/price/ohlc` cho futures. Logic trong `get_vn30f1m_dnse.py` la nen tang tot nhat hien co.
- `vn-intraday-backfill`: one-shot/manual de keo history dai bang DNSE hoac vnstock.

Fallback:

- Stocks primary `vnstock/KBS`, fallback `DNSE` neu co `DNSE_API_KEY` va `DNSE_API_SECRET_KEY`.
- Futures primary `DNSE`, fallback `vnstock/KBS` neu DNSE het quota hoac loi dai.
- Moi candle can co `source` trong manifest hoac partition metadata. Khong tron cung mot file neu source co cach adjust gia khac nhau ma khong danh dau.

### 4. Options data

Muc tieu:

- Can snapshot option nhanh cho chien luoc option, tan suat 5m la chap nhan duoc.
- Free/low-cost uu tien hon coverage tuyet doi.

Khuyen nghi:

- Primary: Binance Options snapshot/mark/price cho BTC va ETH. Service moi nen lay danh sach expiry/symbol dong tu exchange, khong hard-code expiry nhu `fetch_option_data_all.py`.
- Tan suat: 5 phut, round `snapshot_time` ve moc 5m.
- Loc ban dau: near expiries 2-4 ky gan nhat, delta abs trong `[0.15, 0.9]`, optional moneyness quanh spot.
- Luu snapshot full chain da filter, khong overwrite history.
- Bo sung neu can: option klines 5m/15m cho contracts duoc trade nhieu de backtest gia option theo thoi gian.

Danh gia provider:

- Binance Options: hop nhat cho muc tieu free/nhanh/tu dong. Dang co code dung `options_mark_price()` va `options_price()`. Nen bo fixed expiries va chuyen sang dynamic discovery.
- CME: chinh thong hon cho futures options va co 5-minute Greeks/IV snapshots, historical 5 nam, REST/streaming, nhung la san pham data can licensing/access. Khong coi la free fallback mac dinh.
- Yahoo/yfinance: dung duoc cho snapshot option chains co do tre, phu hop tham khao/low-stakes. Khong nen lam primary trading-grade vi Yahoo Finance data co disclaimer khong dung cho trading/investing va co gioi han redistribution; OPRA tren Yahoo co delay 15 phut.

Nguon da kiem tra:

- Local code: `fetch_option_data_all.py` dang goi `python-binance` `options_mark_price()` va `options_price()`. `python-binance` cung expose Options kline endpoint `/eapi/v1/klines`: https://python-binance.readthedocs.io/en/latest/_modules/binance/client.html
- CME Market Data APIs: https://www.cmegroup.com/market-data/market-data-api.html
- CME Greeks and Implied Volatility snapshots/historical: https://www.cmegroup.com/market-data/greeks-and-implied-volatility-data.html
- Yahoo Finance data delay/disclaimer: https://help.yahoo.com/kb/SLN2310.html
- yfinance API reference: https://ranaroussi.github.io/yfinance/reference/index.html

## Kien truc de xuat

Khong can warehouse phuc tap. Chi can mot "storage lake" nhe, co quy uoc path va manifest.

Nguon doc chuan sau refactor la duy nhat:

```text
/root/bobby/pool_alpha/alphas_storage/_get_data/storage
```

Nhung file cu trong `crypto_1m_data/` va `data_stock/` khong phai la noi query chinh nua. Chung chi la input de seed/migrate history vao `storage/`, giup collector biet lich su da co den dau va sau do tiep tuc append thong minh.

```
_get_data/
  collectors/
    common/
      storage.py
      retry.py
      calendar_vn.py
      manifest.py
      logging.py
    crypto_1m.py
    vn_daily.py
    vn_intraday_vnstock.py
    vn_intraday_dnse.py
    binance_options_snapshot.py
    seed_storage_from_existing.py
  configs/
    symbols.crypto.yml
    symbols.vn_daily.yml
    symbols.vn_intraday.yml
    options.yml
  storage/
    crypto/binance_futures/1m/symbol=BTCUSDT/year=2026/month=06/part.csv.gz
    vn/equity/1d/symbol=FPT/year=2026/part.csv.gz
    vn/equity/1m/symbol=FPT/year=2026/month=06/part.csv.gz
    vn/futures/1m/symbol=VN30F1M/year=2026/month=06/part.csv.gz
    options/binance/snapshot_5m/underlying=BTC/year=2026/month=06/part.csv.gz
  state/
    manifests/
      crypto_1m.json
      vn_daily.json
      vn_intraday.json
      options_snapshot.json
    locks/
  logs/
  docker/
    Dockerfile
    docker-compose.yml
```

## History-first policy da implement

Collector live khong duoc coi manifest 2026 la su that duy nhat. Policy hien tai:

1. Seed/migrate file cu vao canonical storage truoc bang `collectors.seed_storage_from_existing`.
2. Moi append vao storage deu dedupe theo natural key, ghi atomic, va khong lam giam `latest_time`.
3. Khi service live chay, no doc `state`, tail cua storage, va tail file cu de resume an toan. Sau khi seed xong, duong query/backtest chi can doc storage.
4. Symbol moi khong co file cu, vi du `DOGEUSDT`, crypto live se bat dau tu `backfill_start` thay vi chi lay vai gio gan nhat.
5. Audit mac dinh chi kiem tra storage. Neu muon doi chieu file cu thi them `--include-existing-files`.

Lenh seed idempotent:

```bash
run-py -m collectors.seed_storage_from_existing --dataset all
```

Hoac bang Docker chung image:

```bash
docker compose --profile bootstrap run --rm seed-existing-history
```

Ket qua seed da chay ngay 2026-06-06:

- Crypto Binance futures 1m: 4 file, 12,891,357 dong normalized vao storage.
- VN daily: 272 file, 616,077 dong normalized.
- VN intraday stocks: 29 file, 903,256 dong normalized.
- VN futures: 2 file, 132,714 dong normalized.
- Options old files: 0 file; Binance options hien tai la snapshot-forward tu luc service chay.

Audit storage-only sau seed:

- `BTCUSDT`: 2020-01-01 00:00:00 -> 2026-06-06 12:28:00, gap 1m = 0.
- `ETHUSDT`: 2020-01-01 00:00:00 -> 2026-06-06 12:28:00, gap 1m = 0.
- `SOLUSDT`: 2020-09-14 07:00:00 -> 2026-06-06 12:29:00, gap 1m = 0.
- `BNBUSDT`: 2020-02-10 08:01:00 -> 2026-06-06 12:29:00, gap 1m = 0.
- VN daily sample `FPT`, `VCB`, `HPG`: 2015-07-16 -> 2026-06-05.
- VN intraday sample `FPT`, `ACB`, `VCB`: den 2026-05-21 14:40:00.
- VN futures `VN30F1M`: 2024-05-02 09:00:00 -> 2026-05-22 11:28:00.
- Options BTC/ETH snapshot: 2026-06-06 10:30:00 -> 2026-06-06 12:30:00, gap 5m = 0.

Neu muon it doi cho script cu, co the dat storage vao `_get_data/data_stock` va `_get_data/crypto_1m_data` tiep. Nhung nen them `state/manifests` va partition moi cho 1m/options de tranh doc/ghi full file ngay cang nang.

## Docker Compose hien tai

Tat ca service get-data dung chung mot image `get_data-collectors:latest`. Chi `crypto-1m-live` khai bao `build:` de build image; cac service con lai reuse image do, tranh moi job mot image.

```yaml
name: get_data

x-get-data-service: &get-data-service
  image: get_data-collectors:latest
  restart: unless-stopped
  volumes:
    - ./storage:/app/storage
    - ./state:/app/state
    - ./logs:/app/logs

services:
  crypto-1m-live:
    <<: *get-data-service
    build:
      context: .
      dockerfile: docker/Dockerfile
    command: ["python", "-m", "collectors.crypto_1m", "--mode", "live"]
    environment:
      TZ: UTC
      DATA_ROOT: /app/storage
      STATE_ROOT: /app/state
      LOG_ROOT: /app/logs

  vn-daily:
    <<: *get-data-service
    command: ["python", "-m", "collectors.vn_daily", "--schedule", "16:30"]
    environment:
      TZ: Asia/Ho_Chi_Minh
      DATA_ROOT: /app/storage
      STATE_ROOT: /app/state
      LOG_ROOT: /app/logs

  vn-intraday-stocks:
    <<: *get-data-service
    command: ["python", "-m", "collectors.vn_intraday_vnstock", "--mode", "live"]
    environment:
      TZ: Asia/Ho_Chi_Minh
      DATA_ROOT: /app/storage
      STATE_ROOT: /app/state
      LOG_ROOT: /app/logs

  vn30f1m-dnse:
    <<: *get-data-service
    command: ["python", "-m", "collectors.vn_intraday_dnse", "--symbols", "VN30F1M", "--mode", "live"]
    environment:
      TZ: Asia/Ho_Chi_Minh
      DATA_ROOT: /app/storage
      STATE_ROOT: /app/state
      LOG_ROOT: /app/logs

  options-binance-5m:
    <<: *get-data-service
    command: ["python", "-m", "collectors.binance_options_snapshot", "--mode", "live", "--interval-minutes", "5"]
    environment:
      TZ: UTC
      DATA_ROOT: /app/storage
      STATE_ROOT: /app/state
      LOG_ROOT: /app/logs

  seed-existing-history:
    <<: *get-data-service
    profiles: ["bootstrap"]
    restart: "no"
    command: ["python", "-m", "collectors.seed_storage_from_existing", "--dataset", "all"]
```

Dockerfile toi thieu:

```dockerfile
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc g++ curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml poetry.lock* /app/
RUN pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --only main --no-interaction --no-ansi

COPY . /app
```

Neu build tu `_get_data` rieng, nen tao `requirements.txt` gon:

```text
pandas
pyarrow
python-dotenv
python-binance
vnstock
requests
urllib3
websockets
msgpack
certifi
PyYAML
filelock
```

## Storage design

Nguyen tac:

- Moi dataset co key rieng: `provider`, `market`, `asset_type`, `interval`, `symbol`, `year`, `month`.
- Daily partition theo year la du; intraday 1m va option snapshots partition theo month.
- Moi append chi doc partition hien tai va mot overlap ngan, khong doc toan bo history.
- Write atomic: ghi vao `.tmp`, fsync/close, rename vao `part.csv.gz`.
- Dedupe theo natural key:
  - OHLCV: `symbol + time`.
  - Binance futures: `symbol + open_time`.
  - Options snapshot: `snapshot_time + option_symbol`.
- Manifest luu `latest_time`, `first_time`, `row_count_estimate`, `last_success_at`, `last_error`, `source`, `schema_version`.
- Lock theo dataset/symbol de tranh hai container cung ghi mot file.

Manifest example:

```json
{
  "dataset": "crypto_binance_futures_1m",
  "symbols": {
    "BTCUSDT": {
      "first_time": "2020-01-01T00:00:00Z",
      "latest_time": "2026-06-06T09:58:00Z",
      "last_success_at": "2026-06-06T10:00:08Z",
      "source": "binance_futures",
      "schema_version": 1
    }
  }
}
```

## Append thong minh

Pseudo-flow chung cho moi symbol:

```text
1. acquire lock dataset/symbol
2. read manifest latest_time
3. from_time = latest_time - overlap
   - crypto/options: overlap 5-15 minutes
   - VN 1m: overlap 1 trading day neu vendor hay sua candle cuoi
   - VN 1d: overlap 5 trading days de bat adjustment/split
4. fetch [from_time, closed_until]
5. normalize schema + timezone
6. validate OHLC/volume/timestamps
7. group rows by partition
8. for each partition:
   - read existing partition only
   - concat
   - drop duplicates by key, keep last
   - sort
   - atomic write
9. update manifest only after write success
10. release lock
```

## Retry, fallback, va health

Retry policy:

- Network/5xx: exponential backoff + jitter, vi du 2s, 4s, 8s, 16s, max 5 phut.
- 429/rate-limit: ton trong header neu co; neu khong co thi dung local token bucket nhu `OHLCRateLimiter`.
- Data empty: khong coi la fatal neu ngoai gio giao dich hoac symbol moi niem yet; ghi warning vao manifest.
- Parse/schema error: fail symbol do, khong lam chet ca batch.

Fallback policy:

- Crypto futures: Binance primary. Neu Binance loi dai thi service sleep/backoff, khong nen fallback sang exchange khac trong cung dataset vi gia/volume khac.
- VN daily: VCI/vnstock primary. Neu muon fallback source khac thi ghi dataset/source rieng hoac them cot `source`.
- VN intraday stocks: vnstock/KBS primary, DNSE fallback co rate limit rieng.
- VN futures: DNSE primary, vnstock fallback.
- Options: Binance primary; Yahoo chi fallback snapshot tham khao; CME la premium path neu co license.

Health:

- Moi service ghi heartbeat vao `state/heartbeats/<service>.json` moi vong thanh cong hoac moi 60s.
- Health check doc heartbeat:
  - crypto live stale neu qua 5 phut khong update trong thoi gian binh thuong.
  - VN intraday stale neu trong gio giao dich qua 3 phut khong co heartbeat.
  - VN daily stale neu qua 36h chua success.
  - options stale neu qua 15 phut chua snapshot.
- Docker restart xu ly process crash. Logic service xu ly loi vendor bang backoff de khong crash loop lien tuc.

## Lich chay

| Dataset | Cach chay | Tan suat | Closed candle rule |
| --- | --- | --- | --- |
| Crypto futures 1m | Always-on | 60-75s | Chi lay candle co `close_time <= now - 5s` |
| VN daily 1d | Scheduler | 16:30 VN va catch-up luc start | End date la ngay giao dich gan nhat da dong |
| VN intraday stocks 1m | Trading-hour daemon | Moi 60s trong phien | Bo candle dang hinh thanh neu vendor tra ve |
| VN30F1M DNSE 1m | Trading-hour daemon | Moi 60-70s trong phien | Overlap 5 phut nhu script hien tai |
| Binance options snapshot | Always-on | Moi 5 phut | Round snapshot ve moc 5m |

Trading calendar VN nen nam trong `calendar_vn.py`, co:

- timezone `Asia/Ho_Chi_Minh`.
- session stocks: 09:00-11:30, 13:00-14:45.
- derivatives co the bat dau 08:45 nhu `get_vn30f1m_dnse.py`.
- holidays dua vao config de sua hang nam, khong hard-code trong collector.

## CSV.GZ vs Parquet vs Database

Ket luan thuc dung cho muc tieu cua ban: dung CSV.GZ partitioned cho storage chinh la on, dac biet neu khong can query nhanh va uu tien don gian/ben. Parquet nen dung cho file read-optimized phu/phien ban cache, khong bat buoc lam primary. Database chua can.

| Tieu chi | CSV.GZ | Parquet zstd/snappy | Database |
| --- | --- | --- | --- |
| Dung luong | Tot voi OHLCV text lap lai; crypto hien tai 100-142 MB/symbol tu 2020 kha on | Thuong nho hon CSV hoac tuong duong, dac biet numeric columnar | Phu thuoc engine, overhead lon hon file nen khong loi cho storage don gian |
| RAM khi append | Kem neu mot file duy nhat vi phai doc full de dedupe | Cung kem neu doc/ghi full file; tot hon neu partition | Tot neu append row-level/index, nhung van can maintenance |
| RAM khi load nghien cuu | CSV.GZ phai parse text, ton CPU; doc chunk duoc | Tot hon, column pruning duoc, load numeric nhanh hon | Tot cho query loc, nhung them service/backup |
| Toc do | Cham hon, nhung ban noi khong can nhanh | Nhanh hon cho backtest/query | Nhanh neu index dung |
| Don gian/debug | Rat tot, mo bang shell/pandas de doc | Tot nhung can pyarrow | Kem hon, can schema/migration/backup |
| Rui ro corrupt | Thap neu atomic write; file gzip corrupt la mat partition do | Thap neu atomic write; parquet metadata corrupt cung mat partition | Can backup/WAL |
| Append dung nghia | Gzip khong append + dedupe tot neu chi noi text; nen rewrite partition | Parquet cung nen rewrite partition | Tot nhat |

Khuyen nghi cu the:

- Daily VN: giu CSV.GZ, 1 file/symbol hoac partition year deu nhe.
- Crypto 1m: chuyen tu 1 file/symbol sang CSV.GZ partition theo month. File 488 MB hien tai se con nhe va append re hon.
- VN intraday 1m: CSV.GZ partition month/symbol. Khong nen 1 file duy nhat neu backfill nhieu nam.
- Options snapshot 5m: CSV.GZ partition month/underlying. Snapshot co nhieu cot va schema co the thay doi; CSV de debug hon.
- Parquet: tao cache optional bang job `compact-parquet` neu sau nay can backtest nhanh hon. Primary van la CSV.GZ de de cuu ho va it phu thuoc.
- Database: chi nen them khi can query API nhieu user, join/filter phuc tap, hoac dashboard realtime. Neu can nhe, DuckDB doc thang CSV.GZ/Parquet la du, khong can Postgres/ClickHouse.

## Migration tu file cu

1. Freeze script cu, khong cho ghi tiep vao path cu trong luc migrate.
2. Scan existing files:
   - `_get_data/data_stock/*_1d_max.csv.gz`
   - `_get_data/data_stock/_intraday_storage/**/*.csv.gz`
   - `_get_data/crypto_1m_data/*_perpetual_1m.csv.gz`
3. Normalize schema:
   - Standard OHLCV: `time,symbol,open,high,low,close,volume,source,ingested_at`.
   - Binance futures them: `quote_volume,number_of_trades,taker_buy_base_volume,taker_buy_quote_volume`.
   - Options snapshot: `snapshot_time,underlying,symbol,expiry,strike,type,spot,mark_price,bid,ask,delta,gamma,theta,vega,iv,volume,open_interest,source`.
4. Split theo partition month/year.
5. Tao manifest tu max timestamp moi symbol.
6. Start services voi overlap de bat phan missing sau migration.

## Trien khai theo phase

### Phase 1: on dinh storage + Docker

- Tao `collectors/common/storage.py` co atomic partition append.
- Tao `collectors/common/manifest.py`, `retry.py`, `calendar_vn.py`.
- Tao Dockerfile + compose.
- Sua duong dan ve `_get_data/storage`, `_get_data/state`, `_get_data/logs`.
- Chay `crypto-1m-live`, `vn-daily`, `vn30f1m-dnse`, `options-binance-5m` truoc.

### Phase 2: backfill va migrate

- Migrate crypto files cu sang partition.
- Migrate VN daily files cu.
- Migrate VN intraday CSV/parquet cu.
- Chay one-shot backfill cho missing gap.
- So sanh row count va latest timestamp voi file cu.

### Phase 3: intraday universe lon

- Bat `vn-intraday-stocks` cho VN30/liquid set.
- Gioi han rate:
  - vnstock guest: 18-20 req/min conservative.
  - DNSE: 900 req/hour safe nhu code hien co.
- Neu universe qua lon, chia thanh shards:
  - `vn-intraday-stocks-a`
  - `vn-intraday-stocks-b`
  - moi service mot symbol subset, khong trung lock.

### Phase 4: monitoring

- Them `healthcheck.py` doc heartbeat/manifest.
- Them summary command:
  - latest per dataset/symbol.
  - stale symbols.
  - file size per partition.
  - error counters.
- Optionally push log/heartbeat ra Telegram/Discord neu can.

## Acceptance checklist

- `docker compose up -d` start duoc cac services, restart policy la `unless-stopped`.
- Tat server/container roi bat lai, service resume tu manifest, khong mat data.
- Khong service nao doc toan bo history moi phut.
- Moi write la atomic va dedupe theo natural key.
- Daily VN update sau market close va catch-up neu miss.
- Crypto 1m latest khong stale qua 5 phut trong dieu kien Binance available.
- VN intraday khong fetch ngoai gio giao dich, sleep toi phien tiep theo.
- Options snapshot co moc 5m deu va khong hard-code expiry.
- Logs co rotate, state co manifest + heartbeat.

## Trien khai da tao

Package/service moi da nam trong:

- `collectors/common/`: storage partition CSV.GZ, manifest JSON, heartbeat, file lock, retry, VN calendar, logging.
- `collectors/crypto_1m.py`: Binance USD-M futures 1m.
- `collectors/binance_options_snapshot.py`: Binance Options snapshot 5m.
- `collectors/vn_daily.py`: VN equity daily qua vnstock/VCI.
- `collectors/vn_intraday_vnstock.py`: VN equity 1m qua vnstock/KBS.
- `collectors/vn_intraday_dnse.py`: DNSE OHLC 1m, mac dinh `VN30F1M`.
- `collectors/healthcheck.py`: doc heartbeat/manifest.
- `configs/*.yml`: universe va rate settings.
- `docker/Dockerfile`, `docker-compose.yml`, `requirements.txt`.
- `tests/smoke_storage.py`, `tests/smoke_sources.py`.

Alias Python dang dung tren server:

```bash
run-py="/root/.cache/pypoetry/virtualenvs/backtest-env-38u0mE5g-py3.12/bin/python"
```

Vi shell non-interactive co the khong load alias, co the goi truc tiep interpreter tren.

Lenh test local:

```bash
run-py -m compileall collectors tests
run-py -m tests.smoke_storage
HOME=/tmp MPLCONFIGDIR=/tmp/mpl run-py -m tests.smoke_sources --source all
docker compose -f docker-compose.yml config --no-interpolate
```

Lenh chay services:

```bash
docker compose up -d --build
docker compose logs -f crypto-1m-live
docker compose logs -f vn-daily
docker compose logs -f vn-intraday-stocks
docker compose logs -f vn30f1m-dnse
docker compose logs -f options-binance-5m
```

Lenh one-shot de test ghi vao storage production:

```bash
run-py -m collectors.crypto_1m --mode once --symbols BTCUSDT
run-py -m collectors.binance_options_snapshot --mode once --underlyings BTC --interval-minutes 5
run-py -m collectors.vn_daily --mode once --symbols FPT --backfill-start 2026-05-20
run-py -m collectors.vn_intraday_vnstock --mode once --symbols FPT --backfill-start 2026-05-23
run-py -m collectors.vn_intraday_dnse --mode once --symbols VN30F1M --backfill-start 2026-06-01
```

Ket qua smoke live ngay 2026-06-06:

- Binance futures: OK, `BTCUSDT` returned 2 rows trong source smoke; collector once ghi 180 rows vao `/tmp`.
- Binance options: OK, mark endpoint returned 1828 rows; collector once BTC filter ghi 74 rows vao `/tmp`.
- vnstock daily VCI: OK, FPT returned 11 rows trong source smoke; collector once ghi 14 daily rows vao `/tmp`.
- vnstock intraday KBS: OK, FPT returned 2260 rows; collector once ghi 2260 rows vao `/tmp`.
- DNSE: OK, VN30F1M returned 638 rows trong source smoke; collector once ghi 1205 rows vao `/tmp`.
- yfinance fallback smoke: OK, SPY returned 31 expiries.

## Viec nen sua ngay trong scripts hien co neu chua refactor lon

- Doi tat ca path lech:
  - `/root/bobby/pool_alpha/alphas_storage/data_stock`
  - thanh `/root/bobby/pool_alpha/alphas_storage/_get_data/data_stock` hoac storage root moi.
- `get_crypto_1m.py`: dung `DATA_DIR`, tao dir, filename absolute, them incremental.
- `fetch_option_data_all.py`: dung `DATA_DIR`, dynamic expiry, interval 5m, filename absolute.
- `get_stock_vnstock.py` va `dnse_intraday_collector.py`: thay `readlines()`/merge full file bang partition append.
- Dua symbol lists ra YAML config de thay doi universe khong can edit code.
