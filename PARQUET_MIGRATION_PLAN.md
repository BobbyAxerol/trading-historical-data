# Parquet Migration Plan

Mục tiêu là chuyển storage chính từ `csv.gz` sang Parquet theo cách ổn định, không bắt downstream sửa endpoint, không đổi schema trả về của `data_loader`, và không phải call lại dữ liệu từ API.

## Nguyên tắc

- Giữ cấu trúc partition hiện tại.
- Trong giai đoạn chuyển đổi, `part.csv.gz` và `part.parquet` được phép cùng tồn tại.
- Không xoá `csv.gz` cho tới khi có validation report sạch.
- Không tự ý đổi default behavior của `data_loader`: `check_val=True` vẫn giữ nguyên, timezone/schema/column names giữ nguyên.
- Mọi thay đổi writer/reader phải có fallback hoặc phase rollout rõ ràng.
- Parquet dùng `pyarrow` và compression mặc định `zstd`.

## Cấu Trúc Đích

Long-format partition giữ nguyên layout:

```text
storage/crypto/binance_futures/1m/symbol=BTCUSDT/year=2026/month=07/part.parquet
storage/crypto/binance_futures_metrics/5m/symbol=BTCUSDT/year=2026/month=07/part.parquet
storage/vn/equity/1d/symbol=FPT/year=2026/part.parquet
```

Trong transition:

```text
part.csv.gz
part.parquet
```

Matrix wide-format sẽ xử lý ở phase riêng:

```text
storage/crypto/binance_daily_matrix/open.parquet
storage/vn/equity/daily_matrix/close.parquet
```

## Phase 1: Parquet Storage Layer

Trạng thái: done.

Thêm `PartitionedParquetStore` tương thích với `PartitionedCsvGzStore`:

- `append(df, time_col, dedupe_cols, attrs, lock_name)` giữ cùng contract.
- Normalize datetime trước khi ghi.
- Append vào partition hiện có bằng cách đọc partition cũ, concat, dedupe, sort.
- Atomic write qua temp file rồi replace.
- Compression mặc định `zstd`.
- Không đổi collector nào trong phase này.

Test cần có:

- Ghi partition mới.
- Append trùng partition.
- Dedupe theo `symbol,time`.
- Partition theo month/year đúng layout.
- Datetime roundtrip giữ dtype parse được.

## Phase 2: CSV.GZ -> Parquet Converter

Trạng thái: done.

Thêm tool convert local, không call API:

```bash
python -m tools.convert_csv_gz_to_parquet --dry-run
python -m tools.convert_csv_gz_to_parquet --workers 4
```

Yêu cầu:

- Scan `storage/**/part.csv.gz`.
- Ghi `part.parquet` cùng thư mục.
- Skip nếu parquet mới hơn csv và không `--overwrite`.
- Giữ nguyên columns.
- Parse các cột datetime phổ biến: `time`, `close_time`, `sample_time`, `ingested_at` nếu parse được.
- Ghi migration report vào `state/parquet_migration_report.json`.

Tool hiện tại:

```bash
python -m tools.convert_csv_gz_to_parquet --dry-run
python -m tools.convert_csv_gz_to_parquet --dataset crypto/binance_futures_metrics/5m --workers 4
```

Guard hiện có:

- Không xoá `csv.gz`.
- Không overwrite Parquet mới hơn CSV trừ khi truyền `--overwrite`.
- Validate row count và column order sau khi ghi.
- Ghi report gồm status từng file, row count, size, datetime columns, min/max `time`, errors.

## Phase 3: Data Loader Parquet-First Fallback CSV

Trạng thái: done.

Sửa `data_loader.py` tối thiểu:

- Cùng partition có `part.parquet` fresh hơn hoặc bằng `part.csv.gz` thì đọc Parquet.
- Nếu chưa có Parquet, Parquet cũ hơn CSV, hoặc Parquet read lỗi thì fallback `part.csv.gz`.
- Public endpoint behavior giữ nguyên.
- Validation mặc định giữ nguyên.
- Chưa thêm public parameter mới.
- Chưa đổi matrix wide-format loader; matrix sẽ xử lý ở phase riêng.

Lý do dùng freshness guard: trong transition, collectors live vẫn có thể ghi `part.csv.gz` mới hơn `part.parquet`. Data loader không được đọc Parquet cũ và làm mất dữ liệu mới.

## Phase 4: Collector Parquet Writer

Sau khi converter và loader ổn:

- Chuyển collector từ `PartitionedCsvGzStore` sang `PartitionedParquetStore` hoặc store factory.
- Append/dedupe/sort giữ nguyên.
- Không đổi schema output.
- Có thể dual-write ngắn hạn nếu cần rollback, nhưng không duy trì lâu dài vì tốn disk.

## Phase 5: Validation

Validate trước khi xem Parquet là source chính:

- Row count `csv.gz` vs Parquet.
- Column set và column order.
- Min/max time.
- Duplicate key theo dataset.
- Null `time`.
- Sample equality một số partition.
- Smoke test các loader endpoint chính.

## Phase 6: CSV Cleanup

Chỉ xoá `csv.gz` bằng tool có guard:

```bash
python -m tools.cleanup_csv_gz_after_parquet --dry-run
python -m tools.cleanup_csv_gz_after_parquet --confirm
```

Điều kiện xoá:

- `part.parquet` tồn tại.
- Row count khớp.
- Min/max time khớp.
- Parquet mtime >= CSV mtime.
- Validation pass.
