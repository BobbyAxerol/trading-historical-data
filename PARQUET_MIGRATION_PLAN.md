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

Trạng thái: done for long-format collectors.

Sau khi converter và loader ổn:

- Chuyển collector từ `PartitionedCsvGzStore` sang `PartitionedParquetStore` hoặc store factory.
- Append/dedupe/sort giữ nguyên.
- Không đổi schema output.
- Có thể dual-write ngắn hạn nếu cần rollback, nhưng không duy trì lâu dài vì tốn disk.

Quy ước triển khai:

- Long-format live collectors ghi `part.parquet`.
- `PartitionedParquetStore` vẫn đọc `part.csv.gz` cũ nếu partition đó chưa có Parquet, rồi merge/dedupe và ghi Parquet mới.
- Các audit/repair nội bộ dùng helper đọc/ghi partition chung để đọc được cả Parquet và CSV fallback.
- Không dual-write CSV trong Phase 4.
- Matrix wide-format (`open/high/low/close/volume`) được xử lý riêng ở Phase 7/8.

## Phase 5: Validation

Trạng thái: done for long-format partitioned storage.

Validate trước khi xem Parquet là source chính:

- Row count `csv.gz` vs Parquet.
- Column set và column order.
- Min/max time.
- Duplicate key theo dataset.
- Null `time`.
- Sample equality một số partition.
- Smoke test các loader endpoint chính.

Tool hiện tại:

```bash
python -m tools.validate_parquet_migration --workers 4 --sample-rows 25
python -m tools.validate_parquet_migration --dataset crypto/binance_futures/1m --workers 4
```

Quy ước validation trong transition sau Phase 4:

- Vì live collectors đã ghi Parquet trực tiếp, `part.csv.gz` ở các partition active có thể stale.
- Parquet được coi là pass nếu không mất key từ CSV, không có duplicate key, column order giữ nguyên, time range của Parquet bao phủ CSV, và `time` không null.
- Nếu cùng key nhưng value khác trong khi Parquet mới hơn CSV, tool ghi warning `sample_value_mismatch_parquet_newer_than_csv`, không fail. Đây là case active partitions được REST/Vision refill hoặc snapshot/orderbook cập nhật sau khi CSV đã ngừng ghi.
- Report được ghi tại `state/parquet_validation_report.json`.

Kết quả full validation gần nhất:

- `total_files=4431`
- `ok_files=4431`
- `error_files=0`
- `warning_files=10`
- `row_delta=3995`
- `csv_keys_missing_in_parquet=0`
- `parquet_duplicate_keys=0`

10 warning hiện là CSV stale ở active partitions của `binance_futures_metrics/5m` và `binance_orderbook_snapshot/1h`; không phải mất dữ liệu.

## Phase 6: CSV Cleanup

Trạng thái: done for long-format `part.csv.gz`.

Chỉ xoá `csv.gz` bằng tool có guard:

```bash
python -m tools.cleanup_csv_gz_after_parquet --dry-run
python -m tools.cleanup_csv_gz_after_parquet --confirm
python -m tools.cleanup_csv_gz_after_parquet --dataset crypto/binance_spot/1m --confirm
```

Điều kiện xoá:

- `part.parquet` tồn tại.
- Validation pass theo cùng policy Phase 5: không mất key từ CSV, không duplicate key, column order giữ nguyên, time range của Parquet bao phủ CSV, và `time` không null.
- Parquet mtime >= CSV mtime.
- Warning CSV stale được phép mặc định nếu Parquet mới hơn CSV và không mất key. Dùng `--strict-no-warnings` nếu muốn chặn cả warning.

Tool mặc định chạy dry-run nếu không truyền `--confirm`.

Report được ghi tại `state/parquet_cleanup_report.json`.

Kết quả cleanup đã chạy:

- Dry-run cuối: `total=4431`, `dry_run_delete=4431`, `blocked=0`, `errors=0`, `reclaimable_bytes=1397226950`.
- Confirm cleanup: `total=4431`, `deleted=4431`, `blocked=0`, `errors=0`, `deleted_bytes=1397226950`.
- Post-check: `storage/**/part.csv.gz = 0`.
- Post-check: `storage/**/part.parquet = 4472` tại thời điểm kiểm tra sau cleanup, vì services live tiếp tục ghi thêm Parquet.
- `*.csv.gz` còn lại lúc đó là matrix wide-format (`open/high/low/close/volume`), sau đó đã được xử lý ở Phase 7/8.
- Smoke `data_loader` sau cleanup pass cho futures 1m, spot 1m, VN 1m, VN daily, futures metrics 5m, orderbook snapshot 1h.

## Phase 7: Binance Daily Matrix Parquet

Trạng thái: implemented.

Scope phase này chỉ là wide-format Binance daily matrix:

```text
storage/crypto/binance_daily_matrix/open.parquet
storage/crypto/binance_daily_matrix/high.parquet
storage/crypto/binance_daily_matrix/low.parquet
storage/crypto/binance_daily_matrix/close.parquet
storage/crypto/binance_daily_matrix/volume.parquet
```

Quy ước:

- Collector `binance_daily_matrix` đọc CSV fallback nếu chưa có Parquet, nhưng ghi Parquet.
- `CryptoDailyMatrix` loader ưu tiên Parquet fresh, fallback CSV nếu Parquet chưa có hoặc cũ hơn CSV.
- Public endpoint giữ nguyên: `load(feature)`, `load_features()`, `load_ohlcv()`, `load_ohlcv_frame()`.
- CSV cleanup matrix chạy bằng tool riêng, không dùng cleanup `part.csv.gz` long-format.

Tool:

```bash
python -m tools.migrate_binance_daily_matrix_parquet --dry-run
python -m tools.migrate_binance_daily_matrix_parquet
python -m tools.migrate_binance_daily_matrix_parquet --cleanup-csv --confirm
```

Guard:

- So sánh shape.
- So sánh column order.
- So sánh DatetimeIndex.
- So sánh giá trị numeric với `equal_nan=True`.
- Chỉ xoá CSV nếu validation pass và Parquet không cũ hơn CSV.

Report được ghi tại `state/binance_daily_matrix_parquet_migration_report.json`.

## Phase 8: VN Daily Matrix Parquet And Final CSV Cleanup

Trạng thái: implemented.

Scope phase này là wide-format VN daily matrix:

```text
storage/vn/equity/daily_matrix/open.parquet
storage/vn/equity/daily_matrix/high.parquet
storage/vn/equity/daily_matrix/low.parquet
storage/vn/equity/daily_matrix/close.parquet
storage/vn/equity/daily_matrix/volume.parquet
```

Quy ước:

- Collector `vn_daily_matrix` đọc canonical raw `storage/vn/equity/1d/**/part.parquet` trước, fallback `part.csv.gz` nếu còn legacy.
- Collector ghi matrix Parquet trực tiếp.
- `VNDailyMatrix` loader ưu tiên Parquet fresh, fallback CSV nếu Parquet chưa có hoặc cũ hơn CSV.
- Public endpoint giữ nguyên: `load(feature)`, `load_features()`, `load_ohlcv()`, `load_ohlcv_frame()`.

Cleanup CSV cuối:

```bash
python -m tools.migrate_binance_daily_matrix_parquet --dataset vn_daily_matrix --dry-run
python -m tools.migrate_binance_daily_matrix_parquet --dataset vn_daily_matrix
python -m tools.migrate_binance_daily_matrix_parquet --dataset vn_daily_matrix --cleanup-csv --confirm
```

Guard giống Phase 7: shape, column order, DatetimeIndex và numeric values phải khớp; chỉ xoá CSV nếu validation pass và Parquet không cũ hơn CSV.

Report được ghi tại `state/vn_daily_matrix_parquet_migration_report.json`.
