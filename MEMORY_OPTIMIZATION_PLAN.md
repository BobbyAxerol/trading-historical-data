# Memory Optimization Plan

## Problem

Sau migration sang Parquet, storage đọc nhanh hơn nhưng các loader/collectors vẫn có các pattern tốn RAM:

- Loader đọc full schema từ mọi partition rồi `pd.concat` toàn bộ lịch sử.
- Strategy thường load full `1m` rồi mới resample, tạo nhiều bản copy Pandas trung gian.
- Một số collectors append bằng cách đọc lại cả partition cũ, concat với dữ liệu mới, dedupe/sort, rồi rewrite.

Benchmark SOLUSDT trước Phase 1:

| Mode | Peak RSS |
| :--- | :--- |
| `CryptoBinance1m().load("SOLUSDT")` + Pandas resample | ~2.0GB |
| Đọc Parquet full 14 columns rồi concat | ~817MB |
| Đọc Parquet chỉ OHLCV columns | ~605MB |
| Resample theo partition | ~265MB |

## Phase 1: Loader Projection And Resample-On-Read

Trạng thái: implemented.

Thay đổi:

- Các OHLCV loaders mặc định chỉ đọc `time/symbol/open/high/low/close/volume`.
- Muốn đọc full stored schema thì truyền `columns="full"`.
- Muốn đọc custom schema thì truyền `columns=[...]`.
- Thêm `load_resampled()` cho OHLCV partitioned loaders.
- `load_resampled()` dùng DuckDB để query trực tiếp Parquet, pushdown columns/date filters, aggregate trước khi trả về Pandas.
- Nếu DuckDB không khả dụng, fallback Pandas chunk theo partition.
- `load_data(..., timeframe="5min")` route sang endpoint resample.
- Sau materialization lớn, loader gọi `gc.collect()` và `pyarrow.default_memory_pool().release_unused()` best-effort.

Supported resample loaders:

- `CryptoBinance1m`
- `CryptoBinanceQuarterly1m`
- `CryptoBinanceSpot1m`
- `VnStock1m`
- `VnFutures1m`

Raw `1m` vẫn là canonical storage. Phase này không tạo cache/materialized timeframe mới.

## Phase 2: Collector And Writer Memory Reduction

Trạng thái: implemented.

Thay đổi:

- `PartitionedParquetStore.append`: hỗ trợ partition `day` và cleanup memory sau mỗi partition write bằng `gc.collect()` + release PyArrow memory pool best-effort.
- Options snapshot đổi write partition từ monthly sang daily: `year=YYYY/month=MM/day=DD/part.parquet`.
- Loader options đọc được cả monthly legacy và daily mới trong giai đoạn chuyển đổi.
- `BinanceOptions5m` dùng `snapshot_time` là time-column canonical để filter/sort/validate.
- Migration/split monthly options sang daily qua `tools.migrate_options_snapshot_daily`, validate đủ key `snapshot_time,symbol` trước khi cho phép cleanup monthly.
- `collectors.audit_continuity` scan được partition daily để audit gap/duplicate options snapshot.

Phase 2 có migration/cleanup guard riêng vì đổi layout options. Quy trình chuẩn:

```bash
python -m tools.migrate_options_snapshot_daily --dry-run
python -m tools.migrate_options_snapshot_daily
python -m tools.migrate_options_snapshot_daily --cleanup-monthly --confirm
```

Report được ghi vào `state/options_snapshot_daily_migration_report.json`.

Phần còn lại cho Phase 3+: tiếp tục giảm pattern `frames=[] -> concat all` trong các collector backfill rất lớn nếu benchmark thực tế còn tạo peak RSS cao.
