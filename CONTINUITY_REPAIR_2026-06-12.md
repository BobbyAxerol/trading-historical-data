# Continuity Repair 2026-06-12

## Bug

Khi load `ETHUSDT` 1m tu canonical storage va resample len 4h, du lieu bi dut quang. Kiem tra raw 1m confirm co gap that, khong phai loi resample.

Nguyen nhan truc tiep:

- `collectors.audit_continuity` ban cu doc tung `part.csv.gz` va tinh `diff()` rieng trong tung partition.
- Cach do khong bat duoc gap nam giua cac partition sau khi concat lai toan chuoi.
- Crypto storage co gap lich su tu ngay 2026-05-01 toi 2026-06-06 do seed history cu ket thuc dau thang 5, trong khi live service bat dau append lai tu 2026-06-06.

## Phuong an chon

Khong xoa full va call lai tu dau, vi du lieu truoc/sau gap van hop le va storage da co nhieu nam data.

Phuong an dung:

1. Scan toan bo timestamps cua tung symbol sau khi concat tat ca partitions.
2. Xac dinh cac gap `diff > 1 minute`.
3. Goi Binance Futures 1m dung doan thieu `prev_time + 1m` den `next_time - 1m`.
4. Append vao storage bang `PartitionedCsvGzStore.append()`, dedupe theo `symbol,time`, atomic replace partition.
5. Audit lai toan bo dataset.

## Code da them/sua

- `collectors/audit_continuity.py`
  - Sua audit de concat timestamps cua toan symbol truoc khi check gap.
  - Them `--all-symbols` de scan tat ca symbol folders trong dataset.
- `collectors/fill_crypto_gaps.py`
  - Job repair crypto 1m gaps tu Binance.
  - Ho tro `--dry-run`, `--symbols`, `--since`, `--until`, `--max-gaps`.
- `data_loader.py`
  - `validate_data(..., "crypto_1m")` bat dau check continuity 1m theo tung symbol.

## Gap phat hien truoc repair

```text
BTCUSDT: 2026-05-01 10:19:00 -> 2026-06-06 07:34:00
ETHUSDT: 2026-05-01 11:40:00 -> 2026-06-06 07:34:00
SOLUSDT: 2026-05-01 12:25:00 -> 2026-06-06 07:34:00
BNBUSDT: 2026-05-01 12:30:00 -> 2026-06-06 07:34:00
DOGEUSDT: no gap
```

## Repair da chay

Dry-run:

```bash
run-py -m collectors.fill_crypto_gaps --symbols BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,DOGEUSDT --dry-run
```

Repair:

```bash
run-py -m collectors.fill_crypto_gaps --symbols BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT
```

Rows da fill:

```text
BTCUSDT: 51,674 rows
ETHUSDT: 51,593 rows
SOLUSDT: 51,548 rows
BNBUSDT: 51,543 rows
Total: 206,358 rows
```

## Audit sau repair

```text
BNBUSDT: first=2020-02-10 08:01:00 latest=2026-06-12 11:15:00 gaps>1m=0
BTCUSDT: first=2020-01-01 00:00:00 latest=2026-06-12 11:16:00 gaps>1m=0
DOGEUSDT: first=2020-07-10 09:00:00 latest=2026-06-12 11:17:00 gaps>1m=0
ETHUSDT: first=2020-01-01 00:00:00 latest=2026-06-12 11:16:00 gaps>1m=0
SOLUSDT: first=2020-09-14 07:00:00 latest=2026-06-12 11:17:00 gaps>1m=0
```

ETH loader/resample check:

```text
ETHUSDT 2026-04-25 -> 2026-06-08
rows=63,361
validate_data valid=True
continuity_gap_count=0
4h resample close NaN count=0
```

Other dataset checks:

- Options BTC/ETH snapshot: gap 5m = 0.
- VN futures `VN30F1M`: read/merge audit ok.
- VN intraday all symbols: read/merge audit ok. Strict 1m 24/7 continuity is not applied because VN data has sessions, lunch break, weekends, and holidays.
- VN daily all symbols: read/merge audit ok. Some delisted/inactive symbols have old latest dates, which is expected and should be handled by universe metadata rather than gap repair.
- Binance daily matrix sample `BTCUSDT/ETHUSDT`: all features valid, date range `2026-06-01 -> 2026-06-12`.

## Lenh van hanh sau nay

Scan all crypto:

```bash
run-py -m collectors.audit_continuity --dataset crypto --all-symbols
```

Dry-run repair:

```bash
run-py -m collectors.fill_crypto_gaps --symbols BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,DOGEUSDT --dry-run
```

Repair all configured crypto symbols:

```bash
run-py -m collectors.fill_crypto_gaps
```
