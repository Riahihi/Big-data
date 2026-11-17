# Checkpoint 3 — Spark Processing Pipeline

_Tạo lúc 2026-11-14T07:50:03+07:00_

## Kiến trúc

`Open-Meteo JSON.gz → đọc schema → chuẩn hóa/làm sạch → join lưới giờ và dimension địa điểm → tổng hợp ngày/tháng → Parquet + CSV`

## Input contract

- Nguồn: Open-Meteo Historical Weather API, model `era5`, timezone `Asia/Ho_Chi_Minh`.
- Raw: `data/raw/<location>/<location>_<year>.json.gz`; 340 file hourly, 2,980,848 bản ghi trong manifest.
- Grain: một bản ghi cho mỗi địa điểm và thời điểm giờ; khóa nghiệp vụ `(location_id, ts)`.
- Schema raw được khai báo tường minh trong pipeline; output schema sau làm sạch:

| Cột | Kiểu | Nullable |
| --- | --- | --- |
| `location_id` | `string` | False |
| `location_name` | `string` | False |
| `site_name` | `string` | False |
| `region` | `string` | False |
| `is_core` | `boolean` | False |
| `latitude` | `decimal(6,4)` | False |
| `longitude` | `decimal(7,4)` | False |
| `grid_latitude` | `double` | True |
| `grid_longitude` | `double` | True |
| `elevation_m` | `double` | True |
| `ts` | `timestamp` | False |
| `date` | `date` | False |
| `precip_date` | `date` | False |
| `year` | `integer` | False |
| `month` | `integer` | False |
| `hour` | `integer` | False |
| `temperature_2m` | `double` | True |
| `relative_humidity_2m` | `double` | True |
| `dew_point_2m` | `double` | True |
| `apparent_temperature` | `double` | True |
| `precipitation` | `double` | True |
| `rain` | `double` | True |
| `weather_code` | `integer` | True |
| `cloud_cover` | `double` | True |
| `wind_speed_10m` | `double` | True |
| `wind_gusts_10m` | `double` | True |
| `is_missing_row` | `boolean` | False |
| `temp_imputed` | `boolean` | True |

## Transform, join và quality

- Input rows: 2,980,848; sau deduplicate: 2,980,848; duplicates đã loại: 0.
- Khóa dimension địa điểm: 34 khóa, unmatched: 0.
- Trước grid join: 2,980,848; matched input rows: 2,980,848; outside grid: 0.
- Grid rows dự kiến: 2,980,848; sau join: 2,980,848; delta: 0.
- Quality: missing 0; imputed 0; out-of-range 0; null temperature 0; null precipitation 0.
- Transformations: parse/cast, deduplicate theo khóa, dựng đủ 24 giờ, kiểm tra range, nội suy tối đa 3 giờ cho biến liên tục; không nội suy mưa.
- Join gate dừng pipeline nếu có record ngoài grid, thiếu dimension key, hoặc row-count delta.

## Aggregation và output contract

| Output | Grain | Consumer |
| --- | --- | --- |
| `q1_monthly_temp_core` | địa điểm × năm × tháng | nhiệt độ trung bình tháng |
| `q2_monthly_precip_core` | địa điểm × năm × tháng | tổng lượng mưa tháng |
| `q3_top5_rain_days_core` | địa điểm × ngày/rank (giữ đồng hạng) | top ngày mưa đủ dữ liệu |

Bảng chính được ghi ở `data/results/` dưới dạng Parquet và CSV; chế độ ghi là overwrite.

### Sample output

#### Câu 1

| location_id | year | month | location_name | site_name | region | is_core | avg_temp_c | min_temp_c | max_temp_c | n_hours | avg_daily_max_c | avg_daily_min_c | n_days | coverage_pct | year_month |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| CTO | 2019 | 1 | Cần Thơ | Cần Thơ | Đồng bằng sông Cửu Long | True | 26.51 | 21.7 | 31.3 | 744 | 29.89 | 23.99 | 31 | 100.0 | 2019-01 |
| CTO | 2019 | 2 | Cần Thơ | Cần Thơ | Đồng bằng sông Cửu Long | True | 27.11 | 22.6 | 31.9 | 672 | 31.17 | 24.08 | 28 | 100.0 | 2019-02 |
| CTO | 2019 | 3 | Cần Thơ | Cần Thơ | Đồng bằng sông Cửu Long | True | 28.51 | 21.3 | 34.9 | 744 | 33.07 | 24.92 | 31 | 100.0 | 2019-03 |

#### Câu 2

| location_id | location_name | site_name | region | is_core | year | month | total_precip_mm | rain_days_ge_1mm | max_daily_precip_mm | n_days | n_complete_days | is_complete_month | year_month |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| CTO | Cần Thơ | Cần Thơ | Đồng bằng sông Cửu Long | True | 2019 | 1 | 54.0 | 5 | 33.6 | 31 | 31 | True | 2019-01 |
| CTO | Cần Thơ | Cần Thơ | Đồng bằng sông Cửu Long | True | 2019 | 2 | 1.6 | 0 | 0.7 | 28 | 28 | True | 2019-02 |
| CTO | Cần Thơ | Cần Thơ | Đồng bằng sông Cửu Long | True | 2019 | 3 | 13.3 | 3 | 4.5 | 31 | 31 | True | 2019-03 |

#### Câu 3

| location_id | location_name | site_name | region | is_core | rank | date | precip_mm | precip_max_1h | wet_hours | t_mean | n_tied | rain_class |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| CTO | Cần Thơ | Cần Thơ | Đồng bằng sông Cửu Long | True | 1 | 2021-11-16 | 55.9 | 9.5 | 15 | 25.98 | 1 | Mưa to (51–100 mm) |
| CTO | Cần Thơ | Cần Thơ | Đồng bằng sông Cửu Long | True | 2 | 2021-11-30 | 51.9 | 11.4 | 18 | 25.45 | 1 | Mưa to (51–100 mm) |
| CTO | Cần Thơ | Cần Thơ | Đồng bằng sông Cửu Long | True | 3 | 2022-05-04 | 43.1 | 8.9 | 18 | 26.71 | 1 | Mưa vừa (16–50 mm) |

## Verification

- Parquet round-trip: 2,980,848/2,980,848 rows; passed = `True`.
- Independent/API checks: `all_passed = True`.

| Check | Result |
| --- | --- |
| `V1_q1_monthly_temp` | PASS |
| `V1_q2_monthly_precip` | PASS |
| `V1_q3_top5` | PASS |
| `V2_open_meteo_daily` | PASS |
| `V3_manual_sample` | PASS |
| `V4_parquet_roundtrip` | PASS |

- Formatted Spark plan: `reports/evidence/clean_plan.txt`.

## Baseline và khả năng tái chạy

| Bước | Trạng thái | Thời gian lần ghi log gần nhất (s) | Kết thúc lần ghi log gần nhất |
| --- | --- | ---: | --- |
| `ingest` | ok | 0.9 | 2026-11-13T23:19:43 |
| `daily` | ok | 0.3 | 2026-11-13T23:19:43 |
| `clean` | ok | 185.6 | 2026-11-14T07:44:04 |
| `analysis` | ok | 56.4 | 2026-11-14T07:45:01 |
| `advanced` | ok | 96.4 | 2026-11-14T07:46:37 |
| `verify` | ok | 9.2 | 2026-11-14T07:46:46 |
| `benchmark` | ok | 189.0 | 2026-11-14T07:49:55 |
| `figures` | ok | 7.6 | 2026-11-14T07:50:03 |

- CSV size: 614.6 MB; flat Parquet size: 37.0 MB.
- Q1 timing: CSV 6.5 s; Parquet 0.6 s.
- Rerun dùng `overwrite`; pipeline log được lưu tại `data/results/pipeline_run.json`.
- Mỗi lần đo và máy có thể khác nhau; dùng cùng input/máy để so sánh baseline.

## Hạn chế và bước tiếp theo

- ERA5 là dữ liệu tái phân tích theo ô lưới, không phải số đo trạm.
- Trình bày nguyên nhân dịch mốc mưa một giờ và giới hạn suy luận xu hướng 10 năm.
- Chốt commit checkpoint sau khi review report, plan và các output mẫu.
