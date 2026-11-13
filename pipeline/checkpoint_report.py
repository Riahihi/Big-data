"""Write the Checkpoint 3 evidence report after a successful pipeline run."""
import csv
import json
import sys
from datetime import datetime
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import config as C


def read_json(path):
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def markdown_sample(path, limit=3):
    with path.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))
    if not rows:
        return "_Không có dữ liệu mẫu._"
    headers, values = rows[0], rows[1:limit + 1]
    table = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    table.extend("| " + " | ".join(value.replace("|", "\\|") for value in row) + " |" for row in values)
    return "\n".join(table)


def main():
    results = C.RESULTS_DIR
    log = read_json(results / "pipeline_run.json")
    required_steps = ("clean", "analysis", "advanced", "verify", "benchmark", "figures")
    incomplete = [name for name in required_steps
                  if log.get("steps", {}).get(name, {}).get("status") != "ok"]
    if incomplete:
        raise RuntimeError(f"Cannot create checkpoint report; unsuccessful steps: {', '.join(incomplete)}")

    manifest = [
        json.loads(line) for line in C.MANIFEST.read_text(encoding="utf-8").splitlines() if line.strip()
    ]
    hourly = [record for record in manifest if record.get("kind") == "hourly"]
    clean = read_json(results / "clean_stats.json")
    verification = read_json(results / "verification_report.json")
    benchmark = read_json(results / "benchmark.json")
    plan_path = C.REPORTS / "evidence" / "clean_plan.txt"
    if not plan_path.exists():
        raise FileNotFoundError(f"Missing formatted Spark plan: {plan_path}")

    fields = clean["output_schema"]["fields"]
    schema_rows = [
        f"| `{field['name']}` | `{field['type']}` | {field['nullable']} |"
        for field in fields
    ]
    verification_rows = []
    for name, result in verification.items():
        if not isinstance(result, dict):
            continue
        status = "SKIP" if "skipped" in result else ("PASS" if result.get("passed") else "FAIL")
        verification_rows.append(f"| `{name}` | {status} |")

    step_rows = [
        f"| `{name}` | {entry.get('status', 'unknown')} | {entry.get('elapsed_s', '—')} | "
        f"{entry.get('finished_at', '—')} |"
        for name, entry in log.get("steps", {}).items() if name != "report"
    ]
    storage = benchmark.get("B1_storage_MB", {})
    q1_time = benchmark.get("B2_q1_seconds", {})
    report = [
        "# Checkpoint 3 — Spark Processing Pipeline",
        "",
        f"_Tạo lúc {datetime.now().astimezone().isoformat(timespec='seconds')}_",
        "",
        "## Kiến trúc",
        "",
        "`Open-Meteo JSON.gz → đọc schema → chuẩn hóa/làm sạch → join lưới giờ và dimension "
        "địa điểm → tổng hợp ngày/tháng → Parquet + CSV`",
        "",
        "## Input contract",
        "",
        f"- Nguồn: Open-Meteo Historical Weather API, model `{C.MODEL}`, timezone `{C.TIMEZONE}`.",
        f"- Raw: `data/raw/<location>/<location>_<year>.json.gz`; {len(hourly)} file hourly, "
        f"{sum(record.get('n_records', 0) for record in hourly):,} bản ghi trong manifest.",
        "- Grain: một bản ghi cho mỗi địa điểm và thời điểm giờ; khóa nghiệp vụ `(location_id, ts)`.",
        "- Schema raw được khai báo tường minh trong pipeline; output schema sau làm sạch:",
        "",
        "| Cột | Kiểu | Nullable |",
        "| --- | --- | --- |",
        *schema_rows,
        "",
        "## Transform, join và quality",
        "",
        f"- Input rows: {clean['raw_rows']:,}; sau deduplicate: {clean['rows_after_deduplicate']:,}; "
        f"duplicates đã loại: {clean['duplicates_removed']:,}.",
        f"- Khóa dimension địa điểm: {clean['location_dimension_keys']} khóa, "
        f"unmatched: {clean['rows_unmatched_location_dimension']}.",
        f"- Trước grid join: {clean['rows_before_grid_join']:,}; matched input rows: "
        f"{clean['rows_matched_to_grid']:,}; outside grid: {clean['rows_outside_grid']:,}.",
        f"- Grid rows dự kiến: {clean['expected_grid_rows']:,}; sau join: "
        f"{clean['rows_after_dimension_join']:,}; delta: {clean['grid_join_row_delta']:,}.",
        f"- Quality: missing {clean['missing_rows']:,}; imputed {clean['rows_imputed']:,}; "
        f"out-of-range {clean['out_of_range_total']:,}; null temperature "
        f"{clean['null_temperature_after_clean']:,}; null precipitation "
        f"{clean['null_precipitation_after_clean']:,}.",
        "- Transformations: parse/cast, deduplicate theo khóa, dựng đủ 24 giờ, kiểm tra range, "
        "nội suy tối đa 3 giờ cho biến liên tục; không nội suy mưa.",
        "- Join gate dừng pipeline nếu có record ngoài grid, thiếu dimension key, hoặc row-count delta.",
        "",
        "## Aggregation và output contract",
        "",
        "| Output | Grain | Consumer |",
        "| --- | --- | --- |",
        "| `q1_monthly_temp_core` | địa điểm × năm × tháng | nhiệt độ trung bình tháng |",
        "| `q2_monthly_precip_core` | địa điểm × năm × tháng | tổng lượng mưa tháng |",
        "| `q3_top5_rain_days_core` | địa điểm × ngày/rank (giữ đồng hạng) | top ngày mưa đủ dữ liệu |",
        "",
        "Bảng chính được ghi ở `data/results/` dưới dạng Parquet và CSV; chế độ ghi là overwrite.",
        "",
        "### Sample output",
        "",
        "#### Câu 1",
        "",
        markdown_sample(results / "q1_monthly_temp_core.csv"),
        "",
        "#### Câu 2",
        "",
        markdown_sample(results / "q2_monthly_precip_core.csv"),
        "",
        "#### Câu 3",
        "",
        markdown_sample(results / "q3_top5_rain_days_core.csv"),
        "",
        "## Verification",
        "",
        f"- Parquet round-trip: {clean['clean_rows']:,}/{clean['expected_rows']:,} rows; "
        f"passed = `{clean['parquet_roundtrip_ok']}`.",
        f"- Independent/API checks: `all_passed = {verification['all_passed']}`.",
        "",
        "| Check | Result |",
        "| --- | --- |",
        *verification_rows,
        "",
        f"- Formatted Spark plan: `reports/evidence/{plan_path.name}`.",
        "",
        "## Baseline và khả năng tái chạy",
        "",
        "| Bước | Trạng thái | Thời gian lần ghi log gần nhất (s) | Kết thúc lần ghi log gần nhất |",
        "| --- | --- | ---: | --- |",
        *step_rows,
        "",
        f"- CSV size: {storage.get('csv_uncompressed', '—')} MB; "
        f"flat Parquet size: {storage.get('parquet_snappy_flat', '—')} MB.",
        f"- Q1 timing: CSV {q1_time.get('csv', ['—'])[0]} s; "
        f"Parquet {q1_time.get('parquet', ['—'])[0]} s.",
        "- Rerun dùng `overwrite`; pipeline log được lưu tại `data/results/pipeline_run.json`.",
        "- Mỗi lần đo và máy có thể khác nhau; dùng cùng input/máy để so sánh baseline.",
        "",
        "## Hạn chế và bước tiếp theo",
        "",
        "- ERA5 là dữ liệu tái phân tích theo ô lưới, không phải số đo trạm.",
        "- Trình bày nguyên nhân dịch mốc mưa một giờ và giới hạn suy luận xu hướng 10 năm.",
        "- Chốt commit checkpoint sau khi review report, plan và các output mẫu.",
        "",
    ]

    C.REPORTS.mkdir(parents=True, exist_ok=True)
    (C.REPORTS / "checkpoint3.md").write_text("\n".join(report), encoding="utf-8")
    print(f"saved {C.REPORTS / 'checkpoint3.md'}")


if __name__ == "__main__":
    main()
