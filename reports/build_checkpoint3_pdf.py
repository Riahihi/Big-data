"""Build a six-page Checkpoint 3 PDF from the report evidence.

Run: python reports/build_checkpoint3_pdf.py
"""
import csv
import json
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.utils import simpleSplit
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas

ROOT = Path(__file__).resolve().parent.parent
REPORTS = ROOT / "reports"
RESULTS = ROOT / "data" / "results"
OUTPUT = REPORTS / "checkpoint3.pdf"
WIDTH, HEIGHT = 960, 540

BG = colors.HexColor("#F6F8FB")
NAVY = colors.HexColor("#0B2634")
INK = colors.HexColor("#102D40")
MUTED = colors.HexColor("#657D94")
TEAL = colors.HexColor("#13B8A6")
STROKE = colors.HexColor("#CCD8E4")
BLUE_BG, BLUE = colors.HexColor("#EAF2FF"), colors.HexColor("#377DFF")
GREEN_BG, GREEN = colors.HexColor("#E9F9F1"), colors.HexColor("#10B86B")
ORANGE_BG, ORANGE = colors.HexColor("#FFF4E5"), colors.HexColor("#F29A19")
PURPLE_BG, PURPLE = colors.HexColor("#F2EEFF"), colors.HexColor("#8A63F6")
WHITE = colors.white


def load_json(name):
    with (RESULTS / name).open(encoding="utf-8") as f:
        return json.load(f)


def first_csv(name):
    with (RESULTS / name).open(encoding="utf-8-sig", newline="") as f:
        return next(csv.DictReader(f))


def draw_text(c, x, y, value, size=9, color=INK, font="ReportArial"):
    c.setFillColor(color)
    c.setFont(font, size)
    c.drawString(x, HEIGHT - y - size, str(value))


def draw_centered(c, x, y, width, value, size=9, color=INK, font="ReportArial"):
    c.setFillColor(color)
    c.setFont(font, size)
    c.drawCentredString(x + width / 2, HEIGHT - y - size, str(value))


def draw_paragraph(c, x, y, value, width, size=9, leading=12,
                   color=INK, font="ReportArial"):
    c.setFillColor(color)
    c.setFont(font, size)
    for paragraph in str(value).split("\n"):
        lines = simpleSplit(paragraph, font, size, width) if paragraph else [""]
        for line in lines:
            if line:
                c.drawString(x, HEIGHT - y - size, line)
            y += leading
    return y


def draw_box(c, x, y, width, height, fill=WHITE, stroke=STROKE, radius=12):
    c.setLineWidth(0.8)
    c.setFillColor(fill)
    c.setStrokeColor(stroke)
    c.roundRect(x, HEIGHT - y - height, width, height, radius, fill=1, stroke=1)


def draw_card(c, x, y, width, height, title, fill=WHITE, stroke=STROKE):
    draw_box(c, x, y, width, height, fill, stroke)
    draw_text(c, x + 13, y + 11, title, 11, INK, "ReportArial-Bold")
    return x + 13, y + 31, width - 26, height - 42


def draw_header(c, page, title, subtitle=""):
    c.setFillColor(BG)
    c.rect(0, 0, WIDTH, HEIGHT, fill=1, stroke=0)
    c.setFillColor(TEAL)
    c.rect(0, HEIGHT - 6, WIDTH, 6, fill=1, stroke=0)
    draw_text(c, 36, 17, "CHECKPOINT 3  •  SPARK PROCESSING PIPELINE",
              8, MUTED, "ReportArial-Bold")
    draw_text(c, 40, 45, title, 23, INK, "ReportArial-Bold")
    if subtitle:
        draw_text(c, 41, 77, subtitle, 9, MUTED)
    draw_text(c, 908, 516, page, 7, MUTED)


def draw_table(c, x, y, col_widths, rows, row_height=27,
               header_fill=BLUE_BG, font_size=8.2):
    total_width = sum(col_widths)
    for row_index, row in enumerate(rows):
        fill = header_fill if row_index == 0 else WHITE
        c.setFillColor(fill)
        c.setStrokeColor(STROKE)
        c.setLineWidth(0.5)
        c.rect(x, HEIGHT - y - row_height, total_width, row_height, fill=1, stroke=1)
        xx = x
        for i, value in enumerate(row):
            font = "ReportArial-Bold" if row_index == 0 else "ReportArial"
            c.setFillColor(INK if row_index == 0 else MUTED)
            c.setFont(font, font_size)
            lines = simpleSplit(str(value), font, font_size, col_widths[i] - 12)
            line_y = HEIGHT - y - 10
            for line in lines[:2]:
                c.drawString(xx + 6, line_y, line)
                line_y -= font_size + 1
            xx += col_widths[i]
        xx = x
        for width in col_widths[:-1]:
            xx += width
            c.line(xx, HEIGHT - y, xx, HEIGHT - y - row_height)
        y += row_height
    return y


def build():
    stats = load_json("clean_stats.json")
    verification = load_json("verification_report.json")
    benchmark = load_json("benchmark.json")
    run_log = load_json("pipeline_run.json")
    if not (REPORTS / "checkpoint3.md").is_file():
        raise FileNotFoundError(REPORTS / "checkpoint3.md")
    if not verification.get("all_passed"):
        raise RuntimeError("Cannot create PDF: independent verification did not pass")
    required = ("clean", "analysis", "advanced", "verify", "benchmark", "figures", "report")
    if any(run_log.get("steps", {}).get(name, {}).get("status") != "ok"
           for name in required):
        raise RuntimeError("Cannot create PDF: a required pipeline step did not succeed")

    q1 = first_csv("q1_monthly_temp_core.csv")
    q2 = first_csv("q2_monthly_precip_core.csv")
    q3 = first_csv("q3_top5_rain_days_core.csv")
    sizes = benchmark["B1_storage_MB"]
    q1_times = benchmark["B2_q1_seconds"]
    q1_csv_s = float(q1_times["csv"][0])
    q1_parquet_s = float(q1_times["parquet"][0])
    csv_mb = float(sizes["csv_uncompressed"])
    parquet_mb = float(sizes["parquet_snappy_flat"])
    size_ratio = float(benchmark["B1_csv_over_parquet_ratio"])
    time_ratio = q1_csv_s / q1_parquet_s
    elapsed = sum(float(run_log.get("steps", {}).get(name, {}).get("elapsed_s", 0) or 0)
                  for name in ("clean", "analysis", "advanced", "verify", "benchmark", "figures"))

    font_dir = Path(r"C:\Windows\Fonts")
    regular_font = font_dir / "arial.ttf"
    bold_font = font_dir / "arialbd.ttf"
    if not regular_font.is_file() or not bold_font.is_file():
        raise FileNotFoundError("Arial TrueType fonts are required for Vietnamese PDF text")
    pdfmetrics.registerFont(TTFont("ReportArial", str(regular_font)))
    pdfmetrics.registerFont(TTFont("ReportArial-Bold", str(bold_font)))

    c = canvas.Canvas(str(OUTPUT), pagesize=(WIDTH, HEIGHT), pageCompression=1)
    c.setTitle("Checkpoint 3 — Spark Processing Pipeline")
    c.setAuthor("Nhóm Đồ Án BigData")
    c.setSubject("Báo cáo pipeline xử lý dữ liệu thời tiết bằng PySpark")

    # Cover
    c.setFillColor(NAVY)
    c.rect(0, 0, WIDTH, HEIGHT, fill=1, stroke=0)
    c.setFillColor(TEAL)
    c.rect(0, 0, 10, HEIGHT, fill=1, stroke=0)
    draw_text(c, 52, 48, "CHECKPOINT 3", 10, TEAL, "ReportArial-Bold")
    draw_text(c, 52, 126, "SPARK PROCESSING PIPELINE", 30, WHITE, "ReportArial-Bold")
    draw_text(c, 54, 186, "Nhóm: Đồ án BigData  •  Ngày: 13/11/2026",
              11, colors.HexColor("#C6D8E3"))
    draw_text(c, 54, 211,
              "Thành viên: Phan Duy Minh Thắng (21T1020694)  •  Lê Anh Quân (22T1020710)",
              9, colors.HexColor("#C6D8E3"))
    draw_text(c, 54, 231,
              "Lê Trần Khánh Tùng (21T1020819)  •  Đậu Hà Văn (21T1020826)",
              9, colors.HexColor("#C6D8E3"))
    for x, label, value in (
        (54, "DỮ LIỆU", "2,980,848 giờ"),
        (298, "PHẠM VI", "34 địa điểm"),
        (542, "THỜI GIAN", "2016–2025"),
    ):
        draw_box(c, x, 292, 215, 72, colors.HexColor("#143747"), colors.HexColor("#2C5365"), 10)
        draw_text(c, x + 15, 305, label, 8, TEAL, "ReportArial-Bold")
        draw_text(c, x + 15, 327, value, 15, WHITE, "ReportArial-Bold")
    draw_paragraph(
        c, 54, 420,
        "Mục tiêu: chứng minh pipeline chạy end-to-end từ dữ liệu raw đến bảng phân tích, "
        "có kiểm soát chất lượng, kiểm chứng độc lập và bằng chứng hiệu năng.",
        800, 11, 17, colors.HexColor("#C6D8E3"),
    )
    c.showPage()

    # 1. Pipeline architecture
    draw_header(c, 2, "1. Pipeline architecture",
                "Luồng thực tế đã chạy trên dữ liệu raw có sẵn; bước ingest được bỏ qua theo lệnh --skip-ingest.")
    draw_box(c, 52, 113, 856, 248, WHITE, STROKE, 20)
    nodes = [
        ("RAW JSON.GZ", "340 files"),
        ("READ + SCHEMA", "schema tường minh"),
        ("TRANSFORM", "cast • dedup • fill"),
        ("JOIN", "hour grid + location"),
        ("AGGREGATE", "daily / monthly / rank"),
        ("OUTPUT", "Parquet + CSV"),
    ]
    for i, (title, subtitle) in enumerate(nodes):
        x = 66 + i * 140
        fill, border = (
            (BLUE_BG, BLUE), (PURPLE_BG, PURPLE), (ORANGE_BG, ORANGE),
            (BLUE_BG, BLUE), (GREEN_BG, GREEN), (GREEN_BG, GREEN),
        )[i]
        draw_box(c, x, 180, 122, 76, fill, border, 9)
        draw_centered(c, x + 4, 194, 114, title, 8.3, INK, "ReportArial-Bold")
        draw_centered(c, x + 5, 219, 112, subtitle, 7.2, MUTED)
        if i < len(nodes) - 1:
            start_x, arrow_y = x + 124, HEIGHT - 218
            c.setStrokeColor(TEAL)
            c.setLineWidth(1.7)
            c.line(start_x, arrow_y, start_x + 13, arrow_y)
            c.line(start_x + 13, arrow_y, start_x + 8, arrow_y + 4)
            c.line(start_x + 13, arrow_y, start_x + 8, arrow_y - 4)
    draw_paragraph(c, 78, 280, "Open-Meteo Historical API • ERA5 • Asia/Ho_Chi_Minh",
                   800, 10, 14, INK, "ReportArial-Bold")
    draw_paragraph(c, 78, 302,
                   "Raw JSON.gz → Spark read/schema → chuẩn hóa và kiểm tra → join lưới giờ + "
                   "dimension → các câu hỏi phân tích → bảng Parquet/CSV, biểu đồ và report.",
                   800, 9, 13, MUTED)
    for x, title, detail, fill, border in (
        (54, "INPUT", "340 hourly files • 2016–2025 • 34 địa điểm", BLUE_BG, BLUE),
        (350, "PROCESSING", "PySpark 4.0.1 • local[8] • timezone VN",
         colors.HexColor("#E8F8F5"), TEAL),
        (646, "OUTPUT", "data/clean, data/results, reports/figures", GREEN_BG, GREEN),
    ):
        cx, cy, cw, ch = draw_card(c, x, 389, 260, 82, title, fill, border)
        draw_paragraph(c, x + 13, 427, detail, 232, 8.7, 12, INK)
    c.showPage()

    # 2. Input, schema and transformations
    draw_header(c, 3, "2. Input, schema & transformations",
                "Nguồn và grain được chốt trước khi tổng hợp; output schema ghi trong clean_stats.json.")
    x, y, w, h = draw_card(c, 54, 112, 264, 177, "Input contract", BLUE_BG, BLUE)
    draw_paragraph(
        c, x, y,
        f"Dataset: Open-Meteo Historical Weather API (ERA5)\n"
        f"Grain: 1 địa điểm × 1 thời điểm giờ\n"
        f"Rows/files: {number(stats['raw_rows'])} dòng • 340 file hourly\n"
        f"Thời gian: 2016–2025 • timezone Asia/Ho_Chi_Minh\n"
        f"Khóa nghiệp vụ: (location_id, ts)",
        w, 8.5, 14,
    )
    x, y, w, h = draw_card(c, 334, 112, 572, 177, "Output schema • 28 cột", WHITE, STROKE)
    fields = stats["output_schema"]["fields"]
    mid = (len(fields) + 1) // 2
    for column, group in enumerate((fields[:mid], fields[mid:])):
        xx, yy = x + column * 270, y
        for field in group:
            draw_text(c, xx, yy, f"{field['name']}: {field['type']}", 7.4, INK)
            yy += 9

    x, y, w, h = draw_card(c, 54, 302, 264, 145, "Transformations", ORANGE_BG, ORANGE)
    draw_paragraph(
        c, x, y,
        "1. Parse/cast timestamp và biến thời tiết.\n"
        "2. Deduplicate theo (location_id, ts).\n"
        "3. Dựng lưới 24 giờ, kiểm tra range.\n"
        "4. Nội suy tối đa 3 giờ cho biến liên tục; không nội suy mưa.",
        w, 8.1, 13,
    )
    x, y, w, h = draw_card(c, 334, 302, 572, 145, "Quality delta", GREEN_BG, GREEN)
    draw_paragraph(
        c, x, y,
        f"Input rows: {number(stats['raw_rows'])}  →  Valid rows: "
        f"{number(stats['clean_rows'])}  →  Rejected/invalid: 0",
        w, 9, 15, INK, "ReportArial-Bold",
    )
    draw_paragraph(
        c, x, y + 39,
        f"Duplicates removed: {number(stats['duplicates_removed'])}  •  "
        f"Missing grid rows: {number(stats['missing_rows'])}  •  "
        f"Imputed: {number(stats['rows_imputed'])}",
        w, 8.4, 13,
    )
    draw_paragraph(
        c, x, y + 72,
        f"Out of range: {number(stats['out_of_range_total'])}  •  "
        f"Null temperature: {number(stats['null_temperature_after_clean'])}  •  "
        f"Null precipitation: {number(stats['null_precipitation_after_clean'])}",
        w, 8.1, 12,
    )
    c.showPage()

    # 3. Join and data quality
    draw_header(c, 4, "3. Join & data quality",
                "Quality gates bảo đảm join không làm rơi hoặc nhân bản grain theo địa điểm–giờ.")
    for x, title, fill, border, body in (
        (54, "Join contract", PURPLE_BG, PURPLE,
         "Grid LEFT JOIN facts\nKey: (location_id, ts)\n"
         "Location dim: broadcast LEFT JOIN\nKey: location_id"),
        (350, "Row counts", BLUE_BG, BLUE,
         f"Before grid join: {number(stats['rows_before_grid_join'])}\n"
         f"Expected grid: {number(stats['expected_grid_rows'])}\n"
         f"After dimension join: {number(stats['rows_after_dimension_join'])}\n"
         f"Delta: {stats['grid_join_row_delta']}"),
        (646, "Unmatched & policy", ORANGE_BG, ORANGE,
         f"Unique location keys: {stats['location_dimension_keys']}\n"
         f"Unmatched dimension: {stats['rows_unmatched_location_dimension']}\n"
         f"Outside grid: {stats['rows_outside_grid']}\n"
         "Policy: fail pipeline if any mismatch"),
    ):
        cx, cy, cw, ch = draw_card(c, x, 112, 260, 145, title, fill, border)
        draw_paragraph(c, cx, cy, body, cw, 8.2, 13)

    cx, cy, cw, ch = draw_card(c, 54, 276, 852, 205,
                               "Join evidence and quality gates", WHITE, STROKE)
    rows = [
        ("Check", "Observed", "Expected / gate", "Result"),
        ("Hourly grid row count", f"{number(stats['rows_after_dimension_join'])} rows",
         f"{number(stats['expected_grid_rows'])} rows", "PASS"),
        ("Fact rows outside grid", f"{number(stats['rows_outside_grid'])}", "0", "PASS"),
        ("Location dimension keys", f"{stats['location_dimension_keys']} unique",
         "All raw IDs matched", "PASS"),
        ("Parquet round-trip", f"{number(stats['clean_rows'])} rows",
         f"{number(stats['expected_rows'])} rows", "PASS"),
    ]
    draw_table(c, cx, cy + 4, [174, 206, 256, 150], rows, 29, GREEN_BG, 8.1)
    draw_paragraph(
        c, cx, cy + 158,
        "Matched fact rows: " + number(stats["rows_matched_to_grid"]) +
        "  •  Join row-count delta: " + str(stats["grid_join_row_delta"]),
        cw, 8.3, 12, MUTED,
    )
    c.showPage()

    # 4. Aggregations and outputs
    draw_header(c, 5, "4. Aggregation & output",
                "Ba câu hỏi theo proposal; output ghi đè để có thể rerun cùng dữ liệu.")
    x, y, w, h = draw_card(c, 54, 112, 270, 147, "Aggregation", GREEN_BG, GREEN)
    draw_paragraph(
        c, x, y,
        "Q1: location × year × month → avg hourly temperature.\n"
        "Q2: location × year × month → sum daily precipitation.\n"
        "Q3: location × day/rank → top 5 ngày mưa; giữ đồng hạng.",
        w, 8.4, 14,
    )
    x, y, w, h = draw_card(c, 350, 112, 556, 147, "Output location", BLUE_BG, BLUE)
    draw_paragraph(
        c, x, y,
        "Format: Parquet + CSV\nPath: data/results/*_core\nWrite mode: overwrite\n"
        "Q1 grain: location × year × month\nQ2 grain: location × year × month\n"
        "Q3 grain: location × day/rank (ties retained)",
        w, 8.5, 13,
    )
    x, y, w, h = draw_card(c, 54, 276, 552, 194, "Sample output • core tables", WHITE, STROKE)
    rows = [
        ("Output", "Sample record", "Metric"),
        (f"Q1 temperature", f"{q1['location_id']} • {q1['year']}-{int(q1['month']):02d}",
         f"{float(q1['avg_temp_c']):.2f} °C; {q1['n_hours']} hours"),
        (f"Q2 precipitation", f"{q2['location_id']} • {q2['year']}-{int(q2['month']):02d}",
         f"{float(q2['total_precip_mm']):.1f} mm total"),
        (f"Q3 top rain day", f"{q3['location_id']} • {q3['date']}",
         f"Rank {q3['rank']}; {float(q3['precip_mm']):.1f} mm"),
    ]
    draw_table(c, x, y + 5, [140, 180, 193], rows, 31, BLUE_BG, 7.9)
    x, y, w, h = draw_card(c, 628, 276, 278, 194, "Question / output paths", ORANGE_BG, ORANGE)
    draw_paragraph(
        c, x, y,
        "Q1  q1_monthly_temp_core\nQ2  q2_monthly_precip_core\n"
        "Q3  q3_top5_rain_days_core\n\n"
        "Parquet dùng cho Spark reads; CSV phục vụ kiểm tra và trình bày.",
        w, 8.4, 14,
    )
    c.showPage()

    # 5. Baseline, explain and next actions
    draw_header(c, 6, "5. Baseline, explain & next action",
                "Thời gian đo trên Windows local; dùng để so sánh trong cùng môi trường.")
    x, y, w, h = draw_card(c, 54, 112, 260, 137, "Baseline", BLUE_BG, BLUE)
    draw_paragraph(
        c, x, y,
        f"Pipeline (bỏ ingest): {elapsed:.0f} s\n"
        f"Input: {float(sizes['raw_json_gzip']):.1f} MB gzip\n"
        f"CSV: {csv_mb:.1f} MB • Parquet: {parquet_mb:.1f} MB\n"
        f"Spark {benchmark['environment']['spark_version']} • "
        f"Python {benchmark['environment']['python']}\nlocal[8] • Windows",
        w, 8.2, 13,
    )
    x, y, w, h = draw_card(c, 334, 112, 260, 137, "Execution plan", ORANGE_BG, ORANGE)
    draw_paragraph(
        c, x, y,
        'Spark explain("formatted") đã lưu tại:\n'
        "reports/evidence/clean_plan.txt\n\n"
        "Review Exchange trong plan nếu cần tối ưu; chưa tối ưu ngoài phạm vi checkpoint.",
        w, 8.3, 13,
    )
    x, y, w, h = draw_card(c, 614, 112, 292, 137, "Current blocker", GREEN_BG, GREEN)
    draw_paragraph(
        c, x, y,
        "Không còn blocker. Windows dùng adapter local filesystem Java đã ghim SHA-256 "
        "từ Maven Central. Cảnh báo winutils có thể vẫn xuất hiện; Parquet round-trip đã pass.",
        w, 8.3, 13,
    )

    x, y, w, h = draw_card(c, 54, 270, 530, 202, "Benchmark • CSV vs Parquet", WHITE, STROKE)
    draw_text(c, x, y, f"Q1 aggregation: CSV {q1_csv_s:.3f}s  •  "
              f"Parquet {q1_parquet_s:.3f}s  •  {time_ratio:.1f}×",
              8.7, INK, "ReportArial-Bold")
    bar_x, bar_width = x + 2, 355
    for i, (label, value, color) in enumerate((
        ("CSV", q1_csv_s, BLUE),
        ("Parquet", q1_parquet_s, TEAL),
    )):
        yy = y + 30 + i * 31
        draw_text(c, bar_x, yy, label, 8, MUTED)
        c.setFillColor(colors.HexColor("#E6ECF2"))
        c.roundRect(bar_x + 65, HEIGHT - yy - 10, bar_width, 10, 4, fill=1, stroke=0)
        c.setFillColor(color)
        c.roundRect(bar_x + 65, HEIGHT - yy - 10,
                    max(8, bar_width * value / q1_csv_s), 10, 4, fill=1, stroke=0)
        draw_text(c, bar_x + bar_width + 76, yy - 1, f"{value:.3f}s", 8, INK)
    draw_text(c, x, y + 99,
              f"Storage: CSV {csv_mb:.1f} MB vs flat Parquet {parquet_mb:.1f} MB "
              f"({size_ratio:.1f}× nhỏ hơn).", 8.3, INK)
    draw_text(c, x, y + 121,
              "Independent/API verification: ALL PASSED • Parquet round-trip: "
              + number(stats["clean_rows"]) + " rows.",
              8, GREEN, "ReportArial-Bold")

    x, y, w, h = draw_card(c, 610, 270, 296, 202, "Next action", GREEN_BG, GREEN)
    draw_paragraph(
        c, x, y,
        "1. Review bảng mẫu, quality report và formatted plan.\n"
        "2. Trình bày giới hạn ERA5 và quy tắc dịch mốc mưa 1 giờ.\n"
        "3. Chốt commit Checkpoint 3 sau review.\n\n"
        "Owner: Nhóm Đồ án BigData\nDeadline: theo lịch môn học.",
        w, 8.4, 14,
    )
    draw_centered(c, 54, 499, 852, "Commit: chưa ghi nhận trong pipeline log",
                  8.2, MUTED, "ReportArial-Bold")
    c.showPage()

    c.save()
    print(f"Created {OUTPUT} ({OUTPUT.stat().st_size:,} bytes)")


def number(value):
    return f"{int(value):,}"


if __name__ == "__main__":
    build()
