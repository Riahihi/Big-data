"""Chạy cả pipeline

    python run_pipeline.py
    python run_pipeline.py --skip-ingest          # không tải lại dữ liệu
    python run_pipeline.py --only clean analysis  # chạy vài bước

Các bước: ingest, daily, clean, analysis, advanced, verify, benchmark, figures
Thời gian từng bước lưu ở data/results/pipeline_run.json
"""
import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent

STEPS = [
    ("ingest", "Thu thập & lưu dữ liệu gốc", ["pipeline/ingest.py"]),
    ("daily", "Tải daily API để đối chiếu", ["pipeline/ingest.py", "--daily-check"]),
    ("clean", "Làm sạch & ghi Parquet", ["pipeline/clean.py"]),
    ("analysis", "Ba câu hỏi phân tích", ["pipeline/analysis.py"]),
    ("advanced", "Phân tích mở rộng", ["pipeline/advanced.py"]),
    ("verify", "Kiểm chứng độc lập", ["pipeline/verify.py"]),
    ("benchmark", "Đo hiệu năng (Mức B)", ["pipeline/benchmark.py"]),
    ("figures", "Vẽ biểu đồ", ["pipeline/figures.py"]),
]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--skip-ingest", action="store_true", help="không gọi API, dùng dữ liệu gốc đã tải")
    ap.add_argument("--only", nargs="+", choices=[s[0] for s in STEPS], help="chỉ chạy các bước này")
    a = ap.parse_args()

    selected = [s for s in STEPS if (not a.only or s[0] in a.only)
                and not (a.skip_ingest and s[0] in ("ingest", "daily"))]
    log_path = ROOT / "data" / "results" / "pipeline_run.json"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    # giữ lại log của các bước không chạy lần này
    log = json.loads(log_path.read_text()) if log_path.exists() else {"steps": {}}

    for key, title, args in selected:
        print(f"\n=== [{key}] {title} ===", flush=True)
        t0 = time.time()
        proc = subprocess.run([sys.executable, *args], cwd=ROOT)
        elapsed = round(time.time() - t0, 1)
        log["steps"][key] = {"title": title, "status": "ok" if proc.returncode == 0 else "failed",
                             "elapsed_s": elapsed, "finished_at": datetime.now().isoformat(timespec="seconds")}
        log_path.write_text(json.dumps(log, indent=2, ensure_ascii=False))
        if proc.returncode != 0:
            print(f"Bước {key} thất bại (mã {proc.returncode}) – dừng pipeline.")
            sys.exit(proc.returncode)
        print(f"--> {key} xong sau {elapsed}s")
    print("\nPipeline hoàn tất. Mở web demo:  python web/app.py")


if __name__ == "__main__":
    main()
