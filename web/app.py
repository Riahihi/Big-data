"""Web demo

    python web/app.py              # http://127.0.0.1:7890
    python web/app.py --port 8000

Kết quả đọc từ các file parquet trong data/results. Phần truy vấn trực tiếp dùng 1 SparkSession chung.
"""
import argparse
import json
import re
import sys
import threading
import time
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from pipeline import config as C

STATIC = Path(__file__).resolve().parent / "static"
app = FastAPI(title="Weather Big Data – Việt Nam")


# đọc bảng kết quả
def records(df: pd.DataFrame):
    """Đổi DataFrame sang list dict để trả json"""
    df = df.copy()
    for c in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[c]) or df[c].dtype == object:
            df[c] = df[c].map(lambda v: v.isoformat() if hasattr(v, "isoformat") else v)
    return json.loads(df.to_json(orient="records", force_ascii=False, date_format="iso"))


@lru_cache(maxsize=None)
def table(name: str) -> pd.DataFrame:
    p = C.RESULTS_DIR / name
    if not p.exists():
        raise HTTPException(404, f"chưa có bảng {name} – hãy chạy python run_pipeline.py")
    return pd.read_parquet(p)


def read_json(name):
    p = C.RESULTS_DIR / name
    return json.loads(p.read_text()) if p.exists() else None


@lru_cache(maxsize=1)
def locations():
    loc = pd.DataFrame(C.locations_as_dicts())
    grid = pd.read_parquet(C.CLEAN_DIR / "hourly", columns=["location_id", "grid_latitude", "grid_longitude",
                                                            "elevation_m"]).drop_duplicates("location_id")
    grid["location_id"] = grid["location_id"].astype(str)
    return loc.merge(grid, on="location_id", how="left")


# aPI đọc kết quả có sẵn
@app.get("/api/overview")
def overview():
    manifest = [json.loads(l) for l in C.MANIFEST.read_text().splitlines() if l.strip()]
    q = table("quality_report")
    return {
        "locations": records(locations()),
        "core_ids": C.core_ids(),
        "scope": {"full": [C.START_YEAR, C.END_YEAR], "core": [C.CORE_START, C.CORE_END]},
        "variables": C.HOURLY_VARS, "model": C.MODEL, "timezone": C.TIMEZONE, "api": C.API_URL,
        "manifest": {"files": len(manifest), "records": sum(m["n_records"] for m in manifest),
                     "bytes_raw": sum(m["bytes_raw"] for m in manifest),
                     "bytes_gz": sum(m["bytes_gz"] for m in manifest),
                     "first_fetch": min(m["fetched_at"] for m in manifest),
                     "last_fetch": max(m["fetched_at"] for m in manifest),
                     "sample": manifest[:3]},
        "clean_stats": read_json("clean_stats.json"),
        "quality": records(q[["location_id", "location_name", "year", "expected_hours", "missing_rows",
                              "rows_imputed"]]),
        "verification": read_json("verification_report.json"),
        "benchmark": read_json("benchmark.json"),
        "pipeline_run": read_json("pipeline_run.json"),
        "cluster_info": read_json("a9_cluster_info.json"),
    }


@app.get("/api/q/{qid}")
def question(qid: str, scope: str = "core"):
    if scope not in ("core", "full"):
        raise HTTPException(400, "scope phải là core hoặc full")
    names = {"q1": [f"q1_monthly_temp_{scope}", f"q1_climatology_{scope}"],
             "q2": [f"q2_monthly_precip_{scope}"],
             "q3": [f"q3_top5_rain_days_{scope}"]}
    if qid not in names:
        raise HTTPException(404)
    return {n: records(table(n)) for n in names[qid]}


ADVANCED = ["a1_climate_indices", "a2_heatwave_summary", "a3_dry_spells_top3", "a4_rain_class_distribution",
            "a5_temperature_trends", "a5_annual_anomaly", "a5_national_annual_anomaly", "a7_return_levels",
            "a8_multiday_rain_events", "a9_climate_clusters"]


@app.get("/api/advanced")
def advanced():
    return {n: records(table(n)) for n in ADVANCED}


@app.get("/api/heatwaves/{location_id}")
def heatwave_events(location_id: str):
    hw = table("a2_heatwave_events")
    return records(hw[hw["location_id"] == location_id].sort_values("start_date"))


@app.get("/api/diurnal/{location_id}")
def diurnal(location_id: str):
    d = table("a6_diurnal_cycle")
    return records(d[d["location_id"] == location_id])


@app.get("/api/daily/{location_id}")
def daily(location_id: str, start: str = "2016-01-01", end: str = "2025-12-31"):
    part = C.CLEAN_DIR / "daily" / f"location_id={location_id}"
    if not part.exists():
        raise HTTPException(404, "không có địa điểm này")
    d = pd.read_parquet(part, columns=["date", "t_mean", "t_max", "t_min", "precip_mm"])
    d["date"] = d["date"].astype(str)
    d = d[(d["date"] >= start) & (d["date"] <= end)].sort_values("date")
    return records(d)


# spark trực tiếp
_spark, _lock = None, threading.Lock()
VIEWS = {"hourly": C.CLEAN_DIR / "hourly", "daily": C.CLEAN_DIR / "daily",
         "q1_monthly_temp": C.RESULTS_DIR / "q1_monthly_temp_full",
         "q2_monthly_precip": C.RESULTS_DIR / "q2_monthly_precip_full",
         "climate_indices": C.RESULTS_DIR / "a1_climate_indices",
         "heatwave_events": C.RESULTS_DIR / "a2_heatwave_events"}


def spark():
    """Tạo spark 1 lần rồi dùng lại"""
    global _spark
    if _spark is None:
        from pipeline.spark_utils import get_spark
        _spark = get_spark("web-demo", threads=4)
        _spark.conf.set("spark.sql.runSQLOnFiles", "false")   # chặn SELECT trực tiếp từ đường dẫn file
        for v, p in VIEWS.items():
            _spark.read.parquet(str(p)).createOrReplaceTempView(v)
    return _spark


# chỉ cho select, chặn các lệnh ghi và các hàm gọi java
FORBIDDEN = re.compile(r"\b(insert|create|drop|alter|delete|update|merge|truncate|cache|uncache|refresh|set|reset|"
                       r"add|load|msck|grant|revoke|use|call|analyze|reflect|java_method|try_reflect|"
                       r"input_file_name|export|import|directory)\b", re.I)


class SqlIn(BaseModel):
    sql: str = Field(max_length=5000)
    limit: int = Field(default=500, ge=1, le=5000)


QUERY_TIMEOUT_S = 30  # quá 30s thì hủy


def run_df(df, limit):
    """Chạy query, trả về kết quả, thời gian và physical plan"""
    sc = _spark.sparkContext
    group = f"web-{time.time_ns()}"
    sc.setJobGroup(group, "web demo query", interruptOnCancel=True)
    timer = threading.Timer(QUERY_TIMEOUT_S, lambda: sc.cancelJobGroup(group))
    timer.start()
    t0 = time.perf_counter()
    try:
        rows = df.limit(limit).toPandas()
    except Exception as e:
        if time.perf_counter() - t0 >= QUERY_TIMEOUT_S - 1:
            raise HTTPException(400, f"Truy vấn vượt quá {QUERY_TIMEOUT_S} giây và đã bị hủy.")
        raise
    finally:
        timer.cancel()
    elapsed = time.perf_counter() - t0
    plan = df._jdf.queryExecution().executedPlan().toString()
    return {"columns": list(rows.columns), "rows": records(rows), "elapsed_s": round(elapsed, 3),
            "n_rows": len(rows), "plan": plan[:6000]}


@app.post("/api/spark/sql")
def spark_sql(body: SqlIn):
    sql = body.sql.strip().rstrip(";").strip()
    if ";" in sql:
        raise HTTPException(400, "Chỉ chạy một câu lệnh.")
    if not re.match(r"^(select|with)\b", sql, re.I):
        raise HTTPException(400, "Chỉ hỗ trợ câu lệnh SELECT / WITH.")
    if FORBIDDEN.search(sql):
        raise HTTPException(400, f"Từ khóa không được phép: {FORBIDDEN.search(sql).group(0)}")
    with _lock:
        s = spark()
        try:
            return run_df(s.sql(sql), body.limit)
        except HTTPException:
            raise
        except Exception as e:  # sai cú pháp, sai tên cột...
            msg = str(e).split("\n")[0][:600]
            raise HTTPException(400, msg)


class TopRainIn(BaseModel):
    locations: list[str] = Field(default_factory=list, max_length=34)
    start: str = "2019-01-01"
    end: str = "2023-12-31"
    n: int = Field(default=5, ge=1, le=50)


@app.post("/api/spark/top-rain")
def spark_top_rain(body: TopRainIn):
    """Chạy lại câu 3 với tham số chọn trên web"""
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", body.start) or not re.match(r"^\d{4}-\d{2}-\d{2}$", body.end):
        raise HTTPException(400, "Ngày phải có dạng YYYY-MM-DD")
    valid = {l[0] for l in C.LOCATIONS}
    locs = [l for l in body.locations if l in valid] or sorted(valid)
    with _lock:
        s = spark()
        d = (s.read.parquet(str(C.CLEAN_DIR / "daily"))
             .filter(F.col("location_id").isin(locs))  # partition pruning
             .filter(F.col("date").between(F.lit(body.start).cast("date"), F.lit(body.end).cast("date")))
             .filter("is_complete"))
        w = Window.partitionBy("location_id").orderBy(F.col("precip_mm").desc())
        q = (d.withColumn("rank", F.rank().over(w)).filter(F.col("rank") <= body.n)
             .select("location_id", "location_name", "rank", F.col("date").cast("string").alias("date"),
                     "precip_mm", "precip_max_1h", "wet_hours", "t_mean")
             .orderBy("location_id", "rank", "date"))
        return run_df(q, 2000)


# trang tĩnh & file kết quả
@app.get("/download/{name}")
def download(name: str):
    p = (C.RESULTS_DIR / name).resolve()
    if p.parent != C.RESULTS_DIR.resolve() or p.suffix != ".csv" or not p.exists():
        raise HTTPException(404)
    return FileResponse(p, filename=name, media_type="text/csv")


@app.get("/figures/{name}")
def figure(name: str):
    p = (C.FIGURES / name).resolve()
    if p.parent != C.FIGURES.resolve() or p.suffix != ".png" or not p.exists():
        raise HTTPException(404)
    return FileResponse(p)


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.exception_handler(HTTPException)
def http_error(_, exc: HTTPException):
    return JSONResponse({"error": exc.detail}, status_code=exc.status_code)


app.mount("/static", StaticFiles(directory=STATIC), name="static")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=7890)
    a = ap.parse_args()
    uvicorn.run(app, host=a.host, port=a.port, log_level="info")
