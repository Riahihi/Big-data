"""Bước 7: kiểm tra lại kết quả

V1 - tính lại câu 1, 2, 3 bằng pandas (đọc thẳng file json gốc, không qua spark) rồi so sánh
V2 - so với dữ liệu ngày do Open-Meteo tự tính
V3 - cộng tay 24 giờ của 1 ngày
V4 - số dòng parquet đọc lại có đúng không

Chạy: python pipeline/verify.py
"""
import gzip
import json
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from pipeline import config as C

TOL = 0.0101  # spark và pandas làm tròn khác nhau nên cho lệch 0.01


def load_raw_pandas(loc_ids, years):
    """Đọc json gốc bằng pandas, không dùng spark"""
    frames = []
    for lid in loc_ids:
        for y in years:
            p = C.RAW_DIR / lid / f"{lid}_{y}.json.gz"
            with gzip.open(p, "rt") as f:
                h = json.load(f)["hourly"]
            df = pd.DataFrame({"time": h["time"], "temperature_2m": h["temperature_2m"],
                               "precipitation": h["precipitation"]})
            df["location_id"] = lid
            frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    df["time"] = pd.to_datetime(df["time"])
    # nhiệt độ thì lấy theo ngày của thời điểm đo
    df["date"] = df["time"].dt.date.astype(str)
    df["year"] = df["time"].dt.year
    df["month"] = df["time"].dt.month
    # mưa thì lùi 1 giờ
    pt = df["time"] - pd.Timedelta(hours=1)
    df["pdate"] = pt.dt.date.astype(str)
    df["pyear"] = pt.dt.year
    df["pmonth"] = pt.dt.month
    return df


def compare(a, b, keys, cols):
    """So sánh 2 bảng theo khóa"""
    m = a.merge(b, on=keys, suffixes=("_indep", "_spark"), how="outer", indicator=True)
    out = {"rows_independent": len(a), "rows_spark": len(b),
           "rows_only_one_side": int((m["_merge"] != "both").sum())}
    for c in cols:
        diff = (m[f"{c}_indep"] - m[f"{c}_spark"]).abs()
        out[c] = {"max_abs_diff": round(float(diff.max()), 6),
                  "n_over_tolerance": int((diff > TOL).sum())}
    out["passed"] = bool(out["rows_only_one_side"] == 0 and all(out[c]["n_over_tolerance"] == 0 for c in cols))
    return out


def v1_independent_recompute(report):
    core = C.core_ids()
    years = range(C.CORE_START, C.CORE_END + 1)
    # đọc thêm năm sau vì cần giá trị 00:00 ngày 1/1 cho ngày 31/12
    raw = load_raw_pandas(core, range(C.CORE_START, C.CORE_END + 2))
    R = C.RESULTS_DIR
    temp = raw[raw["year"].isin(years)]
    rain = raw[raw["pyear"].isin(years)]

    # Câu 1
    q1_ind = temp.groupby(["location_id", "year", "month"], as_index=False)["temperature_2m"].mean() \
        .rename(columns={"temperature_2m": "avg_temp_c"})
    q1_sp = pd.read_parquet(R / "q1_monthly_temp_core")[["location_id", "year", "month", "avg_temp_c"]]
    report["V1_q1_monthly_temp"] = compare(q1_ind, q1_sp, ["location_id", "year", "month"], ["avg_temp_c"])

    # Câu 2
    day = rain.groupby(["location_id", "pdate", "pyear", "pmonth"], as_index=False) \
        .agg(precip_mm=("precipitation", "sum"), n=("precipitation", "count")) \
        .rename(columns={"pdate": "date", "pyear": "year", "pmonth": "month"})
    nt = temp.groupby(["location_id", "date"], as_index=False).agg(nt=("temperature_2m", "count"))
    day = day.merge(nt, on=["location_id", "date"], how="left")
    q2_ind = day.groupby(["location_id", "year", "month"], as_index=False)["precip_mm"].sum() \
        .rename(columns={"precip_mm": "total_precip_mm"})
    q2_sp = pd.read_parquet(R / "q2_monthly_precip_core")[["location_id", "year", "month", "total_precip_mm"]]
    report["V1_q2_monthly_precip"] = compare(q2_ind, q2_sp, ["location_id", "year", "month"], ["total_precip_mm"])

    # Câu 3, rank(method='min') giống rank() bên spark
    day["precip_mm"] = day["precip_mm"].round(2)
    complete = day[(day["n"] == 24) & (day["nt"] == 24)].copy()
    complete["rank"] = complete.groupby("location_id")["precip_mm"].rank(method="min", ascending=False)
    q3_ind = complete[complete["rank"] <= 5][["location_id", "date", "precip_mm", "rank"]]
    q3_sp = pd.read_parquet(R / "q3_top5_rain_days_core")[["location_id", "date", "precip_mm", "rank"]]
    q3_sp["date"] = q3_sp["date"].astype(str)
    report["V1_q3_top5"] = compare(q3_ind, q3_sp, ["location_id", "date"], ["precip_mm", "rank"])
    return raw


def v2_open_meteo_daily(report):
    files = sorted(C.RAW_DAILY_DIR.glob("*_daily.json.gz")) if C.RAW_DAILY_DIR.exists() else []
    if not files:
        report["V2_open_meteo_daily"] = {"skipped": "chưa tải daily API (python pipeline/ingest.py --daily-check)"}
        return
    rows = []
    for p in files:
        with gzip.open(p, "rt") as f:
            d = json.load(f)["daily"]
        df = pd.DataFrame(d).rename(columns={"time": "date"})
        df["location_id"] = p.name.split("_")[0]
        rows.append(df)
    api = pd.concat(rows, ignore_index=True)
    sp = pd.read_parquet(C.CLEAN_DIR / "daily", columns=["location_id", "date", "t_mean", "t_max", "t_min", "precip_mm"])
    sp["date"] = sp["date"].astype(str)
    m = api.merge(sp, on=["location_id", "date"], how="inner")
    res = {"n_days_compared": len(m), "n_locations": int(m["location_id"].nunique())}
    for api_col, sp_col in [("precipitation_sum", "precip_mm"), ("temperature_2m_mean", "t_mean"),
                            ("temperature_2m_max", "t_max"), ("temperature_2m_min", "t_min")]:
        diff = (m[api_col] - m[sp_col]).abs()
        res[f"{api_col}_vs_{sp_col}"] = {
            "mae": round(float(diff.mean()), 4), "max_abs_diff": round(float(diff.max()), 3),
            "pct_within_0.1": round(float(100 * (diff <= 0.1 + 1e-9).mean()), 3),
            "pearson_r": round(float(np.corrcoef(m[api_col], m[sp_col])[0, 1]), 6),
        }
    # Tmax/Tmin của API là cực trị thật trong giờ nên luôn >= / <= giá trị theo giờ
    res["tmax_api_ge_hourly_pct"] = round(float(100 * (m["temperature_2m_max"] >= m["t_max"] - 1e-9).mean()), 3)
    res["tmin_api_le_hourly_pct"] = round(float(100 * (m["temperature_2m_min"] <= m["t_min"] + 1e-9).mean()), 3)
    res["passed"] = bool(res["precipitation_sum_vs_precip_mm"]["pct_within_0.1"] >= 99.9
                         and res["temperature_2m_mean_vs_t_mean"]["pct_within_0.1"] >= 99.9
                         and res["tmax_api_ge_hourly_pct"] == 100 and res["tmin_api_le_hourly_pct"] == 100)
    report["V2_open_meteo_daily"] = res


def v3_manual_sample(report, raw):
    """Lấy ngày mưa lớn nhất ở Đà Nẵng rồi cộng tay 24 giờ"""
    q3 = pd.read_parquet(C.RESULTS_DIR / "q3_top5_rain_days_core")
    top = q3[(q3["location_id"] == "DNG") & (q3["rank"] == 1)].iloc[0]
    hours = raw[(raw["location_id"] == "DNG") & (raw["pdate"] == str(top["date"]))].sort_values("time")
    report["V3_manual_sample"] = {
        "location_id": "DNG", "date": str(top["date"]),
        "hourly_precip_mm": hours["precipitation"].tolist(),
        "hour_labels": hours["time"].dt.strftime("%d/%m %H:%M").tolist(),
        "manual_sum_mm": round(float(hours["precipitation"].sum()), 2),
        "spark_value_mm": float(top["precip_mm"]),
        "passed": bool(abs(hours["precipitation"].sum() - top["precip_mm"]) < 0.01),
    }


def v4_parquet_and_completeness(report):
    stats = json.loads((C.RESULTS_DIR / "clean_stats.json").read_text())
    report["V4_parquet_roundtrip"] = {
        "clean_rows": stats["clean_rows"], "expected_rows": stats["expected_rows"],
        "missing_rows": stats["missing_rows"], "duplicates_removed": stats["duplicates_removed"],
        "passed": bool(stats["parquet_roundtrip_ok"]),
    }


def main():
    report = {}
    raw = v1_independent_recompute(report)
    v2_open_meteo_daily(report)
    v3_manual_sample(report, raw)
    v4_parquet_and_completeness(report)
    report["all_passed"] = all(v.get("passed", True) for v in report.values() if isinstance(v, dict))
    (C.RESULTS_DIR / "verification_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False))
    for k, v in report.items():
        if isinstance(v, dict):
            print(k, "->", "PASS" if v.get("passed") else ("SKIP" if "skipped" in v else "FAIL"))
    print("ALL PASSED" if report["all_passed"] else "CÓ KIỂM TRA THẤT BẠI – xem verification_report.json")


if __name__ == "__main__":
    main()
