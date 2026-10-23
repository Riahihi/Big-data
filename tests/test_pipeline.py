"""Test các hàm làm sạch và phân tích trên dữ liệu giả

Dữ liệu thật không có giờ thiếu hay trùng nên phải tự tạo lỗi để test.
Chạy: python -m pytest tests -q
"""
import gzip
import json
import math
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import functions as F

from pipeline import config as C
from pipeline.advanced import ols, spells
from pipeline.analysis import build_daily, q3_top_rain_days
from pipeline.clean import build_clean, interpolate_short_gaps
from pipeline.spark_utils import get_spark


@pytest.fixture(scope="session")
def spark():
    s = get_spark("tests", threads=2)
    yield s
    s.stop()


def write_raw(dirpath: Path, loc="HNI", year=2020, edit=None):
    """Tạo file json giống format của Open-Meteo, edit() để chèn lỗi"""
    t0 = datetime(year, 1, 1)
    n = (datetime(year + 1, 1, 1) - t0).days * 24
    times = [(t0 + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M") for i in range(n)]
    hourly = {"time": times}
    for v in C.HOURLY_VARS:
        hourly[v] = [0.0] * n
    hourly["temperature_2m"] = [round(25 + 5 * math.sin(2 * math.pi * i / 24), 2) for i in range(n)]
    hourly["relative_humidity_2m"] = [80.0] * n
    if edit:
        edit(hourly)
    payload = {"latitude": 21.0, "longitude": 105.75, "elevation": 19.0, "utc_offset_seconds": 25200,
               "timezone": C.TIMEZONE, "hourly": hourly}
    p = dirpath / loc / f"{loc}_{year}.json.gz"
    p.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(p, "wt") as f:
        json.dump(payload, f)
    return p


def test_clean_detects_missing_duplicate_out_of_range_and_imputes(spark, tmp_path):
    def edit(h):
        # bỏ 2 giờ index 10, 11
        for v in list(h):
            del h[v][10:12]
        h["temperature_2m"][100] = 99.0  # ngoài ngưỡng -> null -> nội suy (gap 1 giờ)
        for i in range(200, 206):  # khoảng trống 6 giờ -> KHÔNG nội suy
            h["temperature_2m"][i] = None
        h["precipitation"][300] = -1.0  # mưa âm -> null, không nội suy

    write_raw(tmp_path / "a", edit=edit)
    write_raw(tmp_path / "b")  # bản tải thứ hai cùng (địa điểm, năm) -> trùng
    clean, quality, keep, stats, raw = build_clean(spark, str(tmp_path / "*" / "*" / "*.json.gz"))
    q = quality.collect()[0]

    assert q["expected_hours"] == 8784  # năm nhuận 2020
    assert stats["duplicates_removed"] == 8784 - 2  # mọi giờ của file a đều trùng với file b
    # bỏ trùng xong thì file b (đủ giờ) được giữ nên không còn giờ thiếu
    assert q["missing_rows"] == 0


def test_rules_on_single_file(spark, tmp_path):
    def edit(h):
        # chèn lỗi theo chỉ số giờ gốc TRƯỚC, rồi mới xóa giờ (xóa trước sẽ làm lệch chỉ số phía sau)
        h["temperature_2m"][100] = 99.0
        for i in range(200, 206):
            h["temperature_2m"][i] = None
        h["precipitation"][300] = -1.0
        for v in list(h):
            del h[v][10:12]

    write_raw(tmp_path, edit=edit)
    clean, quality, keep, stats, raw = build_clean(spark, str(tmp_path / "*" / "*.json.gz"))
    q = quality.collect()[0]
    assert stats["duplicates_removed"] == 0
    assert q["missing_rows"] == 2  # R3: 2 giờ bị bỏ được phát hiện
    assert q["out_of_range_temperature_2m"] == 1  # R4
    assert q["out_of_range_precipitation"] == 1
    assert q["null_after_clean_temperature_2m"] == 6  # R5: khoảng 6 giờ không nội suy
    assert q["null_after_clean_precipitation"] == 3  # 2 giờ thiếu + 1 giá trị âm

    rows = {r["hour_idx"]: r for r in clean.select(
        ((F.col("ts").cast("long") - F.lit(int(datetime(2020, 1, 1).timestamp()) - 7 * 3600)) / 3600)
        .cast("int").alias("hour_idx"), "temperature_2m", "temp_imputed", "is_missing_row").collect()}
    # Giờ thiếu index 10, 11 được nội suy tuyến tính giữa giờ 9 và giờ 12
    t9, t12 = rows[9]["temperature_2m"], rows[12]["temperature_2m"]
    assert rows[10]["is_missing_row"] and rows[10]["temp_imputed"]
    assert rows[10]["temperature_2m"] == pytest.approx(t9 + (t12 - t9) / 3, abs=0.01)
    assert rows[11]["temperature_2m"] == pytest.approx(t9 + 2 * (t12 - t9) / 3, abs=0.01)
    # Giá trị 99 °C bị thay bằng trung bình hai giờ kề
    assert rows[100]["temperature_2m"] == pytest.approx((rows[99]["temperature_2m"] + rows[101]["temperature_2m"]) / 2, abs=0.01)
    assert rows[203]["temperature_2m"] is None


def test_interpolation_respects_max_gap(spark):
    base = datetime(2020, 1, 1)
    vals = [1.0, None, None, None, 5.0, None, None, None, None, 10.0]  # gap 3 -> nội suy; gap 4 -> không
    df = spark.createDataFrame([("HNI", base + timedelta(hours=i), v) for i, v in enumerate(vals)],
                               "location_id string, ts timestamp, temperature_2m double")
    out = interpolate_short_gaps(df, cols=["temperature_2m"], max_gap=3).orderBy("ts").collect()
    got = [r["temperature_2m"] for r in out]
    assert got[:5] == [1.0, 2.0, 3.0, 4.0, 5.0]
    assert got[5:9] == [None] * 4


def _daily(spark, rows):
    return spark.createDataFrame(rows, "location_id string, location_name string, site_name string, region string, "
                                       "is_core boolean, date date, year int, month int, precip_mm double, t_max double, "
                                       "t_mean double, precip_max_1h double, wet_hours int, is_complete boolean")


def test_q3_keeps_ties_at_fifth_place(spark):
    p = [90.0, 80.0, 70.0, 60.0, 50.0, 50.0, 10.0]
    rows = [("HNI", "Hà Nội", "Hà Nội", "R", True, date(2020, 1, i + 1), 2020, 1, v, 30.0, 25.0, 5.0, 3, True)
            for i, v in enumerate(p)]
    rows.append(("HNI", "Hà Nội", "Hà Nội", "R", True, date(2020, 2, 1), 2020, 2, 999.0, 30.0, 25.0, 5.0, 3, False))
    out = q3_top_rain_days(_daily(spark, rows)).orderBy("rank", "date").collect()
    assert [r["precip_mm"] for r in out] == [90, 80, 70, 60, 50, 50]  # đồng hạng 5 -> giữ cả hai, bỏ ngày thiếu dữ liệu
    assert [r["rank"] for r in out] == [1, 2, 3, 4, 5, 5]
    assert out[-1]["n_tied"] == 2


def test_spells_gaps_and_islands(spark):
    hot = [1, 1, 1, 0, 1, 1, 0, 0, 1, 1, 1, 1]
    rows = [("HNI", "Hà Nội", "Hà Nội", "R", True, date(2020, 5, 1) + timedelta(days=i), 2020, 5, 0.0,
             36.0 if h else 30.0, 28.0, 0.0, 0, True) for i, h in enumerate(hot)]
    s = spells(_daily(spark, rows), F.col("t_max") >= 35).orderBy("start_date").collect()
    assert [r["length_days"] for r in s] == [3, 2, 4]
    assert s[2]["start_date"] == date(2020, 5, 9) and s[2]["end_date"] == date(2020, 5, 12)


def test_ols_matches_numpy(spark):
    rng = np.random.default_rng(0)
    x = np.arange(120) / 12 + 2016
    y = 0.03 * x + rng.normal(0, 0.3, x.size)
    df = spark.createDataFrame([("A", float(a), float(b)) for a, b in zip(x, y)], "g string, x double, y double")
    r = ols(df, "x", "y", ["g"]).collect()[0]
    slope, intercept = np.polyfit(x, y, 1)
    resid = y - (slope * x + intercept)
    se = math.sqrt((resid ** 2).sum() / (x.size - 2) / ((x - x.mean()) ** 2).sum())
    assert r["slope"] == pytest.approx(slope, rel=1e-9)
    assert r["se"] == pytest.approx(se, rel=1e-6)


def test_daily_sum_and_completeness(spark):
    """Mưa ngày D lấy từ 01:00 ngày D đến 00:00 ngày D+1"""
    t0 = datetime(2020, 1, 1)
    rows = []
    for h in range(49):  # 01/01 00:00 -> 03/01 00:00
        ts = t0 + timedelta(hours=h)
        p = None if h == 30 else 0.5  # thiếu 1 giờ mưa thuộc ngày 02/01
        if h == 24:
            p = 7.0  # giá trị 00:00 ngày 2/1 là mưa của ngày 1/1
        rows.append(("HNI", "Hà Nội", "Hà Nội", "R", True, ts.date(), (ts - timedelta(hours=1)).date(),
                     20.0 + h % 24, p, 80.0, 25.0, 10.0))
    df = spark.createDataFrame(rows, "location_id string, location_name string, site_name string, region string, "
                                     "is_core boolean, date date, precip_date date, temperature_2m double, "
                                     "precipitation double, relative_humidity_2m double, apparent_temperature double, "
                                     "wind_gusts_10m double")
    d = {r["date"]: r for r in build_daily(df).collect()}
    assert date(2019, 12, 31) not in d  # "ngày mưa" chỉ có 1 giá trị lúc 00:00 bị loại
    assert d[date(2020, 1, 1)]["precip_mm"] == pytest.approx(23 * 0.5 + 7.0)
    assert d[date(2020, 1, 1)]["t_max"] == 43.0 and d[date(2020, 1, 1)]["is_complete"]
    assert d[date(2020, 1, 2)]["n_precip_hours"] == 23 and not d[date(2020, 1, 2)]["is_complete"]
    assert d[date(2020, 1, 2)]["precip_mm"] == pytest.approx(22 * 0.5 + 0.5)
    assert not d[date(2020, 1, 3)]["is_complete"]  # chỉ có 1 giờ nhiệt độ, thiếu mưa
