"""Bước 5: 3 câu hỏi phân tích chính

- Câu 1: nhiệt độ TB tháng = trung bình các giá trị theo giờ trong tháng
- Câu 2: mưa giờ -> mưa ngày -> mưa tháng. Mưa ngày D lấy từ 01:00 ngày D đến 00:00 ngày D+1
  (vì giá trị lúc H là mưa của giờ trước đó, đã so với dữ liệu ngày của Open-Meteo thấy khớp)
- Câu 3: top 5 ngày mưa lớn nhất, chỉ xét ngày đủ 24h. Dùng rank() nên bằng nhau ở hạng 5 thì lấy hết

core = 10 điểm, 2019-2023 (theo đề), full = 34 tỉnh, 2016-2025

Chạy: python pipeline/analysis.py
"""
import sys
import time
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pipeline import config as C
from pipeline.spark_utils import get_spark

LOC_COLS = ["location_id", "location_name", "site_name", "region", "is_core"]


def save(df: DataFrame, name: str, order_by=None):
    """Lưu bảng kết quả ra parquet và csv"""
    if order_by:
        df = df.orderBy(*order_by)
    df = df.coalesce(1)
    df.write.mode("overwrite").parquet(str(C.RESULTS_DIR / name))
    # utf-8-sig để mở bằng excel không bị lỗi font
    df.toPandas().to_csv(C.RESULTS_DIR / f"{name}.csv", index=False, encoding="utf-8-sig")


def core_filter(df: DataFrame) -> DataFrame:
    """Lọc theo phạm vi của đề: 10 điểm, 2019-2023"""
    return df.filter(F.col("is_core") & F.col("year").between(C.CORE_START, C.CORE_END))


def build_daily(hourly: DataFrame) -> DataFrame:
    """Gộp giờ thành ngày. Nhiệt độ gộp theo date, mưa gộp theo precip_date"""
    inst = hourly.groupBy(*LOC_COLS, "date").agg(
        F.round(F.avg("temperature_2m"), 2).alias("t_mean"),
        F.max("temperature_2m").alias("t_max"),
        F.min("temperature_2m").alias("t_min"),
        F.round(F.avg("relative_humidity_2m"), 1).alias("rh_mean"),
        F.max("apparent_temperature").alias("at_max"),
        F.max("wind_gusts_10m").alias("gust_max"),
        F.count("temperature_2m").alias("n_temp_hours"),
    )
    rain = hourly.groupBy("location_id", F.col("precip_date").alias("date")).agg(
        F.round(F.sum("precipitation"), 2).alias("precip_mm"),
        F.max("precipitation").alias("precip_max_1h"),
        F.sum((F.col("precipitation") >= 0.1).cast("int")).alias("wet_hours"),
        F.count("precipitation").alias("n_precip_hours"),
    )
    # left join theo ngày của nhiệt độ, bỏ ngày 31/12/2015 (chỉ có 1 giá trị mưa)
    return (
        inst.join(rain, ["location_id", "date"], "left")
        .fillna({"n_precip_hours": 0})
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        # ngày cuối chỉ có 23 giá trị mưa nên bị tính là thiếu
        .withColumn("is_complete", (F.col("n_precip_hours") == 24) & (F.col("n_temp_hours") == 24))
    )


def q1_monthly_temperature(hourly: DataFrame, daily: DataFrame) -> DataFrame:
    """Câu 1: nhiệt độ TB từng tháng của từng năm"""
    from_hourly = hourly.groupBy(*LOC_COLS, "year", "month").agg(
        F.round(F.avg("temperature_2m"), 2).alias("avg_temp_c"),
        F.min("temperature_2m").alias("min_temp_c"),
        F.max("temperature_2m").alias("max_temp_c"),
        F.count("temperature_2m").alias("n_hours"),
    )
    # thêm TB max/min ngày
    from_daily = daily.groupBy("location_id", "year", "month").agg(
        F.round(F.avg("t_max"), 2).alias("avg_daily_max_c"),
        F.round(F.avg("t_min"), 2).alias("avg_daily_min_c"),
        F.count("*").alias("n_days"),
    )
    return (
        from_hourly.join(from_daily, ["location_id", "year", "month"])
        # % số giờ có dữ liệu
        .withColumn("coverage_pct", F.round(100 * F.col("n_hours") / (F.col("n_days") * 24), 2))
        .withColumn("year_month", F.format_string("%d-%02d", "year", "month"))
    )


def q1_climatology(q1: DataFrame) -> DataFrame:
    """TB của từng tháng qua các năm"""
    return q1.groupBy(*LOC_COLS, "month").agg(
        F.round(F.avg("avg_temp_c"), 2).alias("clim_temp_c"),
        F.round(F.min("avg_temp_c"), 2).alias("coldest_year_temp_c"),
        F.round(F.max("avg_temp_c"), 2).alias("warmest_year_temp_c"),
        F.count("*").alias("n_years"),
    )


def q2_monthly_precipitation(daily: DataFrame) -> DataFrame:
    """Câu 2: tổng mưa tháng"""
    return (
        daily.groupBy(*LOC_COLS, "year", "month")
        .agg(
            F.round(F.sum("precip_mm"), 1).alias("total_precip_mm"),
            F.sum((F.col("precip_mm") >= 1.0).cast("int")).alias("rain_days_ge_1mm"),
            F.round(F.max("precip_mm"), 1).alias("max_daily_precip_mm"),
            F.count("*").alias("n_days"),
            F.sum(F.col("is_complete").cast("int")).alias("n_complete_days"),
        )
        .withColumn("is_complete_month", F.col("n_days") == F.col("n_complete_days"))
        .withColumn("year_month", F.format_string("%d-%02d", "year", "month"))
    )


def q3_top_rain_days(daily: DataFrame, top_n=5) -> DataFrame:
    """Câu 3: top n ngày mưa lớn nhất mỗi địa điểm"""
    w = Window.partitionBy("location_id").orderBy(F.col("precip_mm").desc())
    ranked = (
        daily.filter("is_complete")
        .withColumn("rank", F.rank().over(w))
        .withColumn("n_tied", F.count("*").over(Window.partitionBy("location_id", "precip_mm")))
        .filter(F.col("rank") <= top_n)
    )
    return ranked.select(
        *LOC_COLS, "rank", "date", "precip_mm", "precip_max_1h", "wet_hours", "t_mean", "n_tied",
        rain_class(F.col("precip_mm")).alias("rain_class"),
    )


def rain_class(p):
    """Phân cấp mưa 24h theo QĐ 18/2021/QĐ-TTg"""
    return (F.when(p > 100, "Mưa rất to (>100 mm)")
             .when(p > 50, "Mưa to (51–100 mm)")
             .when(p >= 16, "Mưa vừa (16–50 mm)")
             .when(p >= 1, "Mưa nhỏ (1–15 mm)")
             .otherwise("Không mưa đáng kể (<1 mm)"))


def main():
    t0 = time.time()
    spark = get_spark("analysis")
    C.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    hourly = spark.read.parquet(str(C.CLEAN_DIR / "hourly"))

    # bảng ngày dùng cho nhiều phần nên lưu lại
    daily = build_daily(hourly)
    daily.repartition("location_id").write.mode("overwrite").partitionBy("location_id") \
        .parquet(str(C.CLEAN_DIR / "daily"))
    daily = spark.read.parquet(str(C.CLEAN_DIR / "daily")).cache()

    q1 = q1_monthly_temperature(hourly, daily).cache()
    q2 = q2_monthly_precipitation(daily).cache()
    order_m = ["location_id", "year", "month"]

    # core = theo đề, full = mở rộng
    save(core_filter(q1), "q1_monthly_temp_core", order_m)
    save(q1, "q1_monthly_temp_full", order_m)
    save(q1_climatology(core_filter(q1)), "q1_climatology_core", ["location_id", "month"])
    save(q1_climatology(q1), "q1_climatology_full", ["location_id", "month"])

    save(core_filter(q2), "q2_monthly_precip_core", order_m)
    save(q2, "q2_monthly_precip_full", order_m)

    save(q3_top_rain_days(core_filter(daily)), "q3_top5_rain_days_core", ["location_id", "rank", "date"])
    save(q3_top_rain_days(daily), "q3_top5_rain_days_full", ["location_id", "rank", "date"])

    print(f"analysis done in {time.time() - t0:.1f}s")
    spark.stop()


if __name__ == "__main__":
    main()
