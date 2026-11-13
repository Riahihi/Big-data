"""Phần mở rộng, chạy trên toàn bộ 34 tỉnh 2016-2025

A1 chỉ số cực trị theo năm (TXx, TNn, SU35, Rx1day, Rx5day, CDD, CWD...)
A2 đợt nắng nóng >= 3 ngày liên tiếp
A3 chuỗi ngày khô dài nhất
A4 số ngày theo cấp mưa
A5 dị thường và xu hướng nhiệt độ (hồi quy tuyến tính)
A6 chu trình ngày đêm
A7 chu kỳ lặp lại mưa lớn (Gumbel)
A8 các đợt mưa lớn 3 ngày
A9 phân cụm khí hậu bằng KMeans

Chạy sau clean.py và analysis.py:  python pipeline/advanced.py
"""
import json
import math
import sys
import time
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pipeline import config as C
from pipeline.analysis import LOC_COLS, rain_class, save
from pipeline.spark_utils import get_spark

HOT_ABS = 35.0      # ngưỡng nắng nóng (°C) theo QĐ 18/2021/QĐ-TTg
DRY_MM = 1.0        # ngày khô: lượng mưa < 1 mm (quy ước ETCCDI)
MIN_SPELL = 3       # độ dài tối thiểu của một "đợt" (ngày)


# hàm dùng chung: gaps-and-islands
def spells(daily: DataFrame, condition, by_year=False) -> DataFrame:
    """Tìm các chuỗi ngày liên tiếp thỏa điều kiện (gaps and islands).

    Các ngày liên tiếp thì date - row_number bằng nhau nên dùng làm khóa để group.
    """
    part = ["location_id", "year"] if by_year else ["location_id"]
    w = Window.partitionBy(*part).orderBy("date")
    flagged = (daily.filter(condition)
               .withColumn("_rn", F.row_number().over(w))
               .withColumn("_grp", F.expr("date_sub(date, _rn)")))
    return flagged.groupBy(*part, "_grp").agg(
        F.min("date").alias("start_date"),
        F.max("date").alias("end_date"),
        F.count("*").alias("length_days"),
        F.max("t_max").alias("peak_tmax"),
        F.round(F.avg("t_max"), 2).alias("mean_tmax"),
        F.round(F.sum("precip_mm"), 1).alias("total_precip_mm"),
    ).drop("_grp")


# A1. Chỉ số khí hậu theo năm
def climate_indices(daily: DataFrame) -> DataFrame:
    w5 = Window.partitionBy("location_id").orderBy("date").rowsBetween(-4, 0)
    d = (daily
         .withColumn("_sum5", F.sum("precip_mm").over(w5))
         .withColumn("_cnt5", F.count("precip_mm").over(w5))
         # chỉ tính Rx5day khi cửa sổ đủ 5 ngày
         .withColumn("rx5", F.when(F.col("_cnt5") == 5, F.col("_sum5"))))
    wet = F.col("precip_mm") >= DRY_MM
    base = d.groupBy(*LOC_COLS, "year").agg(
        F.round(F.avg("t_mean"), 2).alias("annual_mean_temp"),
        F.max("t_max").alias("TXx"),
        F.min("t_min").alias("TNn"),
        F.sum((F.col("t_max") >= 35).cast("int")).alias("SU35"),
        F.sum((F.col("t_max") >= 37).cast("int")).alias("SU37"),
        F.sum((F.col("t_min") < 15).cast("int")).alias("cold_nights_lt15"),
        F.round(F.sum("precip_mm"), 1).alias("annual_precip_mm"),
        F.round(F.sum(F.when(wet, F.col("precip_mm"))), 1).alias("PRCPTOT"),
        F.sum(wet.cast("int")).alias("wet_days"),
        F.sum((F.col("precip_mm") >= 10).cast("int")).alias("R10mm"),
        F.sum((F.col("precip_mm") >= 20).cast("int")).alias("R20mm"),
        F.sum((F.col("precip_mm") > 50).cast("int")).alias("R50mm"),
        F.round(F.max("precip_mm"), 1).alias("Rx1day"),
        F.round(F.max("rx5"), 1).alias("Rx5day"),
    ).withColumn("SDII", F.round(F.col("PRCPTOT") / F.col("wet_days"), 2))

    # CDD, CWD tính trong từng năm (chuỗi qua năm mới bị cắt ở 31/12)
    cdd = spells(daily, F.col("precip_mm") < DRY_MM, by_year=True) \
        .groupBy("location_id", "year").agg(F.max("length_days").alias("CDD"))
    cwd = spells(daily, F.col("precip_mm") >= DRY_MM, by_year=True) \
        .groupBy("location_id", "year").agg(F.max("length_days").alias("CWD"))
    return base.join(cdd, ["location_id", "year"], "left").join(cwd, ["location_id", "year"], "left") \
        .fillna({"CDD": 0, "CWD": 0})


# A2. Đợt nắng nóng + A3. chuỗi khô hạn
def heatwaves(daily: DataFrame):
    names = daily.select(*LOC_COLS).distinct()
    absolute = spells(daily, F.col("t_max") >= HOT_ABS).filter(F.col("length_days") >= MIN_SPELL) \
        .withColumn("kind", F.lit("Tuyệt đối: Tmax ≥ 35 °C"))
    # thêm ngưỡng P90 của từng nơi vì Tmax của ERA5 thường thấp hơn trạm đo
    p90 = daily.groupBy("location_id").agg(F.percentile_approx("t_max", 0.9, 10000).alias("tmax_p90"))
    d = daily.join(F.broadcast(p90), "location_id")
    relative = spells(d, F.col("t_max") > F.col("tmax_p90")).filter(F.col("length_days") >= MIN_SPELL) \
        .withColumn("kind", F.lit("Tương đối: Tmax > P90 địa phương"))
    events = absolute.unionByName(relative).join(names, "location_id").join(p90, "location_id") \
        .withColumn("year", F.year("start_date"))
    summary = events.groupBy(*LOC_COLS, "kind", "tmax_p90").agg(
        F.count("*").alias("n_events"),
        F.sum("length_days").alias("total_days"),
        F.max("length_days").alias("longest_days"),
        F.max("peak_tmax").alias("peak_tmax"),
    )
    return events, summary


def dry_spells(daily: DataFrame, top=3) -> DataFrame:
    s = spells(daily, F.col("precip_mm") < DRY_MM)
    w = Window.partitionBy("location_id").orderBy(F.col("length_days").desc(), "start_date")
    return s.withColumn("rank", F.row_number().over(w)).filter(F.col("rank") <= top) \
        .join(daily.select(*LOC_COLS).distinct(), "location_id")


# A4. Phân bố cấp mưa
def rain_class_distribution(daily: DataFrame) -> DataFrame:
    n_years = daily.groupBy("location_id").agg(F.countDistinct("year").alias("n_years"))
    return (daily.withColumn("rain_class", rain_class(F.col("precip_mm")))
            .groupBy(*LOC_COLS, "rain_class").agg(F.count("*").alias("n_days"))
            .join(n_years, "location_id")
            .withColumn("days_per_year", F.round(F.col("n_days") / F.col("n_years"), 2)))


# A5. Dị thường & xu hướng nhiệt độ (OLS bằng phép tổng hợp)
def ols(df: DataFrame, x: str, y: str, group) -> DataFrame:
    """Hồi quy y = a + b*x cho từng nhóm, chỉ dùng các tổng sum(x), sum(y), sum(xy)...

    b = Sxy / Sxx, se(b) = sqrt(SSE / (n-2) / Sxx), t = b / se(b)
    """
    g = df.groupBy(*group).agg(
        F.count(y).alias("n"), F.sum(x).alias("sx"), F.sum(y).alias("sy"),
        F.sum(F.col(x) * F.col(x)).alias("sxx"), F.sum(F.col(x) * F.col(y)).alias("sxy"),
        F.sum(F.col(y) * F.col(y)).alias("syy"),
    )
    Sxx = F.col("sxx") - F.col("sx") ** 2 / F.col("n")
    Sxy = F.col("sxy") - F.col("sx") * F.col("sy") / F.col("n")
    Syy = F.col("syy") - F.col("sy") ** 2 / F.col("n")
    return (g.withColumn("slope", Sxy / Sxx)
             .withColumn("se", F.sqrt((Syy - F.col("slope") ** 2 * Sxx) / (F.col("n") - 2) / Sxx))
             .withColumn("t_stat", F.col("slope") / F.col("se"))
             .withColumn("r2", (Sxy * Sxy) / (Sxx * Syy))
             .select(*group, "n", "slope", "se", "t_stat", "r2"))


def temperature_trends(q1: DataFrame, ci: DataFrame):
    # dị thường = nhiệt độ tháng - TB của tháng đó qua 10 năm
    clim = q1.groupBy("location_id", "month").agg(F.avg("avg_temp_c").alias("_clim"))
    anom = (q1.join(clim, ["location_id", "month"])
            .withColumn("anomaly_c", F.round(F.col("avg_temp_c") - F.col("_clim"), 3))
            .withColumn("t", F.col("year") + (F.col("month") - 0.5) / 12)
            .drop("_clim"))
    monthly_fit = ols(anom, "t", "anomaly_c", ["location_id"]) \
        .withColumnRenamed("slope", "slope_c_per_year_monthly").withColumnRenamed("se", "se_monthly") \
        .withColumnRenamed("t_stat", "t_monthly").withColumnRenamed("n", "n_months").withColumnRenamed("r2", "r2_monthly")
    annual_fit = ols(ci.withColumn("yr", F.col("year").cast("double")), "yr", "annual_mean_temp", ["location_id"]) \
        .withColumnRenamed("slope", "slope_c_per_year_annual").withColumnRenamed("se", "se_annual") \
        .withColumnRenamed("t_stat", "t_annual").withColumnRenamed("n", "n_years").withColumnRenamed("r2", "r2_annual")
    trends = monthly_fit.join(annual_fit, "location_id").join(q1.select(*LOC_COLS).distinct(), "location_id")

    # dị thường theo năm
    loc_mean = ci.groupBy("location_id").agg(F.avg("annual_mean_temp").alias("_m"))
    annual_anom = ci.join(loc_mean, "location_id") \
        .select(*LOC_COLS, "year", "annual_mean_temp",
                F.round(F.col("annual_mean_temp") - F.col("_m"), 3).alias("anomaly_c"))
    national = annual_anom.groupBy("year").agg(F.round(F.avg("anomaly_c"), 3).alias("anomaly_c"),
                                               F.round(F.avg("annual_mean_temp"), 3).alias("mean_temp_c"))
    return anom, trends, annual_anom, national


def add_p_values(pdf):
    """p-value theo phân phối t, bảng nhỏ nên tính bằng pandas + scipy"""
    from scipy import stats
    pdf["p_monthly"] = 2 * stats.t.sf(pdf["t_monthly"].abs(), pdf["n_months"] - 2)
    pdf["p_annual"] = 2 * stats.t.sf(pdf["t_annual"].abs(), pdf["n_years"] - 2)
    pdf["trend_c_per_decade_monthly"] = (pdf["slope_c_per_year_monthly"] * 10).round(3)
    pdf["trend_c_per_decade_annual"] = (pdf["slope_c_per_year_annual"] * 10).round(3)
    return pdf


# A6. Chu trình ngày đêm
def diurnal_cycle(hourly: DataFrame) -> DataFrame:
    return hourly.groupBy("location_id", "month", "hour").agg(
        F.round(F.avg("temperature_2m"), 2).alias("avg_temp_c"),
        F.round(100 * F.avg((F.col("precipitation") >= 0.1).cast("double")), 2).alias("wet_hour_pct"),
        F.round(F.avg("precipitation"), 3).alias("avg_precip_mm"),
        F.round(F.avg("relative_humidity_2m"), 1).alias("avg_rh"),
    )


# A7. Chu kỳ lặp lại (Gumbel)
def gumbel_return_levels(ci: DataFrame, spark) -> DataFrame:
    """Gumbel theo phương pháp mô-men: x_T = u - a*ln(-ln(1 - 1/T)), a = s*sqrt(6)/pi, u = mean - 0.5772*a"""
    periods = spark.sql(
        "SELECT * FROM VALUES (2), (5), (10), (25), (50), (100) AS periods(return_period_years)"
    )
    m = ci.groupBy(*LOC_COLS).agg(F.avg("Rx1day").alias("mu"), F.stddev_samp("Rx1day").alias("s"),
                                  F.max("Rx1day").alias("observed_max_mm"), F.count("*").alias("n_years"))
    alpha = F.col("s") * math.sqrt(6) / math.pi
    u = F.col("mu") - 0.5772 * alpha
    return m.crossJoin(periods).withColumn(
        "return_level_mm",
        F.round(u - alpha * F.log(-F.log(1 - 1 / F.col("return_period_years"))), 1),
    ).select(*LOC_COLS, "return_period_years", "return_level_mm", "observed_max_mm", "n_years",
             F.round("mu", 1).alias("mean_annual_max_mm"), F.round("s", 1).alias("std_annual_max_mm"))


# A8. Đợt mưa lớn nhiều ngày trên cả nước
def multi_day_rain_events(daily: DataFrame, top=30) -> DataFrame:
    w3 = Window.partitionBy("location_id").orderBy("date").rowsBetween(-2, 0)
    wpk = Window.partitionBy("location_id").orderBy("date").rowsBetween(-3, 3)
    d = (daily.withColumn("rain_3d_mm", F.round(F.sum("precip_mm").over(w3), 1))
              .withColumn("_peak", F.max("rain_3d_mm").over(wpk)))
    # chỉ lấy đỉnh trong khoảng +-3 ngày để 1 đợt mưa không bị đếm nhiều lần
    peaks = d.filter(F.col("rain_3d_mm") == F.col("_peak")).filter(F.col("rain_3d_mm") > 0)
    w = Window.orderBy(F.col("rain_3d_mm").desc())
    return (peaks.withColumn("national_rank", F.row_number().over(w)).filter(F.col("national_rank") <= top)
            .select("national_rank", *LOC_COLS, F.date_sub("date", 2).alias("start_date"),
                    F.col("date").alias("end_date"), "rain_3d_mm"))


# A9. Phân cụm khí hậu (Spark MLlib)
def climate_clusters(q1_clim: DataFrame, q2: DataFrame, daily: DataFrame):
    from pyspark.ml import Pipeline
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator
    from pyspark.ml.feature import PCA, StandardScaler, VectorAssembler
    from pyspark.ml.functions import vector_to_array

    # đặc trưng: 12 tháng nhiệt độ, 12 tháng tỷ lệ mưa, log tổng mưa, biên độ nhiệt
    temp_w = q1_clim.groupBy("location_id").pivot("month", list(range(1, 13))).agg(F.first("clim_temp_c"))
    temp_w = temp_w.select("location_id", *[F.col(str(m)).alias(f"temp_m{m:02d}") for m in range(1, 13)])
    rain_m = q2.groupBy("location_id", "month").agg(F.avg("total_precip_mm").alias("p"))
    rain_tot = rain_m.groupBy("location_id").agg(F.sum("p").alias("annual_p"))
    rain_w = rain_m.join(rain_tot, "location_id").withColumn("share", F.col("p") / F.col("annual_p")) \
        .groupBy("location_id").pivot("month", list(range(1, 13))).agg(F.first("share"))
    rain_w = rain_w.select("location_id", *[F.col(str(m)).alias(f"rain_share_m{m:02d}") for m in range(1, 13)])
    dtr = daily.groupBy("location_id").agg(F.avg(F.col("t_max") - F.col("t_min")).alias("mean_dtr"))
    feats = (temp_w.join(rain_w, "location_id").join(rain_tot, "location_id").join(dtr, "location_id")
             .withColumn("log_annual_p", F.log("annual_p")))
    cols = [c for c in feats.columns if c.startswith(("temp_m", "rain_share_m"))] + ["log_annual_p", "mean_dtr"]

    prep = Pipeline(stages=[
        VectorAssembler(inputCols=cols, outputCol="raw_features"),
        StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True),
    ]).fit(feats)
    X = prep.transform(feats).cache()

    evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
    scores = []
    for k in range(2, 8):
        model = KMeans(k=k, seed=42, featuresCol="features", maxIter=100).fit(X)
        scores.append({"k": k, "silhouette": evaluator.evaluate(model.transform(X)),
                       "wssse": model.summary.trainingCost})
    best_k = max(scores, key=lambda s: s["silhouette"])["k"]
    best = KMeans(k=best_k, seed=42, featuresCol="features", maxIter=100).fit(X)
    pca = PCA(k=2, inputCol="features", outputCol="pc").fit(X)
    out = pca.transform(best.transform(X)).withColumn("pc_arr", vector_to_array("pc"))
    result = out.select("location_id", F.col("prediction").alias("cluster"),
                        F.round(F.col("pc_arr")[0], 3).alias("pc1"), F.round(F.col("pc_arr")[1], 3).alias("pc2"),
                        F.round("annual_p", 0).alias("annual_precip_mm"), F.round("mean_dtr", 2).alias("mean_dtr"),
                        *[F.round(c, 2).alias(c) for c in cols if c.startswith("temp_m")])
    info = {"k_scores": scores, "best_k": best_k,
            "pca_explained_variance": [float(v) for v in pca.explainedVariance.toArray()],
            "features": cols}
    return result, info


def main():
    t0 = time.time()
    spark = get_spark("advanced")
    R = C.RESULTS_DIR
    hourly = spark.read.parquet(str(C.CLEAN_DIR / "hourly"))
    daily = spark.read.parquet(str(C.CLEAN_DIR / "daily")).cache()
    q1 = spark.read.parquet(str(R / "q1_monthly_temp_full"))
    q1_clim = spark.read.parquet(str(R / "q1_climatology_full"))
    q2 = spark.read.parquet(str(R / "q2_monthly_precip_full"))

    ci = climate_indices(daily).cache()
    save(ci, "a1_climate_indices", ["location_id", "year"])

    hw_events, hw_summary = heatwaves(daily)
    save(hw_events, "a2_heatwave_events", ["kind", "location_id", "start_date"])
    save(hw_summary, "a2_heatwave_summary", ["kind", "location_id"])
    save(dry_spells(daily), "a3_dry_spells_top3", ["location_id", "rank"])
    save(rain_class_distribution(daily), "a4_rain_class_distribution", ["location_id", "rain_class"])

    anom, trends, annual_anom, national = temperature_trends(q1, ci)
    save(anom.select(*LOC_COLS, "year", "month", "avg_temp_c", "anomaly_c"), "a5_monthly_anomaly",
         ["location_id", "year", "month"])
    save(spark.createDataFrame(add_p_values(trends.toPandas())), "a5_temperature_trends", ["location_id"])
    save(annual_anom, "a5_annual_anomaly", ["location_id", "year"])
    save(national, "a5_national_annual_anomaly", ["year"])

    save(diurnal_cycle(hourly), "a6_diurnal_cycle", ["location_id", "month", "hour"])
    save(gumbel_return_levels(ci, spark), "a7_return_levels", ["location_id", "return_period_years"])
    save(multi_day_rain_events(daily), "a8_multiday_rain_events", ["national_rank"])

    clusters, info = climate_clusters(q1_clim, q2, daily)
    save(clusters.join(daily.select(*LOC_COLS).distinct(), "location_id"), "a9_climate_clusters", ["cluster", "location_id"])
    (R / "a9_cluster_info.json").write_text(json.dumps(info, indent=2, ensure_ascii=False))
    print(json.dumps(info["k_scores"], indent=1), "best_k =", info["best_k"])
    print(f"advanced done in {time.time() - t0:.1f}s")
    spark.stop()


if __name__ == "__main__":
    main()
