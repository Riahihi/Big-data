"""Bước 4: làm sạch dữ liệu bằng PySpark rồi ghi ra Parquet

Các quy tắc nhóm thống nhất:
  R1 - chuẩn hóa kiểu dữ liệu, thời gian theo giờ VN
  R2 - bỏ bản ghi trùng (location_id, ts)
  R3 - tạo lưới đủ 24h/ngày để phát hiện giờ bị thiếu
  R4 - giá trị ngoài ngưỡng thì gán null
  R5 - thiếu <= 3 giờ thì nội suy tuyến tính (chỉ biến liên tục, không nội suy mưa)
  R6 - mưa lúc H là mưa của giờ trước đó nên ngày mưa = ngày của (ts - 1h)

Chạy: python pipeline/clean.py
"""
import calendar
import glob
import json
import sys
import time
from contextlib import redirect_stdout
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline import config as C
from pipeline.spark_utils import get_spark

# ngưỡng hợp lệ, để rộng hơn kỷ lục thực tế, chỉ nhằm bắt giá trị lỗi
VALID_RANGES = {
    "temperature_2m": (-15.0, 50.0),
    "dew_point_2m": (-30.0, 40.0),
    "apparent_temperature": (-25.0, 65.0),
    "relative_humidity_2m": (0.0, 100.0),
    "precipitation": (0.0, 500.0),
    "rain": (0.0, 500.0),
    "cloud_cover": (0.0, 100.0),
    "wind_speed_10m": (0.0, 300.0),
    "wind_gusts_10m": (0.0, 400.0),
    "weather_code": (0.0, 99.0),
}
# các biến được nội suy, tối đa 3 giờ
INTERPOLATE_VARS = ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature"]
MAX_GAP_HOURS = 3


def raw_schema():
    """Khai báo schema trước, không để spark tự đoán"""
    hourly = T.StructType(
        [T.StructField("time", T.ArrayType(T.StringType()))]
        + [T.StructField(v, T.ArrayType(T.DoubleType())) for v in C.HOURLY_VARS]
    )
    return T.StructType([
        T.StructField("latitude", T.DoubleType()),
        T.StructField("longitude", T.DoubleType()),
        T.StructField("elevation", T.DoubleType()),
        T.StructField("utc_offset_seconds", T.IntegerType()),
        T.StructField("timezone", T.StringType()),
        T.StructField("hourly", hourly),
    ])


def read_raw(spark, raw_glob=None) -> DataFrame:
    """Đọc file json gốc. Mỗi file là các mảng theo giờ nên phải explode ra từng dòng"""
    if raw_glob is None:
        raw_paths = [str(path) for path in sorted(C.RAW_DIR.glob("*/*.json.gz"))]
    elif isinstance(raw_glob, (str, Path)):
        raw_paths = sorted(glob.glob(str(raw_glob))) if glob.has_magic(str(raw_glob)) else [str(raw_glob)]
    else:
        raw_paths = [str(path) for path in raw_glob]
    if not raw_paths:
        raise FileNotFoundError(f"Không tìm thấy file JSON gốc phù hợp với {raw_glob or C.RAW_DIR}")
    df = (
        spark.read.schema(raw_schema()).option("multiLine", True).json(raw_paths)
        .withColumn("source_file", F.input_file_name())
    )
    # lấy mã địa điểm, năm từ tên file (vd HNI_2019.json.gz)
    df = (
        df.withColumn("location_id", F.regexp_extract("source_file", r"/([A-Z]{3})_(\d{4})\.json\.gz$", 1))
          .withColumn("file_year", F.regexp_extract("source_file", r"_(\d{4})\.json\.gz$", 1).cast("int"))
    )
    meta = ["location_id", "file_year", "source_file", "grid_latitude", "grid_longitude",
            "elevation", "utc_offset_seconds"]
    # zip các mảng lại rồi explode 1 lần
    arrays = df.select(
        *[c for c in meta if c not in ("grid_latitude", "grid_longitude")],
        F.col("latitude").alias("grid_latitude"), F.col("longitude").alias("grid_longitude"),
        F.col("hourly.time").alias("time_str"),
        *[F.col(f"hourly.{v}").alias(v) for v in C.HOURLY_VARS],
    )
    zipped = arrays.select(*meta, F.explode(F.arrays_zip("time_str", *C.HOURLY_VARS)).alias("r"))
    return zipped.select(*meta, "r.time_str", *[f"r.{v}" for v in C.HOURLY_VARS])


def standardize(df: DataFrame) -> DataFrame:
    """R1: đổi kiểu dữ liệu, parse thời gian"""
    out = df.withColumn("ts", F.to_timestamp("time_str", "yyyy-MM-dd'T'HH:mm"))
    for v in C.HOURLY_VARS:
        out = out.withColumn(v, F.col(v).cast("double"))
    return out.drop("time_str")


def deduplicate(df: DataFrame):
    """R2: bỏ dòng trùng, trả về (df, số dòng bị bỏ)"""
    w = Window.partitionBy("location_id", "ts").orderBy(F.col("source_file").desc())
    ranked = df.withColumn("_rn", F.row_number().over(w))
    n_dup = ranked.filter("_rn > 1").count()
    return ranked.filter("_rn = 1").drop("_rn"), n_dup


def hourly_grid(loc_years: DataFrame) -> DataFrame:
    """R3: tạo đủ các giờ của từng năm để biết giờ nào thiếu"""
    return loc_years.select(
        "location_id",
        F.explode(F.sequence(
            F.to_timestamp(F.format_string("%d-01-01 00:00", "year"), "yyyy-MM-dd HH:mm"),
            F.to_timestamp(F.format_string("%d-12-31 23:00", "year"), "yyyy-MM-dd HH:mm"),
            F.expr("interval 1 hour"),
        )).alias("ts"),
    )


def apply_valid_ranges(df: DataFrame) -> DataFrame:
    """R4: ngoài ngưỡng thì gán null"""
    for v, (lo, hi) in VALID_RANGES.items():
        bad = F.col(v).isNotNull() & ((F.col(v) < lo) | (F.col(v) > hi))
        df = df.withColumn(f"_oor_{v}", bad).withColumn(v, F.when(bad, None).otherwise(F.col(v)))
    return df


def interpolate_short_gaps(df: DataFrame, cols=INTERPOLATE_VARS, max_gap=MAX_GAP_HOURS) -> DataFrame:
    """R5: nội suy tuyến tính khi khoảng trống <= max_gap giờ"""
    # lưu ý: dùng unboundedFollowing rất chậm (spark tính lại cho từng dòng) nên giá trị phía sau
    # được tìm bằng cách sort ngược ts desc
    w_prev = Window.partitionBy("location_id").orderBy(F.col("ts").asc()).rowsBetween(Window.unboundedPreceding, 0)
    w_next = Window.partitionBy("location_id").orderBy(F.col("ts").desc()).rowsBetween(Window.unboundedPreceding, 0)
    sec = F.col("ts").cast("long")
    imputed_any = F.lit(False)
    for c in cols:
        prev_v = F.last(F.col(c), ignorenulls=True).over(w_prev)
        prev_t = F.last(F.when(F.col(c).isNotNull(), sec), ignorenulls=True).over(w_prev)
        next_v = F.last(F.col(c), ignorenulls=True).over(w_next)
        next_t = F.last(F.when(F.col(c).isNotNull(), sec), ignorenulls=True).over(w_next)
        df = df.withColumn(f"_pv", prev_v).withColumn("_pt", prev_t).withColumn("_nv", next_v).withColumn("_nt", next_t)
        can = F.col(c).isNull() & F.col("_pt").isNotNull() & F.col("_nt").isNotNull() & \
            ((F.col("_nt") - F.col("_pt")) <= (max_gap + 1) * 3600)
        interp = F.col("_pv") + (F.col("_nv") - F.col("_pv")) * (sec - F.col("_pt")) / (F.col("_nt") - F.col("_pt"))
        df = df.withColumn(f"_imp_{c}", can).withColumn(c, F.when(can, F.round(interp, 2)).otherwise(F.col(c)))
        imputed_any = imputed_any | F.col(f"_imp_{c}")
        df = df.drop("_pv", "_pt", "_nv", "_nt")
    return df.withColumn("temp_imputed", imputed_any)


def location_dim(spark) -> DataFrame:
    def sql_literal(value):
        if isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        return str(value)

    values = ", ".join(
        "(" + ", ".join(sql_literal(value) for value in row) + ")"
        for row in C.LOCATIONS
    )
    columns = ", ".join(C.LOCATION_FIELDS)
    return spark.sql(f"SELECT * FROM VALUES {values} AS locations({columns})")


def validate_location_keys(raw_ids, dimension_rows=None):
    """Fail before the dimension join can drop or multiply fact rows."""
    rows = C.LOCATIONS if dimension_rows is None else dimension_rows
    dimension_ids = [row[0] for row in rows]
    duplicates = sorted({location_id for location_id in dimension_ids
                         if dimension_ids.count(location_id) > 1})
    if duplicates:
        raise ValueError(f"location dimension has duplicate keys: {duplicates}")

    if None in raw_ids:
        raise ValueError("raw input contains a null location_id")
    unmatched = sorted(set(raw_ids) - set(dimension_ids))
    if unmatched:
        raise ValueError(f"raw locations missing from location dimension: {unmatched}")
    return len(dimension_ids)


def validate_join_quality(stats):
    failures = []
    if stats["rows_outside_grid"]:
        failures.append(f"{stats['rows_outside_grid']} rows fall outside the hourly grid")
    if stats["rows_unmatched_location_dimension"]:
        failures.append(f"{stats['rows_unmatched_location_dimension']} rows lack a location dimension match")
    if stats["grid_join_row_delta"]:
        failures.append(f"grid join changed row count by {stats['grid_join_row_delta']}")
    if failures:
        raise ValueError("join quality gate failed: " + "; ".join(failures))


def build_clean(spark, raw_glob=None):
    """Chạy R1-R5, trả về dữ liệu sạch và bảng chất lượng"""
    stats = {}
    raw = standardize(read_raw(spark, raw_glob)).cache()
    stats["raw_rows"] = raw.count()

    dedup, stats["duplicates_removed"] = deduplicate(raw)

    # tọa độ ô lưới ERA5 và độ cao lấy từ response của API
    grid_meta = dedup.groupBy("location_id").agg(
        F.first("grid_latitude").alias("grid_latitude"),
        F.first("grid_longitude").alias("grid_longitude"),
        F.first("elevation").alias("elevation_m"),
    )
    loc_years = dedup.select("location_id", F.col("file_year").alias("year")).distinct()
    loc_year_rows = loc_years.collect()
    if not loc_year_rows:
        raise ValueError("No location/year records were found in the raw input")
    if any(row["year"] is None for row in loc_year_rows):
        raise ValueError("raw input contains an invalid year in its source filename")
    raw_ids = {row["location_id"] for row in loc_year_rows}
    stats["location_dimension_keys"] = validate_location_keys(raw_ids)
    stats["rows_after_deduplicate"] = stats["raw_rows"] - stats["duplicates_removed"]
    stats["rows_before_grid_join"] = stats["rows_after_deduplicate"]
    stats["expected_grid_rows"] = sum(
        (366 if calendar.isleap(row["year"]) else 365) * 24 for row in loc_year_rows
    )
    stats["rows_unmatched_location_dimension"] = 0
    grid = hourly_grid(loc_years)

    # left join từ lưới giờ, giờ nào không có dữ liệu thì is_missing_row = true
    joined = grid.join(dedup.drop("file_year", "grid_latitude", "grid_longitude", "elevation"),
                       ["location_id", "ts"], "left") \
                 .withColumn("is_missing_row", F.col("source_file").isNull())
    # đếm số dòng nằm ngoài lưới (nếu có)
    outside_grid = (
        F.col("ts").isNull()
        | F.col("file_year").isNull()
        | (F.year("ts") != F.col("file_year"))
        | (F.minute("ts") != 0)
        | (F.second("ts") != 0)
    )
    stats["rows_outside_grid"] = dedup.filter(outside_grid).count()
    if stats["rows_outside_grid"]:
        raise ValueError(f"{stats['rows_outside_grid']} raw rows fall outside the hourly grid")
    stats["rows_matched_to_grid"] = stats["rows_after_deduplicate"] - stats["rows_outside_grid"]

    checked = apply_valid_ranges(joined)
    filled = interpolate_short_gaps(checked)

    clean = (
        filled
        .withColumn("weather_code", F.col("weather_code").cast("int"))
        .withColumn("date", F.to_date("ts"))
        # mưa lúc H là mưa của giờ trước đó, vd giá trị 00:00 ngày 2/1 là mưa 23h-24h ngày 1/1
        .withColumn("precip_date", F.to_date(F.col("ts") - F.expr("INTERVAL 1 HOUR")))
        .withColumn("year", F.year("ts"))
        .withColumn("month", F.month("ts"))
        .withColumn("hour", F.hour("ts"))
        .join(F.broadcast(location_dim(spark)), "location_id")
        .join(F.broadcast(grid_meta), "location_id", "left")
    )

    # bảng chất lượng theo địa điểm, năm
    agg = [
        F.count("*").alias("expected_hours"),
        F.sum(F.col("is_missing_row").cast("int")).alias("missing_rows"),
        F.sum(F.col("temp_imputed").cast("int")).alias("rows_imputed"),
    ]
    for v in C.HOURLY_VARS:
        agg.append(F.sum(F.col(f"_oor_{v}").cast("int")).alias(f"out_of_range_{v}"))
        agg.append(F.sum(F.col(v).isNull().cast("int")).alias(f"null_after_clean_{v}"))
    quality = clean.groupBy("location_id", "location_name", "year").agg(*agg).orderBy("location_id", "year")

    keep = ["location_id", "location_name", "site_name", "region", "is_core", "latitude", "longitude",
            "grid_latitude", "grid_longitude", "elevation_m", "ts", "date", "precip_date", "year", "month", "hour",
            *C.HOURLY_VARS, "is_missing_row", "temp_imputed"]
    return clean, quality, keep, stats, raw


def main():
    t0 = time.time()
    spark = get_spark("clean")
    clean, quality, keep, stats, raw = build_clean(spark)

    q = quality.toPandas()
    stats["rows_after_dimension_join"] = int(q["expected_hours"].sum())
    stats["grid_join_row_delta"] = stats["rows_after_dimension_join"] - stats["expected_grid_rows"]
    validate_join_quality(stats)

    C.REPORTS.joinpath("evidence").mkdir(parents=True, exist_ok=True)
    stats["input_schema"] = raw.schema.jsonValue()
    stats["output_schema"] = clean.select(*keep).schema.jsonValue()
    with C.REPORTS.joinpath("evidence", "clean_plan.txt").open("w", encoding="utf-8") as plan:
        with redirect_stdout(plan):
            clean.select(*keep).explain("formatted")

    out = C.CLEAN_DIR / "hourly"
    # ghi parquet, chia partition theo địa điểm và năm
    (clean.select(*keep).repartition("location_id", "year")
          .write.mode("overwrite").partitionBy("location_id", "year").parquet(str(out)))

    C.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    quality.write.mode("overwrite").parquet(str(C.RESULTS_DIR / "quality_report"))
    q.to_csv(C.RESULTS_DIR / "quality_report.csv", index=False, encoding="utf-8-sig")

    # đọc lại để kiểm tra số dòng
    back = spark.read.parquet(str(out))
    stats["clean_rows"] = back.count()
    stats["expected_rows"] = int(q["expected_hours"].sum())
    stats["missing_rows"] = int(q["missing_rows"].sum())
    stats["rows_imputed"] = int(q["rows_imputed"].sum())
    stats["out_of_range_total"] = int(q[[c for c in q.columns if c.startswith("out_of_range_")]].sum().sum())
    stats["null_temperature_after_clean"] = int(q["null_after_clean_temperature_2m"].sum())
    stats["null_precipitation_after_clean"] = int(q["null_after_clean_precipitation"].sum())
    stats["n_locations"] = back.select("location_id").distinct().count()
    stats["parquet_roundtrip_ok"] = stats["clean_rows"] == stats["expected_rows"]
    stats["elapsed_s"] = round(time.time() - t0, 1)
    if not stats["parquet_roundtrip_ok"]:
        raise ValueError(
            f"Parquet row count mismatch: wrote {stats['clean_rows']}, expected {stats['expected_rows']}"
        )
    (C.RESULTS_DIR / "clean_stats.json").write_text(
        json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(json.dumps(stats, indent=2, ensure_ascii=False))
    raw.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
