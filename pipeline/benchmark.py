"""Phần mức B: đo dung lượng và thời gian chạy

B1 dung lượng json / csv / parquet
B2 đọc csv và parquet rồi tính câu 1
B3 có phân vùng và không phân vùng
B4 chạy spark với 1, 2, 4, 8 luồng
B5 so với pandas

Mỗi phép đo chạy 3 lần lấy trung vị. Máy dùng chung nên có ghi lại load average lúc đo.

Chạy: python pipeline/benchmark.py
"""
import json
import os
import platform
import shutil
import statistics
import sys
import time
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import functions as F

from pipeline import config as C
from pipeline.spark_utils import get_spark

REPEAT = 3
TMP = Path("/dev/shm") / f"{os.environ.get('USER', 'user')}_weather_bench"  # thư mục tạm riêng của benchmark


def dir_size(p: Path) -> int:
    return sum(f.stat().st_size for f in Path(p).rglob("*") if f.is_file())


def timed(fn, repeat=REPEAT):
    """Chạy fn vài lần, trả về (trung vị, các lần đo)"""
    runs = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        runs.append(time.perf_counter() - t0)
    return round(statistics.median(runs), 3), [round(r, 3) for r in runs]


def q1_job(df):
    return df.groupBy("location_id", "year", "month").agg(F.avg("temperature_2m")).collect()


def environment(spark):
    return {
        "cpu": platform.processor() or platform.machine(),
        "logical_cores_machine": os.cpu_count(),
        "load_average_1m_5m_15m": [round(x, 1) for x in os.getloadavg()],
        "spark_version": spark.version,
        "spark_master": spark.sparkContext.master,
        "python": platform.python_version(),
        "note": "Máy chủ dùng chung nhiều người; số liệu chỉ có giá trị so sánh tương đối trong cùng lần chạy.",
    }


def main():
    if TMP.exists():
        shutil.rmtree(TMP)
    TMP.mkdir(parents=True)
    res = {}
    spark = get_spark("benchmark")
    res["environment"] = environment(spark)
    hourly_path = C.CLEAN_DIR / "hourly"
    hourly = spark.read.parquet(str(hourly_path))
    n_rows = hourly.count()
    res["n_rows"] = n_rows

    # B1: dung lượng
    csv_path, flat_path = TMP / "hourly_csv", TMP / "hourly_parquet_flat"
    hourly.write.mode("overwrite").option("header", True).csv(str(csv_path))
    hourly.write.mode("overwrite").parquet(str(flat_path))  # Parquet KHÔNG phân vùng
    manifest = [json.loads(l) for l in C.MANIFEST.read_text().splitlines() if l.strip()]
    raw_gz = sum(m["bytes_gz"] for m in manifest)
    raw_json = sum(m["bytes_raw"] for m in manifest)
    sizes = {
        "raw_json_uncompressed": raw_json, "raw_json_gzip": raw_gz,
        "csv_uncompressed": dir_size(csv_path),
        "parquet_snappy_partitioned": dir_size(hourly_path),
        "parquet_snappy_flat": dir_size(flat_path),
    }
    res["B1_storage_bytes"] = sizes
    res["B1_storage_MB"] = {k: round(v / 1e6, 1) for k, v in sizes.items()}
    res["B1_csv_over_parquet_ratio"] = round(sizes["csv_uncompressed"] / sizes["parquet_snappy_flat"], 2)

    # B2: csv và parquet
    csv_df = lambda: spark.read.option("header", True).option("inferSchema", False).csv(str(csv_path)) \
        .withColumn("temperature_2m", F.col("temperature_2m").cast("double")) \
        .withColumn("year", F.col("year").cast("int")).withColumn("month", F.col("month").cast("int"))
    pq_df = lambda: spark.read.parquet(str(flat_path))
    q1_job(pq_df())  # làm nóng JVM trước khi đo
    res["B2_q1_seconds"] = {
        "csv": timed(lambda: q1_job(csv_df())),
        "parquet": timed(lambda: q1_job(pq_df())),
    }

    # B3: phân vùng
    def one_slice(path):
        d = spark.read.parquet(str(path)).filter((F.col("location_id") == "HNI") & (F.col("year") == 2024))
        return d.agg(F.avg("temperature_2m"), F.sum("precipitation")).collect()

    def files_read(path):
        """Đếm số file có dữ liệu thỏa điều kiện (là số file spark phải đọc)"""
        d = spark.read.parquet(str(path)).filter((F.col("location_id") == "HNI") & (F.col("year") == 2024))
        files = [r[0] for r in d.select(F.input_file_name()).distinct().collect()]
        size = sum(Path(f.replace("file://", "")).stat().st_size for f in files)
        return len(files), size

    # thêm 1 bản không phân vùng và bị xáo trộn, dữ liệu 1 địa điểm nằm rải ở mọi file
    shuffled_path = TMP / "hourly_parquet_unpartitioned_shuffled"
    hourly.repartition(16).write.mode("overwrite").parquet(str(shuffled_path))
    b3 = {"query": "location_id = 'HNI' AND year = 2024"}
    for key, path in [("partitioned", hourly_path), ("unpartitioned_shuffled", shuffled_path),
                      ("unpartitioned_sorted", flat_path)]:
        n, size = files_read(path)
        b3[key] = {"seconds": timed(lambda: one_slice(path)), "files_read": n,
                   "files_total": len(list(Path(path).rglob("*.parquet"))),
                   "mb_in_files_read": round(size / 1e6, 2), "mb_total": round(dir_size(path) / 1e6, 2)}
    res["B3_partition_pruning"] = b3
    spark.stop()

    # B4: số luồng
    from pipeline.analysis import build_daily
    scaling = {}
    for threads in (1, 2, 4, 8):
        s = get_spark(f"bench-{threads}", threads=threads, shuffle_partitions=max(4, threads * 4))
        h = s.read.parquet(str(hourly_path))
        build_daily(h).count()  # làm nóng
        scaling[threads] = timed(lambda: build_daily(s.read.parquet(str(hourly_path))).agg(F.sum("precip_mm")).collect())
        s.stop()
    base = scaling[1][0]
    res["B4_thread_scaling_daily_agg"] = {str(k): {"seconds": v, "speedup_vs_1": round(base / v[0], 2)}
                                          for k, v in scaling.items()}

    # B5: pandas và spark
    import pandas as pd
    cols = ["location_id", "date", "temperature_2m", "precipitation"]

    def pandas_daily():
        df = pd.read_parquet(flat_path, columns=cols)
        return df.groupby(["location_id", "date"]).agg(t=("temperature_2m", "mean"), p=("precipitation", "sum"))

    s = get_spark("bench-pandas-vs-spark", threads=8)
    spark_daily = lambda: s.read.parquet(str(flat_path)).groupBy("location_id", "date") \
        .agg(F.avg("temperature_2m"), F.sum("precipitation")).count()
    spark_daily()
    t_pd, t_sp = timed(pandas_daily), timed(spark_daily)
    ratio = t_pd[0] / t_sp[0]
    # ghi chú tạo từ số đo thực tế
    res["B5_daily_agg_seconds"] = {
        "pandas_single_process": t_pd, "spark_local8": t_sp, "ratio_pandas_over_spark": round(ratio, 2),
        "note": f"Spark {'nhanh' if ratio > 1 else 'chậm'} hơn pandas khoảng {max(ratio, 1 / ratio):.1f} lần".replace(".", ",")
                + " (chưa tính thời gian khởi động Spark, khoảng 5-10 s).",
    }
    s.stop()

    res["environment"]["load_average_end"] = [round(x, 1) for x in os.getloadavg()]
    shutil.rmtree(TMP)  # chỉ xóa thư mục tạm do chính script tạo
    (C.RESULTS_DIR / "benchmark.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
