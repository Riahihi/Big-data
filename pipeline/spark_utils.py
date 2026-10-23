"""Tạo SparkSession dùng chung cho các bước.

Chạy local mode, mặc định 8 luồng vì server dùng chung (đổi bằng biến SPARK_THREADS).
"""
import os
import sys
from pathlib import Path

# để chạy được python pipeline/xxx.py
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import config as C

# dùng python của venv cho cả driver và worker
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def get_spark(app_name="weather-bigdata", threads=None, shuffle_partitions=None):
    from pyspark.sql import SparkSession

    threads = int(threads or os.environ.get("SPARK_THREADS", 8))
    # thư mục tạm để trên /dev/shm cho nhanh
    local_dir = Path("/dev/shm") / f"{os.environ.get('USER', 'user')}_spark_weather"
    local_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName(app_name)
        .master(f"local[{threads}]")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "8g"))
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions or threads * 4))
        # đặt giờ VN để thời gian từ API được hiểu là giờ địa phương
        .config("spark.sql.session.timeZone", C.TIMEZONE)
        .config("spark.local.dir", str(local_dir))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        # chạy lại thì chỉ ghi đè partition bị thay đổi
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
