"""Tạo SparkSession dùng chung cho các bước.

Chạy local mode, mặc định 8 luồng vì server dùng chung (đổi bằng biến SPARK_THREADS).
"""
import os
import hashlib
import sys
import tempfile
from urllib.request import urlopen
from pathlib import Path

# để chạy được python pipeline/xxx.py
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import config as C

# dùng python của venv cho cả driver và worker
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

_WINDOWS_FS_JAR_URL = (
    "https://repo.maven.apache.org/maven2/com/globalmentor/"
    "hadoop-bare-naked-local-fs/0.1.0/"
    "hadoop-bare-naked-local-fs-0.1.0.jar"
)
_WINDOWS_FS_JAR_SHA256 = "e0cc30fb0531eb0b59468dc0abf5b257533d2365b5e9f45e795edd707aa78c62"
_WINDOWS_FS_CLASS = "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"


def _windows_local_fs_jar() -> Path:
    """Get the checksum-pinned Java NIO filesystem adapter used instead of winutils."""
    jar = Path.home() / ".cache" / "bigdata2" / "jars" / "hadoop-bare-naked-local-fs-0.1.0.jar"
    if jar.is_file():
        digest = hashlib.sha256(jar.read_bytes()).hexdigest()
        if digest != _WINDOWS_FS_JAR_SHA256:
            raise RuntimeError(f"Unexpected checksum for cached Hadoop filesystem adapter: {jar}")
        return jar

    with urlopen(_WINDOWS_FS_JAR_URL, timeout=30) as response:
        contents = response.read(1_000_001)
    if len(contents) > 1_000_000:
        raise RuntimeError("Hadoop filesystem adapter download exceeded the expected size limit")
    if hashlib.sha256(contents).hexdigest() != _WINDOWS_FS_JAR_SHA256:
        raise RuntimeError("Hadoop filesystem adapter checksum verification failed")

    jar.parent.mkdir(parents=True, exist_ok=True)
    temporary = jar.with_name(f"{jar.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(contents)
        temporary.replace(jar)
    finally:
        if temporary.exists():
            temporary.unlink()
    return jar


def local_work_dir(name):
    """Return a per-user Spark work directory on the current platform."""
    user = os.environ.get("USER") or os.environ.get("USERNAME") or "user"
    shm = Path("/dev/shm")
    base = shm if os.name == "posix" and shm.is_dir() else Path(tempfile.gettempdir())
    return base / f"{user}_{name}"


def get_spark(app_name="weather-bigdata", threads=None, shuffle_partitions=None):
    from pyspark.sql import SparkSession

    threads = int(threads or os.environ.get("SPARK_THREADS", 8))
    local_dir = local_work_dir("spark_weather")
    local_dir.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(f"local[{threads}]")
        .config("spark.pyspark.python", os.environ["PYSPARK_PYTHON"])
        .config("spark.pyspark.driver.python", os.environ["PYSPARK_DRIVER_PYTHON"])
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "8g"))
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions or threads * 4))
        # đặt giờ VN để thời gian từ API được hiểu là giờ địa phương
        .config("spark.sql.session.timeZone", C.TIMEZONE)
        .config("spark.local.dir", str(local_dir))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        # chạy lại thì chỉ ghi đè partition bị thay đổi
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if os.name == "nt":
        fs_jar = _windows_local_fs_jar()
        classpath = os.environ.get("SPARK_CLASSPATH")
        classpath_entries = classpath.split(os.pathsep) if classpath else []
        if str(fs_jar) not in classpath_entries:
            classpath_entries.insert(0, str(fs_jar))
        os.environ["SPARK_CLASSPATH"] = os.pathsep.join(classpath_entries)
        builder = (
            builder
            .config("spark.driver.extraClassPath", str(fs_jar))
            .config("spark.executor.extraClassPath", str(fs_jar))
            .config("spark.hadoop.fs.file.impl", _WINDOWS_FS_CLASS)
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
