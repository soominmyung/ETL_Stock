from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import os
import re
import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, date_format, to_date


def create_spark(
    app_name: str = "ETLApp",
    driver_memory: str = "4g",
    executor_memory: str = "4g",
    shuffle_partitions: int = 8,
    extra_confs: Optional[Dict[str, str]] = None,
) -> SparkSession:
    """Create and return a configured SparkSession."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    )
    if extra_confs:
        for k, v in extra_confs.items():
            builder = builder.config(k, v)
    spark = builder.getOrCreate()
    return spark


def ensure_dir(path: Path | str) -> Path:
    """Create a directory if it does not exist and return it as Path."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def log(message: str) -> None:
    """Lightweight logger with consistent prefix."""
    print(f"[ETL] {message}")


class timeit:
    """Simple context manager to measure elapsed time.
    Usage:
        with timeit('step name'):
            ...
    """
    def __init__(self, label: str) -> None:
        self.label = label
        self.start = 0.0

    def __enter__(self):
        self.start = time.time()
        log(f"start {self.label}")
        return self

    def __exit__(self, exc_type, exc, tb):
        elapsed = time.time() - self.start
        log(f"end {self.label} in {elapsed:.2f}s")


def list_year_inputs(input_dir: Path | str = "data") -> List[Tuple[int, Path]]:
    """Discover yearly Excel inputs like 2019.xlsx ... 2025.xlsx."""
    root = Path(input_dir)
    items: List[Tuple[int, Path]] = []
    for f in root.glob("*.xlsx"):
        m = re.fullmatch(r"(\d{4})\.xlsx", f.name)
        if m:
            items.append((int(m.group(1)), f))
    items.sort(key=lambda x: x[0])
    return items


def list_year_parquets(output_dir: Path | str = "output", prefix: str = "cleaned_stock_parquet_") -> List[Path]:
    """List yearly parquet folders like output/cleaned_stock_parquet_2019."""
    root = Path(output_dir)
    return sorted(root.glob(f"{prefix}*"))


def write_parquet(df: DataFrame, path: Path | str, mode: str = "overwrite") -> None:
    """Write a DataFrame to Parquet."""
    df.write.mode(mode).parquet(str(path))
    log(f"written parquet to {path}")


def deduplicate_by_keys(
    df: DataFrame,
    keys: Iterable[str] = ("ItemCode", "WhsCode", "RecordDate"),
    order_col: str = "OnHand",
    order_desc: bool = True,
) -> DataFrame:
    """Keep a single row per key group using row_number over a window."""
    order_by = col(order_col).desc() if order_desc else col(order_col).asc()
    w = Window.partitionBy(*list(keys)).orderBy(order_by)
    return df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")


def format_date_col(
    df: DataFrame,
    column: str = "RecordDate",
    input_fmt: str = "yyyy-MMM-dd",
    output_fmt: str = "yyyy-MM-dd",
) -> DataFrame:
    """Convert a date-like string column to desired format."""
    return df.withColumn(column, date_format(to_date(column, input_fmt), output_fmt))


def load_db_config_from_env(
    url_var: str = "MSSQL_URL",
    user_var: str = "MSSQL_USER",
    password_var: str = "MSSQL_PASSWORD",
    table_var: str = "MSSQL_TABLE",
) -> Dict[str, str]:
    """Read JDBC configuration from environment variables."""
    env = os.environ
    missing = [v for v in (url_var, user_var, password_var) if not env.get(v)]
    if missing:
        raise KeyError(f"missing env vars: {', '.join(missing)}")
    return {
        "url": env[url_var],
        "user": env[user_var],
        "password": env[password_var],
        "table": env.get(table_var, "dbo.z_Stock_History_2"),
    }
