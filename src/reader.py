"""
reader.py  ETL pipeline for yearly stock Excel files

Usage:
    python reader.py --year 2025 --abs_jump 500 --rel_jump 5.0

Requirements:
    pandas, pyarrow, pyspark

Arguments:
    --year       Year of the Excel file to process
    --abs_jump   Absolute day on day change threshold for outlier detection
                 Example: 500 means any change >= 500 units vs the previous valid day is flagged
    --rel_jump   Relative day on day change threshold for outlier detection
                 Example: 5.0 means any change >= 5x vs the previous valid day is flagged

Process overview:
    1) Use pandas to read the Excel and normalise headers and columns so the structure is consistent
    2) Write the normalised result to a Parquet staging file to fix the schema up front
    3) Use PySpark to read the staged Parquet and perform unpivot, type casting, filtering, deduplication,
       and gap aware outlier and null handling using the nearest previous or next valid value
       No new rows are created for missing weekdays
    4) Write the curated result to Parquet
"""

import argparse
import re
from functools import reduce
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import when, lit, col, row_number, date_format, to_date
from pyspark.sql.window import Window

from utils import create_spark, log


def run(year: int, abs_jump: float, rel_jump: float) -> None:
    project_root = Path(__file__).resolve().parents[1]
    data_dir = project_root / "data"
    output_dir = project_root / "output"

    excel_path = data_dir / f"{year}.xlsx"
    parquet_stage_path = data_dir / f"{year}_stage.parquet"
    parquet_path = output_dir / f"cleaned_stock_parquet_{year}"

    output_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    # 1) Read Excel with pandas and normalise structure
    log(f"reading Excel: {excel_path}")
    df_xlsx = pd.read_excel(excel_path, header=0)
    df_no_space = df_xlsx.applymap(lambda x: x if pd.isna(x) else str(x).replace(" ", ""))

    column_names = list(df_no_space.columns)
    valid_cols_idx = [
        i for i, c in enumerate(column_names)
        if i == 0
        or (
            isinstance(c, str)
            and not re.match(r"^\s*$", c)
            and not re.match(r"^Unnamed.*", c)
            and not re.match(r"^\d+(\.\d+)?$", c)
        )
    ]
    filtered_data = df_no_space.iloc[:, valid_cols_idx].copy()

    first_col = filtered_data.columns[0]
    filtered_data = filtered_data.drop_duplicates().drop_duplicates(subset=first_col, keep="first")

    # Force numeric conversion on known numeric columns
    for c in ["OnHand", "IsCommited", "OnOrder", "AvgPrice"]:
        if c in filtered_data.columns:
            filtered_data[c] = pd.to_numeric(filtered_data[c], errors="coerce")

    filtered_data.to_parquet(parquet_stage_path, index=False)
    log(f"wrote staged Parquet: {parquet_stage_path}")

    # 2) Spark reads the staged Parquet and performs ETL
    spark = create_spark(app_name=f"StockETL-{year}", driver_memory="6g", executor_memory="6g")
    df = spark.read.parquet(str(parquet_stage_path))

    all_columns = df.columns
    date_col_indices = [i for i, c in enumerate(all_columns) if "Date" in str(c)]

    reshaped = []
    for i, date_idx in enumerate(date_col_indices):
        record_date = df.first()[date_idx]
        start = date_col_indices[i - 1] + 1 if i > 0 else 1
        end = date_idx
        wh_cols = all_columns[start:end]
        if not wh_cols:
            continue

        tmp = df.selectExpr([f"`{all_columns[0]}`"] + [f"`{c}`" for c in wh_cols])
        stack_expr = ", ".join([f"'{c.split('.')[0]}', `{c}`" for c in wh_cols])
        stacked = tmp.selectExpr(
            f"`{all_columns[0]}` as ItemCode",
            f"stack({len(wh_cols)}, {stack_expr}) as (WhsCode, OnHand)"
        ).withColumn("RecordDate", lit(record_date))
        reshaped.append(stacked)

    final_df = reduce(DataFrame.unionByName, reshaped)

    # Basic structural cleaning
    final_df = (
        final_df.na.drop(subset=["ItemCode", "WhsCode", "OnHand", "RecordDate"])
        .withColumn("ValidFor", when(col("OnHand") == "DC", "N").otherwise("Y"))
        .filter(col("OnHand") != "DC")
        .withColumn("OnHand", col("OnHand").cast("double"))
        .filter(col("OnHand") != 0.0)
        .withColumn("IsCommited", lit(0.0))
        .withColumn("OnOrder", lit(0.0))
        .withColumn("AvgPrice", lit(0.0))
    )

    # Remove duplicates on the business key
    w_dup_key = (
        Window
        .partitionBy("ItemCode", "WhsCode", "RecordDate")
        .orderBy(F.col("OnHand").desc_nulls_last())
    )
    final_df = final_df.withColumn("row_num", row_number().over(w_dup_key)).filter(col("row_num") == 1).drop("row_num")

    # Standardise date
    final_df = final_df.withColumn(
        "RecordDate",
        date_format(to_date("RecordDate", "yyyy-MMM-dd"), "yyyy-MM-dd")
    ).withColumn("RecordDate", F.to_date("RecordDate", "yyyy-MM-dd"))

    # 3) Gap aware outlier and null handling on existing records only
    key_cols = ["ItemCode", "WhsCode"]
    w_ord = Window.partitionBy(*key_cols).orderBy("RecordDate")

    prev_non_null = F.last("OnHand", ignorenulls=True).over(
        w_ord.rowsBetween(Window.unboundedPreceding, -1)
    )
    next_non_null = F.first("OnHand", ignorenulls=True).over(
        w_ord.rowsBetween(1, Window.unboundedFollowing)
    )

    abs_change = F.when(prev_non_null.isNotNull(), F.abs(F.col("OnHand") - prev_non_null))
    rel_change = F.when(
        prev_non_null.isNotNull() & (prev_non_null != 0.0),
        F.abs((F.col("OnHand") - prev_non_null) / prev_non_null)
    )

    is_outlier = F.when(
        F.col("OnHand").isNotNull() & (
            (abs_change.isNotNull() & (abs_change > F.lit(abs_jump))) |
            (rel_change.isNotNull() & (rel_change > F.lit(rel_jump)))
        ),
        F.lit(True)
    ).otherwise(F.lit(False))

    filled_onhand = (
        F.when(is_outlier & prev_non_null.isNotNull(), prev_non_null)
         .when(is_outlier & prev_non_null.isNull() & next_non_null.isNotNull(), next_non_null)
         .when(F.col("OnHand").isNull() & prev_non_null.isNotNull(), prev_non_null)
         .when(F.col("OnHand").isNull() & prev_non_null.isNull() & next_non_null.isNotNull(), next_non_null)
         .otherwise(F.col("OnHand"))
    )

    df_cleaned = (
        final_df
        .withColumn("OnHand", filled_onhand)
        .withColumn("IsCommited", F.coalesce("IsCommited", F.lit(0.0)))
        .withColumn("OnOrder",   F.coalesce("OnOrder",   F.lit(0.0)))
        .withColumn("AvgPrice",  F.coalesce("AvgPrice",  F.lit(0.0)))
        .withColumn("ValidFor",  F.coalesce("ValidFor",  F.lit("Y")))
    )

    # Safety dedup after replacements
    w_final = Window.partitionBy(*key_cols, "RecordDate").orderBy(F.col("OnHand").desc_nulls_last())
    df_cleaned = df_cleaned.withColumn("rn", row_number().over(w_final)).filter(col("rn") == 1).drop("rn")

    # 4) Select columns and write curated Parquet
    df_cleaned = df_cleaned.select(
        "ItemCode", "WhsCode", "OnHand", "IsCommited", "OnOrder", "AvgPrice", "ValidFor", "RecordDate"
    )

    df_cleaned.write.mode("overwrite").parquet(str(parquet_path))
    log(f"wrote curated Parquet: {parquet_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True, help="Year to process, for example 2025")
    parser.add_argument("--abs_jump", type=float, default=500.0, help="Absolute day on day change threshold")
    parser.add_argument("--rel_jump", type=float, default=5.0, help="Relative day on day change threshold where 5.0 means 5x")
    args = parser.parse_args()
    run(args.year, args.abs_jump, args.rel_jump)
