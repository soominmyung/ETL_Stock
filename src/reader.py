# src/reader.py
"""
reader.py - ETL pipeline for yearly stock Excel files

Usage:
    python reader.py --year 2025 --abs_jump 500 --rel_jump 5.0

Arguments:
    --year       Year of the Excel file to process (required)
    --abs_jump   Absolute day-on-day change threshold for outlier detection.
                 Example: 500 means any change >= 500 units from previous day is flagged as outlier.
    --rel_jump   Relative day-on-day change threshold for outlier detection.
                 Example: 5.0 means any change >= 5x (500%) compared to previous day is flagged.

Logic:
    1. Read the yearly Excel file, clean headers and rows, and reshape wide format into long format.
    2. Drop duplicates and invalid rows, standardise date formats, and cast numeric columns.
    3. Detect outliers:
         - If daily change exceeds abs_jump or rel_jump, mark as outlier.
    4. Handle outliers:
         - Replace with the nearest previous valid value.
         - If previous is missing, use the nearest next valid value.
         - If both are missing, keep the original.
    5. Keep original OnHand as OnHand_raw and flag outliers with OutlierFlag for auditing.
    6. Write the cleaned dataset to Parquet.
"""

# src/reader.py
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
    csv_temp_path = data_dir / f"{year}_temp.csv"
    csv_clean_path = data_dir / f"{year}.csv"
    parquet_path = output_dir / f"cleaned_stock_parquet_{year}"

    output_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    log(f"reading Excel: {excel_path}")
    # Read Excel with the first row as header, then normalise headers and rows
    df_xlsx = pd.read_excel(excel_path, header=0)
    df_xlsx.to_csv(csv_temp_path, index=False, header=True)
    df_raw = pd.read_csv(csv_temp_path, header=None)
    df_raw = df_raw.applymap(lambda x: x if pd.isna(x) else str(x).replace(" ", ""))

    column_names = df_raw.iloc[0].tolist()
    data_rows = df_raw.iloc[1:].copy()

    # Keep the first column and named columns that are not empty, not Unnamed, not pure numbers
    valid_cols = [
        i for i, c in enumerate(column_names)
        if i == 0
        or (
            isinstance(c, str)
            and not re.match(r"^\s*$", c)
            and not re.match(r"^Unnamed.*", c)
            and not re.match(r"^\d+(\.\d+)?$", c)
        )
    ]
    filtered_data = data_rows.iloc[:, valid_cols]
    filtered_data.columns = [column_names[i] for i in valid_cols]
    first_col = filtered_data.columns[0]
    filtered_data = filtered_data.drop_duplicates().drop_duplicates(subset=first_col, keep="first")

    filtered_data.to_csv(csv_clean_path, index=False)
    log(f"wrote cleaned CSV: {csv_clean_path}")

    spark = create_spark(app_name=f"StockETL-{year}", driver_memory="6g", executor_memory="6g")
    df = spark.read.option("header", True).csv(str(csv_clean_path))

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

    # De dup by business key keeping one row with window
    w = Window.partitionBy("ItemCode", "WhsCode", "RecordDate").orderBy("OnHand")
    final_df = final_df.withColumn("row_num", row_number().over(w)).filter(col("row_num") == 1).drop("row_num")

    # Standardise date
    final_df = final_df.withColumn(
        "RecordDate",
        date_format(to_date("RecordDate", "yyyy-MMM-dd"), "yyyy-MM-dd")
    )

    # Types for downstream logic
    final_df = (
        final_df
        .withColumn("RecordDate", F.to_date("RecordDate", "yyyy-MM-dd"))
        .withColumn("OnHand", F.col("OnHand").cast("double"))
    )

    # Outlier handling without fabricating missing weekdays
    # We do not create a daily spine and we do not add rows for absent days
    key_cols = ["ItemCode", "WhsCode"]
    w_ord = Window.partitionBy(*key_cols).orderBy("RecordDate")

    # Nearest previous non null within existing records
    prev_non_null = F.last("OnHand", ignorenulls=True).over(
        w_ord.rowsBetween(Window.unboundedPreceding, -1)
    )
    # Nearest next non null within existing records
    next_non_null = F.first("OnHand", ignorenulls=True).over(
        w_ord.rowsBetween(1, Window.unboundedFollowing)
    )

    # Outlier rule compared to the closest previous
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

    # Replacement rule
    # If outlier then use previous non null
    # If previous is absent then use next non null
    # If both are absent keep original
    filled_onhand = (
        F.when(is_outlier & prev_non_null.isNotNull(), prev_non_null)
         .when(is_outlier & prev_non_null.isNull() & next_non_null.isNotNull(), next_non_null)
         .otherwise(F.col("OnHand"))
    )

    df_cleaned = (
        final_df
        .withColumn("OnHand_raw", F.col("OnHand"))
        .withColumn("OutlierFlag", is_outlier.cast("string"))
        .withColumn("OnHand", filled_onhand)
        .withColumn("IsCommited", F.coalesce("IsCommited", F.lit(0.0)))
        .withColumn("OnOrder",   F.coalesce("OnOrder",   F.lit(0.0)))
        .withColumn("AvgPrice",  F.coalesce("AvgPrice",  F.lit(0.0)))
        .withColumn("ValidFor",  F.coalesce("ValidFor",  F.lit("Y")))
    )

    # Safety de dup in case replacements created ties
    w_dup = Window.partitionBy(*key_cols, "RecordDate").orderBy(F.col("OnHand").desc_nulls_last())
    df_cleaned = df_cleaned.withColumn("rn", F.row_number().over(w_dup)).filter(F.col("rn") == 1).drop("rn")

    # Select final columns
    df_cleaned = df_cleaned.select(
        "ItemCode", "WhsCode", "OnHand", "IsCommited", "OnOrder", "AvgPrice", "ValidFor", "RecordDate",
        "OutlierFlag", "OnHand_raw"
    )

    # Write parquet
    df_cleaned.write.mode("overwrite").parquet(str(parquet_path))
    log(f"wrote parquet with outlier handling: {parquet_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True, help="Year to process, for example 2025")
    parser.add_argument("--abs_jump", type=float, default=500.0, help="Absolute day on day jump threshold")
    parser.add_argument("--rel_jump", type=float, default=5.0, help="Relative day on day jump threshold where 5 means 500 percent")
    args = parser.parse_args()
    run(args.year, args.abs_jump, args.rel_jump)
