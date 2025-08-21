# src/reader.py
import argparse
import re
from functools import reduce
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, lit, col, row_number, date_format, to_date
from pyspark.sql.window import Window

from utils import create_spark, log


def run(year: int) -> None:
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
    df_xlsx = pd.read_excel(excel_path, header=0)
    df_xlsx.to_csv(csv_temp_path, index=False, header=True)
    df_raw = pd.read_csv(csv_temp_path, header=None)
    df_raw = df_raw.applymap(lambda x: x if pd.isna(x) else str(x).replace(" ", ""))

    column_names = df_raw.iloc[0].tolist()
    data_rows = df_raw.iloc[1:].copy()

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

    w = Window.partitionBy("ItemCode", "WhsCode", "RecordDate").orderBy("OnHand")
    final_df = final_df.withColumn("row_num", row_number().over(w)).filter(col("row_num") == 1).drop("row_num")

    final_df = final_df.withColumn(
        "RecordDate",
        date_format(to_date("RecordDate", "yyyy-MMM-dd"), "yyyy-MM-dd")
    ).select(
        "ItemCode", "WhsCode", "OnHand", "IsCommited", "OnOrder", "AvgPrice", "ValidFor", "RecordDate"
    )

    final_df.write.mode("overwrite").parquet(str(parquet_path))
    log(f"wrote parquet: {parquet_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True, help="Year to process, for example 2025")
    args = parser.parse_args()
    run(args.year)
