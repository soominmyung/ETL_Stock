# src/merge.py
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame
from utils import create_spark, log, list_year_parquets, deduplicate_by_keys

def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    output_dir = project_root / "output"

    spark = create_spark("MergeStock")
    parquet_folders = list_year_parquets(output_dir=output_dir, prefix="cleaned_stock_parquet_")
    if not parquet_folders:
        log("no yearly parquet folders found under output/")
        return

    log(f"found {len(parquet_folders)} yearly parquet folders")
    dfs = [spark.read.parquet(str(p)) for p in parquet_folders]
    combined = reduce(DataFrame.unionByName, dfs)

    deduped = deduplicate_by_keys(combined, order_col="OnHand", order_desc=True)
    out_path = output_dir / "final_cleaned_stock_parquet"
    deduped.write.mode("overwrite").parquet(str(out_path))
    log(f"wrote final dataset: {out_path}")

if __name__ == "__main__":
    main()
