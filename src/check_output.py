# src/check_output.py
from pathlib import Path
from utils import create_spark, log

def main():
    project_root = Path(__file__).resolve().parents[1]
    path = project_root / "output" / "final_cleaned_stock_parquet"

    spark = create_spark("CheckOutput")
    df = spark.read.parquet(str(path))

    log(f"row count: {df.count()}")
    log("schema:")
    df.printSchema()

    log("sample rows:")
    df.show(10, truncate=False)

    if "ItemCode" in df.columns and "WhsCode" in df.columns:
        uniq_items = df.select("ItemCode").distinct().count()
        uniq_whs = df.select("WhsCode").distinct().count()
        log(f"distinct ItemCode: {uniq_items}")
        log(f"distinct WhsCode: {uniq_whs}")

if __name__ == "__main__":
    main()
