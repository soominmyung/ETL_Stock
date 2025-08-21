# src/writer.py
from pathlib import Path
from dotenv import load_dotenv
from utils import create_spark, load_db_config_from_env, log

def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / ".env")

    spark = create_spark("WriteMSSQL")
    df = spark.read.parquet(str(project_root / "output" / "final_cleaned_stock_parquet"))
    log(f"loaded final dataframe with {df.count()} rows")

    db = load_db_config_from_env()

    (df.write
        .format("jdbc")
        .option("url", db["url"])
        .option("dbtable", db["table"])
        .option("user", db["user"])
        .option("password", db["password"])
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .mode("append")
        .save()
    )
    log(f"data written to {db['table']} in MSSQL")

if __name__ == "__main__":
    main()
