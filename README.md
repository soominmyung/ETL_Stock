# Stock ETL Project

This project demonstrates an end-to-end ETL pipeline for processing historical stock data. It was originally created to replace error-prone manual Excel logging with a reproducible and scalable process using PySpark and SQL Server.

## Features
- Read yearly Excel files and standardise them into a consistent schema
- Clean and transform stock records (dates, numeric types, trimming, handling nulls)
- Store outputs in Parquet format partitioned by year
- Merge multiple yearly datasets into a single consolidated dataset
- Optionally load the dataset into SQL Server for reporting and analysis
- Provide simple validation scripts to check record counts and schema consistency

## Project Structure
```
etl_project/
│── README.md
│── requirements.txt
│── .gitignore
│── .env.example
│
├── data/                # Input Excel files (e.g. 2019.xlsx, 2020.xlsx)
├── output/              # Parquet outputs
│   └── .gitkeep
└── src/
    ├── reader.py        # Excel → Parquet conversion
    ├── merge.py         # Merge multiple Parquet files
    ├── writer.py        # Load dataset into SQL Server
    ├── check_output.py  # Validation of schema and row counts
    └── utils.py         # Spark session setup and shared utilities
```
## Setup
1. Install dependencies
   pip install -r requirements.txt

2. Prepare Java and Spark runtime

3. If loading into SQL Server, download the MSSQL JDBC driver and configure environment variables in .env

## Usage
1. Process a single year Excel file
   python src/reader.py --year 2020

2. Merge yearly Parquet files
   python src/merge.py

3. Validate outputs
   python src/check_output.py

4. Load into SQL Server (optional)
   Set values in .env:
   MSSQL_URL=jdbc:sqlserver://HOST:PORT;databaseName=DB;encrypt=false
   MSSQL_USER=username
   MSSQL_PASSWORD=password
   MSSQL_TABLE=dbo.z_Stock_History

   Then run:
   python src/writer.py

## Example Schema
The cleaned and standardised dataset written to Parquet and optionally SQL Server contains the following columns:

Column       | Type      | Description
-------------|-----------|---------------------------------------------------
ItemCode     | string    | Product identifier
WhsCode      | string    | Warehouse identifier
OnHand       | double    | Current stock quantity
IsCommited   | double    | Committed stock quantity (default 0.0)
OnOrder      | double    | Quantity on order (default 0.0)
AvgPrice     | double    | Average price (default 0.0)
ValidFor     | string    | Item validity flag, set to N when excluded
RecordDate   | date      | Date of record, converted to yyyy-MM-dd format

This schema is generated in reader.py and maintained consistently in downstream steps (check_output.py, writer.py).

## Data Quality Checks
Validation is built into the workflow through check_output.py and utility functions in utils.py. The following checks are supported:

- Row count verification: Logs the number of rows loaded after cleaning and before writing to Parquet or SQL Server.
- Schema validation: Prints the full schema of the DataFrame to confirm expected column names and types.
- Sample records inspection: Displays a small set of rows for quick review.
- Distinct value counts: Calculates unique counts of key identifiers (ItemCode, WhsCode) to confirm coverage.
- Deduplication: Rows are deduplicated by (ItemCode, WhsCode, RecordDate) using a window function in utils.deduplicate_by_keys.
- Date standardisation: All RecordDate values are parsed and reformatted consistently with utils.format_date_col.

These checks provide early detection of schema drift, missing data, or anomalies in stock records.

## Why this project / Next steps
In SAP 9.3 there was no native function to keep daily stock history. As a workaround, the purchasing team had to open SAP each day, copy the stock levels into Excel, and save CSV logs. This manual process depended entirely on a single staff member, was time-consuming, and prone to input errors. The logs were also stored in a format that was difficult to combine with other systems. Issues included missing records, duplicated item codes with different stock levels, and unrealistic jumps in stock values (e.g. from zero to thousands in a day).

Other teams had to request data from the purchasing staff whenever they needed insights, which meant further manual processing, delays, and repeated errors. Given the scale of a £78M turnover business handling thousands of SKUs moving in and out daily, capturing snapshots during working hours introduced significant inconsistencies.

To solve this, I discussed with purchasing, inventory, and related teams how to handle problematic records. Based on agreed rules, I built this project to generate standardised daily snapshots in the company database. The snapshots are written in a consistent schema that is both human-readable and system-friendly. An MSSQL Agent job was created to run T-SQL daily at 23:00, ensuring that stock levels are captured automatically outside business hours. This reduced manual workload, removed errors, and provided reliable daily stock history for the first time.

As next steps, this dataset can be connected directly to BI dashboards (Tableau, Power BI) to provide managers and other teams with immediate visibility into stock trends, out-of-stock risks, and slow-moving items without depending on manual reporting.
