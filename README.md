# Stock ETL Project

## Built with PySpark ETL, this project replaced error-prone Excel logs and delivered the first automated daily stock history for a £78M business
### _Key stacks: PySpark, ETL, SQL, MSSQL Agent, Parquet_

This project demonstrates an end-to-end ETL pipeline for processing historical stock data. It was originally created to replace error-prone manual Excel logging with a reproducible and scalable process using PySpark and SQL Server.


<img width="1061" height="586" alt="Stock-ETL" src="https://github.com/user-attachments/assets/019d6a96-e595-4ab3-8b21-1a3967f75c96" />

<br>

## Why this project
In SAP 9.3 there was no native function to keep daily stock history. As a workaround, the purchasing team had to open SAP each day, copy the stock levels into Excel, and save CSV logs. This manual process depended entirely on a single staff member, was time-consuming, and prone to input errors. The logs were also stored in a format that was difficult to combine with other systems. Issues included missing records, duplicated item codes with different stock levels, and unrealistic jumps in stock values (e.g. from zero to thousands in a day).

Other teams had to request data from the purchasing staff whenever they needed insights, which meant further manual processing, delays, and repeated errors. Given the scale of a £78M turnover business handling thousands of SKUs moving in and out daily, capturing snapshots during working hours introduced significant inconsistencies.

To solve this, I discussed with purchasing, inventory, and related teams how to handle problematic records. Based on agreed rules, I built this project to generate standardised daily snapshots in the company database. The snapshots are written in a consistent schema that is both human-readable and system-friendly. An MSSQL Agent job was created to run T-SQL daily at 23:00, ensuring that stock levels are captured automatically outside business hours. This reduced manual workload, removed errors, and provided reliable daily stock history for the first time.

<br>

## Features
- Read yearly Excel files with pandas and standardise headers/columns into a consistent schema
- Write to Parquet staging files to fix schema before Spark processing
- Use PySpark to reshape, clean, deduplicate, and handle outliers/nulls
- Store curated outputs in Parquet format partitioned by year
- Merge multiple yearly datasets into a single consolidated dataset
- Optionally load the dataset into SQL Server for reporting and analysis
- Provide simple validation scripts to check record counts and schema consistency

<br>

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
    ├── reader.py        # Read yearly stock Excel files with pandas → Parquet staging → PySpark cleaning & final Parquet
    ├── merge.py         # Merge multiple Parquet files
    ├── writer.py        # Load dataset into SQL Server
    ├── check_output.py  # Validation of schema and row counts
    └── utils.py         # Spark session setup and shared utilities
```

<br>

## Setup
1. Install dependencies
   pip install -r requirements.txt
   Dependencies include pandas, pyarrow, pyspark, openpyxl, python-dotenv.
2. Prepare Java and Spark runtime

3. If loading into SQL Server, download the MSSQL JDBC driver and configure environment variables in .env

<br>

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

<br>

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

<br>

## Data Quality Checks
Validation is built into the workflow through check_output.py and utility functions in utils.py. The following checks are supported:

- Row count verification: Logs the number of rows loaded after cleaning and before writing to Parquet or SQL Server.
- Schema validation: Prints the full schema of the DataFrame to confirm expected column names and types.
- Sample records inspection: Displays a small set of rows for quick review.
- Distinct value counts: Calculates unique counts of key identifiers (ItemCode, WhsCode) to confirm coverage.
- Deduplication: Rows are deduplicated by (ItemCode, WhsCode, RecordDate) using a window function in utils.deduplicate_by_keys.
- Date standardisation: All RecordDate values are parsed and reformatted consistently with utils.format_date_col.

These checks provide early detection of schema drift, missing data, or anomalies in stock records.

<br>

## Data Cleaning Process
### A) Pandas Stage (Excel → Parquet Staging)

#### - Excel Load and Whitespace Removal
After loading the Excel file, all string cells were stripped of leading and trailing spaces while preserving NaN values.
This prevents column mismatches and join errors caused by hidden spaces in headers or cell values.

#### - Valid Column Selection
The first column was always retained, while other columns were kept only if they met all of the following conditions:

#### - Non-empty string, Not following the “Unnamed” pattern, Not composed solely of digits
This filters out meaningless or automatically generated headers, ensuring schema stability.

#### - Duplicate Removal
Duplicates were removed twice: once across the entire DataFrame and once based on the first column.
This step eliminates redundant rows with identical **item codes**, preventing key conflicts in later processes.

#### - Numeric Column Enforcement
Columns such as OnHand, IsCommited, OnOrder, and AvgPrice were converted to numeric types.
Any non-convertible values were set to null to prevent type inconsistency and arithmetic errors.

#### - Staging as Parquet
The cleaned result was saved as a Parquet file to fix the schema before PySpark processing.
This ensures consistent column types and structures during Spark reads.

### B) PySpark Stage (Parquet → Cleaned Dataset)

#### - Wide-to-Long Transformation
Columns containing “Date” were used as pivot points to identify warehouse columns.
These were stacked to form a vertical schema of ItemCode, WhsCode, OnHand, and RecordDate.
This unifies daily snapshots into one consistent format.

#### - Base Structure Cleaning
Rows missing key columns were removed.
If OnHand contained the string “DC”, the corresponding row was marked as invalid (ValidFor = 'N') and excluded.
OnHand was converted to float, and rows with zero values were dropped.
Missing IsCommited, OnOrder, and AvgPrice values were filled with 0.
This standardises invalid entries and assigns consistent defaults.

#### - Deduplication by Business Keys
Duplicates were removed using a window function over (ItemCode, WhsCode, RecordDate), keeping only the most meaningful record.
This standardises multiple entries for the same item and warehouse on the same day.

#### - Date Standardisation
All date strings were parsed and reformatted to yyyy-MM-dd before casting to proper date type.
This enforces a consistent date format for sorting and window operations.

#### - Missing Value and Outlier Handling
Within each (ItemCode, WhsCode) partition, previous and next valid values were computed chronologically.
If the absolute change exceeded the defined threshold or the relative rate of change was too high, it was flagged as an outlier.
Missing or outlier values were replaced with the previous valid observation, or the next one if unavailable.
This preserves time-series continuity without creating synthetic weekend or non-working-day rows.

#### - Final Deduplication and Default Completion
After correction, a secondary deduplication ensured unique keys.
Any remaining nulls in IsCommited, OnOrder, AvgPrice, or ValidFor were filled with their default values.

#### - Final Column Selection and Save
Only the standard schema columns were retained, and the cleaned dataset was written back to Parquet.
The result is a consistent, analysis-ready dataset ready for merging and SQL Server loading.

## Next steps
With the reliable daily stock history produced by this project, I built Tableau dashboards that combined sales and stock trends for the first time. This enabled forward-looking analysis, including early detection of products at risk of going out of stock.  

Given that importing goods from Korea takes around four months by sea, these dashboards allowed proactive replenishment planning. By anticipating stock-outs in advance, the company prevented potential revenue loss and minimised missed sales opportunities, turning raw ETL outputs into measurable business impact.
