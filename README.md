# Stock ETL Project (Excel → Parquet → SQL Server)

## Overview
This project demonstrates an end-to-end ETL pipeline:
- **Extract**: Read yearly stock Excel files.
- **Transform**: Clean and reshape the data with PySpark.
- **Load**: Store cleaned data into Parquet files, merge into a single dataset, and (optionally) load into SQL Server.
- **Validate**: Verify outputs with a simple check script.

The project is structured as a portfolio example, showing practical ETL skills with Spark and Python.

---

## Project Structure
```
etl_project/
│── README.md
│── requirements.txt
│── .gitignore
│── .env.example         # Example config for MSSQL connection
│
├── data/                # Place raw Excel files here (e.g., 2019.xlsx, 2020.xlsx …)
├── output/              # Parquet outputs (ignored in git, kept with .gitkeep)
│   └── .gitkeep
└── src/
    ├── reader.py        # Process a single year of Excel → Parquet
    ├── merge.py         # Merge yearly Parquets → Final dataset
    ├── writer.py        # Load final dataset into SQL Server (requires .env)
    ├── check_output.py  # Verify row count, schema, sample rows
    └── utils.py         # Common utilities (Spark session, logging, helpers)
```

---

## Installation
1. Clone this repository
   ```bash
   git clone <your-repo-url>
   cd etl_project
   ```

2. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

3. Place your yearly Excel files into the `data/` directory (e.g., `2019.xlsx`, `2020.xlsx` …).

---

## Usage

### 1. Process a single year
```bash
python src/reader.py --year 2020
```

### 2. Merge all years into one final dataset
```bash
python src/merge.py
```

### 3. Check the output
```bash
python src/check_output.py
```

### 4. (Optional) Load into SQL Server
1. Copy `.env.example` → `.env` and fill in your MSSQL connection info:
   ```
   MSSQL_URL=jdbc:sqlserver://<HOST>:<PORT>;databaseName=<DB>;encrypt=false
   MSSQL_USER=<USERNAME>
   MSSQL_PASSWORD=<PASSWORD>
   MSSQL_TABLE=dbo.z_Stock_History_2
   ```

2. Run:
   ```bash
   python src/writer.py
   ```

> The `.env` file must **not** be committed. Only `.env.example` should be shared.

---

## Notes
- `output/` is ignored in git (via `.gitignore`). The `.gitkeep` ensures the folder appears in the repository without including large Parquet files.
- SparkUI may bind to ports like `4040`, `4041`, etc. This is normal and does not affect execution.
- This repository is intended as a portfolio project to showcase ETL, PySpark, and data engineering practices.
