# 🔄 Data Ingestion Pipeline

An ETL pipeline that automates the process of extracting CSV files from a ZIP archive, converting them to Parquet format, and loading them into a PostgreSQL database. This tool is designed for data professionals looking for a simple and efficient scriptable way to ingest structured data into a relational database.

---

## 🚀 Features

-  Extracts CSV files from a ZIP archive
-  Converts CSV files to efficient **Parquet** format using PyArrow
-  Loads data into **PostgreSQL**, one table per CSV file
-  Fully configurable via command-line arguments
-  Handles directory creation and file processing automatically
-  Prints total runtime for performance tracking

---

## 🛠️ Tools & Technologies

| Category             | Tool/Technology        | Purpose                                                                 |
|----------------------|------------------------|-------------------------------------------------------------------------|
| Programming Language | `Python`               | Core scripting language                                                 |
| Data Handling        | `pandas`               | Reading and processing CSV data                                        |
| File Format          | `pyarrow`, `parquet`   | Converting CSV files into columnar Parquet format                      |
| Database             | `PostgreSQL`           | Target database for loading processed data                             |
| DB Connectivity      | `SQLAlchemy`           | Interfacing with the PostgreSQL database                               |
| CLI Support          | `argparse`             | Accepting user inputs (e.g., credentials, host, port) via terminal     |
| File Handling        | `zipfile`, `os`, `time`| Extracting, managing directories, and timing operations                |
