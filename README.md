# 🔍 Data Quality Checker

A lightweight **PySpark + SQL** tool to profile and validate any dataset before loading it into a data warehouse. Designed for real-world data engineering pipelines.

---

## 🚀 What It Does

Automatically runs 5 quality checks on any dataset:

| Check | What it detects |
|---|---|
| **Null Check** | Missing values per column with percentage |
| **Duplicate Check** | Full-row or primary-key duplicates |
| **Column Statistics** | Min, max, avg, stddev for numeric; length stats for strings |
| **Outlier Detection** | Z-score based outlier flagging for numeric columns |
| **Schema Validation** | Validates column names and data types against expected schema |

Outputs a clean **JSON report** saved to `/reports`.

---

## 🛠️ Tech Stack

- **PySpark** — distributed data processing
- **Spark SQL** — aggregations and statistics
- **Python** — orchestration and reporting

---

## 📁 Project Structure

```
data-quality-checker/
│
├── src/
│   └── data_quality_checker.py   # Main tool
│
├── data/
│   └── sample_orders.csv         # Example dataset
│
├── reports/                      # Auto-generated JSON reports
│
├── requirements.txt
└── README.md
```

---

## ⚙️ Setup

```bash
# Clone the repo
git clone https://github.com/mixstam1821/data-quality-checker.git
cd data-quality-checker
mkdir -p reports

java -version

# Install Java if not already installed
sudo apt install default-jdk -y

export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))

# Install dependencies
pip install -r requirements.txt

# Run on the sample dataset
python src/data_quality_checker.py
```

---

## 📊 Example Output

```
============================================================
       DATA QUALITY REPORT
============================================================
  Timestamp : 20240215_143022
  Rows      : 15
  Columns   : 6
============================================================

📋 NULL CHECK
----------------------------------------
  ❌ FAIL  amount               nulls: 1 (6.67%)
  ❌ FAIL  customer_id          nulls: 1 (6.67%)
  ❌ FAIL  order_date           nulls: 1 (6.67%)
  ✅ PASS  order_id             nulls: 0 (0.0%)

📋 DUPLICATE CHECK
----------------------------------------
  ❌ FAIL  Duplicates (on columns ['order_id'])
       Total rows: 15 | Duplicates: 0 (0.0%)

📋 COLUMN STATISTICS
----------------------------------------
  📊 amount               min=45.0 max=750000.0 avg=50288.77
  🔤 status               min_len=6 max_len=9 distinct=3
  📅 order_date           min=2024-01-15 max=2024-01-28

📋 OUTLIER CHECK (Z-score > 3)
----------------------------------------
  ⚠️  WARN  amount               outliers: 1 (mean=50288.77, stddev=193201.45)
```

---

## 🔧 How To Use On Your Own Data

```python
from src.data_quality_checker import create_spark_session, load_data, generate_report

spark = create_spark_session()

# Load your file (csv, json, or parquet)
df = load_data(spark, "path/to/your/file.csv")

# Define expected schema (optional but recommended)
expected_schema = {
    "id": "integer",
    "name": "string",
    "amount": "double"
}

# Run checks
report = generate_report(
    df,
    output_dir="reports",
    expected_schema=expected_schema,
    pk_columns=["id"]        # columns that should be unique
)

spark.stop()
```

---

## 💡 Real-World Use Cases

- **Before loading to S3/Athena** — catch bad data early
- **After ETL jobs** — validate output quality
- **Data pipeline monitoring** — integrate into Airflow DAG
- **Onboarding new data sources** — profile unknown datasets

---

## 📌 AWS Integration Example

This tool fits naturally into an AWS pipeline:

```
S3 (raw data)
    ↓
Glue Job (run data_quality_checker.py)
    ↓
S3 (quality report JSON)
    ↓
Athena (query reports) / CloudWatch (alert on failures)
```

---

## 🤝 Contributing

Pull requests welcome. For major changes, please open an issue first.

---

## 📄 License

MIT
