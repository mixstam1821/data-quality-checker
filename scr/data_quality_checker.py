"""
Data Quality Checker
====================
A PySpark + SQL tool to profile and validate any dataset.
Useful for detecting issues before loading data into a warehouse.

Author: Your Name
Tools: PySpark, SQL
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, NumericType, DateType, TimestampType
import json
import os
from datetime import datetime


def create_spark_session(app_name="DataQualityChecker"):
    """Create and return a Spark session."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.repl.eagerEval.enabled", True)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_data(spark, file_path: str):
    """
    Load data from CSV, JSON, or Parquet.
    Auto-detects format from file extension.
    """
    ext = file_path.split(".")[-1].lower()

    if ext == "csv":
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    elif ext == "json":
        df = spark.read.option("multiline", "true").json(file_path)
    elif ext == "parquet":
        df = spark.read.parquet(file_path)
    else:
        raise ValueError(f"Unsupported format: {ext}. Use csv, json, or parquet.")

    print(f"✅ Loaded {df.count()} rows and {len(df.columns)} columns from {file_path}")
    return df


# ─────────────────────────────────────────────
# CORE CHECKS
# ─────────────────────────────────────────────

def check_nulls(df):
    """
    Check null/missing values per column.
    Returns a list of dicts with column name, null count, and null percentage.
    """
    total_rows = df.count()
    results = []

    for col_name in df.columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_pct = round((null_count / total_rows) * 100, 2) if total_rows > 0 else 0
        results.append({
            "column": col_name,
            "null_count": null_count,
            "null_pct": null_pct,
            "status": "❌ FAIL" if null_pct > 0 else "✅ PASS"
        })

    return results


def check_duplicates(df, subset_cols=None):
    """
    Check for duplicate rows.
    Optionally check duplicates on a subset of columns (e.g. primary key columns).
    """
    total_rows = df.count()

    if subset_cols:
        distinct_rows = df.select(subset_cols).distinct().count()
        label = f"on columns {subset_cols}"
    else:
        distinct_rows = df.distinct().count()
        label = "full row"

    duplicate_count = total_rows - distinct_rows
    duplicate_pct = round((duplicate_count / total_rows) * 100, 2) if total_rows > 0 else 0

    return {
        "check": f"Duplicates ({label})",
        "total_rows": total_rows,
        "distinct_rows": distinct_rows,
        "duplicate_count": duplicate_count,
        "duplicate_pct": duplicate_pct,
        "status": "❌ FAIL" if duplicate_count > 0 else "✅ PASS"
    }


def check_column_stats(df):
    """
    For numeric columns: min, max, avg, stddev.
    For string columns: min length, max length, distinct count.
    Uses Spark SQL for aggregation.
    """
    df.createOrReplaceTempView("dataset")
    spark = df.sparkSession
    results = []

    for field in df.schema.fields:
        col_name = field.name
        dtype = field.dataType

        if isinstance(dtype, NumericType):
            sql = f"""
                SELECT
                    '{col_name}' AS column,
                    'numeric'    AS type,
                    MIN(`{col_name}`)    AS min_val,
                    MAX(`{col_name}`)    AS max_val,
                    ROUND(AVG(`{col_name}`), 2) AS avg_val,
                    ROUND(STDDEV(`{col_name}`), 2) AS stddev_val,
                    COUNT(DISTINCT `{col_name}`) AS distinct_count
                FROM dataset
            """
            row = spark.sql(sql).collect()[0]
            results.append({
                "column": col_name,
                "type": "numeric",
                "min": row["min_val"],
                "max": row["max_val"],
                "avg": row["avg_val"],
                "stddev": row["stddev_val"],
                "distinct_count": row["distinct_count"]
            })

        elif isinstance(dtype, StringType):
            sql = f"""
                SELECT
                    '{col_name}' AS column,
                    'string'     AS type,
                    MIN(LENGTH(`{col_name}`)) AS min_length,
                    MAX(LENGTH(`{col_name}`)) AS max_length,
                    COUNT(DISTINCT `{col_name}`) AS distinct_count
                FROM dataset
            """
            row = spark.sql(sql).collect()[0]
            results.append({
                "column": col_name,
                "type": "string",
                "min_length": row["min_length"],
                "max_length": row["max_length"],
                "distinct_count": row["distinct_count"]
            })

        elif isinstance(dtype, (DateType, TimestampType)):
            sql = f"""
                SELECT
                    '{col_name}' AS column,
                    'date'       AS type,
                    MIN(`{col_name}`) AS min_date,
                    MAX(`{col_name}`) AS max_date,
                    COUNT(DISTINCT `{col_name}`) AS distinct_count
                FROM dataset
            """
            row = spark.sql(sql).collect()[0]
            results.append({
                "column": col_name,
                "type": "date",
                "min_date": str(row["min_date"]),
                "max_date": str(row["max_date"]),
                "distinct_count": row["distinct_count"]
            })

    return results


def check_outliers(df, z_score_threshold=3.0):
    """
    Detect outliers in numeric columns using Z-score method.
    Flags values that are more than z_score_threshold standard deviations from the mean.
    """
    results = []

    for field in df.schema.fields:
        if isinstance(field.dataType, NumericType):
            col_name = field.name

            stats = df.select(
                F.mean(F.col(col_name)).alias("mean"),
                F.stddev(F.col(col_name)).alias("stddev")
            ).collect()[0]

            mean_val = stats["mean"]
            stddev_val = stats["stddev"]

            if stddev_val and stddev_val > 0:
                outlier_count = df.filter(
                    F.abs((F.col(col_name) - mean_val) / stddev_val) > z_score_threshold
                ).count()

                results.append({
                    "column": col_name,
                    "mean": round(mean_val, 2),
                    "stddev": round(stddev_val, 2),
                    "outlier_count": outlier_count,
                    "threshold": f"Z > {z_score_threshold}",
                    "status": "⚠️  WARN" if outlier_count > 0 else "✅ PASS"
                })

    return results


def check_schema(df, expected_schema: dict):
    """
    Validate that the DataFrame has expected columns and types.
    expected_schema = {"column_name": "expected_type_string"}
    Example: {"age": "integer", "name": "string", "salary": "double"}
    """
    results = []
    actual_schema = {field.name: field.dataType.simpleString() for field in df.schema.fields}

    for col_name, expected_type in expected_schema.items():
        if col_name not in actual_schema:
            results.append({
                "column": col_name,
                "expected_type": expected_type,
                "actual_type": "MISSING",
                "status": "❌ FAIL"
            })
        elif expected_type.lower() not in actual_schema[col_name].lower():
            results.append({
                "column": col_name,
                "expected_type": expected_type,
                "actual_type": actual_schema[col_name],
                "status": "❌ FAIL"
            })
        else:
            results.append({
                "column": col_name,
                "expected_type": expected_type,
                "actual_type": actual_schema[col_name],
                "status": "✅ PASS"
            })

    return results


# ─────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────

def generate_report(df, output_dir="reports", expected_schema=None, pk_columns=None):
    """
    Run all checks and generate a JSON + printed report.
    """
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("\n" + "="*60)
    print("       DATA QUALITY REPORT")
    print("="*60)
    print(f"  Timestamp : {timestamp}")
    print(f"  Rows      : {df.count()}")
    print(f"  Columns   : {len(df.columns)}")
    print(f"  Schema    : {[f.name + ':' + f.dataType.simpleString() for f in df.schema.fields]}")
    print("="*60)

    report = {
        "timestamp": timestamp,
        "row_count": df.count(),
        "column_count": len(df.columns),
        "checks": {}
    }

    # 1. Null Check
    print("\n📋 NULL CHECK")
    print("-"*40)
    null_results = check_nulls(df)
    for r in null_results:
        print(f"  {r['status']}  {r['column']:<20} nulls: {r['null_count']} ({r['null_pct']}%)")
    report["checks"]["nulls"] = null_results

    # 2. Duplicate Check
    print("\n📋 DUPLICATE CHECK")
    print("-"*40)
    dup_result = check_duplicates(df, subset_cols=pk_columns)
    print(f"  {dup_result['status']}  {dup_result['check']}")
    print(f"       Total rows: {dup_result['total_rows']} | Duplicates: {dup_result['duplicate_count']} ({dup_result['duplicate_pct']}%)")
    report["checks"]["duplicates"] = dup_result

    # 3. Column Stats
    print("\n📋 COLUMN STATISTICS")
    print("-"*40)
    stats_results = check_column_stats(df)
    for r in stats_results:
        if r["type"] == "numeric":
            print(f"  📊 {r['column']:<20} min={r['min']} max={r['max']} avg={r['avg']} stddev={r['stddev']}")
        elif r["type"] == "string":
            print(f"  🔤 {r['column']:<20} min_len={r['min_length']} max_len={r['max_length']} distinct={r['distinct_count']}")
        elif r["type"] == "date":
            print(f"  📅 {r['column']:<20} min={r['min_date']} max={r['max_date']} distinct={r['distinct_count']}")
    report["checks"]["column_stats"] = stats_results

    # 4. Outlier Check
    print("\n📋 OUTLIER CHECK (Z-score > 3)")
    print("-"*40)
    outlier_results = check_outliers(df)
    for r in outlier_results:
        print(f"  {r['status']}  {r['column']:<20} outliers: {r['outlier_count']} (mean={r['mean']}, stddev={r['stddev']})")
    report["checks"]["outliers"] = outlier_results

    # 5. Schema Validation (optional)
    if expected_schema:
        print("\n📋 SCHEMA VALIDATION")
        print("-"*40)
        schema_results = check_schema(df, expected_schema)
        for r in schema_results:
            print(f"  {r['status']}  {r['column']:<20} expected={r['expected_type']} actual={r['actual_type']}")
        report["checks"]["schema_validation"] = schema_results

    # Save JSON report
    report_path = os.path.join(output_dir, f"dq_report_{timestamp}.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print("\n" + "="*60)
    print(f"  ✅ Report saved to: {report_path}")
    print("="*60 + "\n")

    return report


# ─────────────────────────────────────────────
# MAIN - Example Usage
# ─────────────────────────────────────────────

if __name__ == "__main__":
    spark = create_spark_session()

    # Load your dataset (change path to your file)
    df = load_data(spark, "data/sample_orders.csv")

    # Optional: define expected schema for validation
    expected_schema = {
        "order_id": "integer",
        "customer_id": "integer",
        "amount": "double",
        "status": "string",
        "order_date": "date"
    }

    # Run full quality report
    # pk_columns = columns that should be unique (like primary keys)
    report = generate_report(
        df,
        output_dir="reports",
        expected_schema=expected_schema,
        pk_columns=["order_id"]
    )

    spark.stop()
