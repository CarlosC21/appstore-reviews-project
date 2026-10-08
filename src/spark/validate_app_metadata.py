import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


# Use the same Python interpreter for Spark
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def main():
    project_root = Path(__file__).resolve().parents[2]

    parquet_path = (
        project_root
        / "processed"
        / "app_metadata"
    )

    spark = (
        SparkSession.builder
        .appName("ValidateAppleAppMetadata")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading Parquet from: {parquet_path}")
    print()

    df = spark.read.parquet(str(parquet_path))

    print("=== Parquet Schema ===")
    df.printSchema()
    print()

    print("=== Row Count ===")
    print(df.count())
    print()

    print("=== Data ===")
    df.show(truncate=False)
    print()

    print("=== Partition Values ===")
    df.select("country").distinct().show()
    print()

    spark.stop()


if __name__ == "__main__":
    main()