import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


# Windows / PySpark configuration
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = r"C:\hadoop\hadoop-win-utils"


def main():
    project_root = Path(__file__).resolve().parents[2]

    spark = (
        SparkSession.builder
        .appName("DemonstratePartitionPruning")
        .master("local[*]")
        .config(
            "spark.sql.warehouse.dir",
            str(project_root / "hive_warehouse")
        )
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("\n=== Registered partitions ===")

    spark.sql("""
        SHOW PARTITIONS appstore.app_metadata
    """).show(truncate=False)

    print("\n=== Query WITHOUT partition filter ===")

    df_all = spark.sql("""
        SELECT
            app_id,
            track_name,
            country
        FROM appstore.app_metadata
    """)

    df_all.show(truncate=False)

    print("\n=== Physical plan WITHOUT partition filter ===")
    df_all.explain(mode="formatted")

    print("\n=== Query WITH partition filter ===")

    df_at = spark.sql("""
        SELECT
            app_id,
            track_name,
            country
        FROM appstore.app_metadata
        WHERE country = 'at'
    """)

    df_at.show(truncate=False)

    print("\n=== Physical plan WITH partition filter ===")
    df_at.explain(mode="formatted")

    print("\n=== CREATE TABLE definition ===")

    spark.sql("""
        SHOW CREATE TABLE appstore.app_metadata
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()