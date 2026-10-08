import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


# Windows / PySpark configuration
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Windows Hadoop configuration
os.environ["HADOOP_HOME"] = r"C:\hadoop\hadoop-win-utils"


def main():
    project_root = Path(__file__).resolve().parents[2]

    parquet_path = project_root / "processed" / "app_metadata"

    # Convert Windows path to a URI that Spark SQL can use reliably.
    parquet_uri = parquet_path.resolve().as_uri()

    print(f"Parquet location: {parquet_uri}")

    spark = (
        SparkSession.builder
        .appName("CreateAppMetadataExternalTable")
        .master("local[*]")
        .config(
            "spark.sql.warehouse.dir",
            str(project_root / "hive_warehouse")
        )
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Create a database for the project.
    spark.sql("""
        CREATE DATABASE IF NOT EXISTS appstore
    """)

    # Recreate the table during development so that we can
    # safely change the definition while learning.
    spark.sql("""
        DROP TABLE IF EXISTS appstore.app_metadata
    """)

    # Register the existing Parquet directory as an external table.
    spark.sql(f"""
        CREATE TABLE appstore.app_metadata (
            app_id INT,
            track_id BIGINT,
            track_name STRING,
            artist_name STRING,
            seller_name STRING,
            bundle_id STRING,
            primary_genre_id BIGINT,
            primary_genre_name STRING,
            average_user_rating DOUBLE,
            user_rating_count BIGINT,
            average_user_rating_current_version DOUBLE,
            user_rating_count_current_version BIGINT,
            price DOUBLE,
            currency STRING,
            formatted_price STRING,
            release_date TIMESTAMP,
            current_version_release_date TIMESTAMP,
            version STRING,
            minimum_os_version STRING,
            file_size_bytes BIGINT,
            content_advisory_rating STRING,
            track_content_rating STRING,
            is_game_center_enabled BOOLEAN,
            is_vpp_device_based_licensing_enabled BOOLEAN,
            wrapper_type STRING
        )
        USING PARQUET
        PARTITIONED BY (country STRING)
        LOCATION '{parquet_uri}'
    """)

    spark.sql("""
    REPAIR TABLE appstore.app_metadata
    """)

    print("\n=== Registered partitions ===")

    spark.sql("""
        SHOW PARTITIONS appstore.app_metadata
    """).show(truncate=False)

    print("\n=== Table data ===")

    spark.sql("""
        SELECT
            app_id,
            track_name,
            average_user_rating,
            user_rating_count,
            country
        FROM appstore.app_metadata
        ORDER BY country
    """).show(truncate=False)

    print("\n=== Table created ===")
    spark.sql("""
        SHOW TABLES IN appstore
    """).show(truncate=False)

    print("\n=== Table schema ===")
    spark.sql("""
        DESCRIBE appstore.app_metadata
    """).show(truncate=False)

    print("\n=== Table metadata ===")
    spark.sql("""
        DESCRIBE EXTENDED appstore.app_metadata
    """).show(truncate=False)

    print("\n=== Table data ===")
    spark.sql("""
        SELECT *
        FROM appstore.app_metadata
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()