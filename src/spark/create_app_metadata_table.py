
import json
import logging
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

# Ensure Spark workers use the Python interpreter running this script.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

from src.configuration.config_loader import (
    DEFAULT_LOG_FORMAT,
    PROJECT_ROOT,
    configure_logging,
    load_config,
)


logger = logging.getLogger(__name__)

DATABASE_NAME = "appstore"
TABLE_NAME = "app_metadata"
FULL_TABLE_NAME = f"{DATABASE_NAME}.{TABLE_NAME}"


# --------------------------------------------------
# Storage path resolution
# --------------------------------------------------

def to_spark_s3_uri(uri: str) -> str:
    """Convert an s3:// URI into the s3a:// URI used by Spark."""

    parsed = urlsplit(uri)

    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"Invalid S3 URI: {uri}")

    path = parsed.path.rstrip("/")
    return f"s3a://{parsed.netloc}{path}"


def resolve_parquet_location(config: dict) -> str:
    """Resolve the registered Parquet location for this environment."""

    storage = config["storage"]
    backend = storage["backend"]

    if backend == "local":
        parquet_path = (
            PROJECT_ROOT / storage["processed_output_dir"]
        ).resolve()

        if not parquet_path.is_dir():
            raise FileNotFoundError(
                f"Processed Parquet directory does not exist: "
                f"{parquet_path}"
            )

        return parquet_path.as_uri()

    if backend == "s3":
        return to_spark_s3_uri(
            storage["processed_output_uri"]
        )

    raise ValueError(
        f"Unsupported storage backend: {backend}"
    )


def normalize_location(location: str) -> str:
    """Normalize comparable file and S3 location URIs."""

    parsed = urlsplit(location.strip())

    scheme = parsed.scheme.lower()

    # Spark and Hadoop may report the same S3 location as s3:// or s3a://.
    if scheme == "s3":
        scheme = "s3a"

    path = parsed.path.rstrip("/")

    if scheme == "file":
        # Normalize equivalent file:/ and file:/// URIs.
        normalized = urlunsplit(
            ("file", "", path, "", "")
        )

        if os.name == "nt":
            return normalized.casefold()

        return normalized

    return urlunsplit(
        (scheme, parsed.netloc, path, "", "")
    )


# --------------------------------------------------
# Spark session
# --------------------------------------------------

def create_spark_session(config: dict) -> SparkSession:
    """Create a Spark session using the selected environment."""

    environment = config["environment"]
    storage_backend = config["storage"]["backend"]

    builder = (
        SparkSession.builder
        .appName("CreateAppMetadataExternalTable")
    )

    # Use an explicitly provided master when available.
    # For dev, default to the local Spark runtime.
    spark_master = os.getenv("SPARK_MASTER")

    if spark_master:
        builder = builder.master(spark_master)
    elif environment == "dev":
        builder = builder.master("local[*]")

    if environment == "dev":
        warehouse_path = PROJECT_ROOT / "hive_warehouse"

        builder = builder.config(
            "spark.sql.warehouse.dir",
            warehouse_path.resolve().as_uri(),
        )

    if storage_backend == "s3":
        # The compatible Hadoop S3A connector must also be available
        # on the Spark classpath. This setting alone does not install it.
        builder = builder.config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )

    return (
        builder
        .enableHiveSupport()
        .getOrCreate()
    )


# --------------------------------------------------
# External table helpers
# --------------------------------------------------

def get_table_location(
    spark: SparkSession,
    table_name: str,
) -> str | None:
    """Return a registered table's location, if the table exists."""

    if not spark.catalog.tableExists(table_name):
        return None

    details = spark.sql(
        f"DESCRIBE EXTENDED {table_name}"
    ).collect()

    for row in details:
        column_name = row["col_name"]

        if (
            column_name
            and column_name.strip().lower() == "location"
        ):
            return row["data_type"]

    return None


def create_external_table(
    spark: SparkSession,
    parquet_location: str,
    environment: str,
) -> None:
    """Register the partitioned Parquet dataset as an external table."""

    # Check that the configured destination contains readable Parquet.
    parquet_df = spark.read.parquet(parquet_location)

    required_columns = {
        "app_id",
        "track_name",
        "country",
    }

    missing_columns = required_columns - set(parquet_df.columns)

    if missing_columns:
        raise ValueError(
            "Processed Parquet is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    row_count = parquet_df.count()

    if row_count == 0:
        raise ValueError(
            "Processed Parquet contains zero rows. "
            "The external table was not created."
        )

    logger.info(
        "Validated Parquet dataset: rows=%d, location=%s",
        row_count,
        parquet_location,
    )

    # Create the database in the current configured catalog.
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}"
    )

    existing_location = get_table_location(
        spark,
        FULL_TABLE_NAME,
    )

    # In UAT and production, do not silently reuse a table
    # pointing to a different environment's data.
    if existing_location is not None and environment != "dev":
        if normalize_location(existing_location) != normalize_location(
            parquet_location
        ):
            raise ValueError(
                f"{FULL_TABLE_NAME} already exists at "
                f"{existing_location}, but the configured location is "
                f"{parquet_location}. Resolve the catalog configuration "
                "before modifying the table."
            )

    # Dev is intentionally repeatable while learning.
    # Dropping the external table removes its catalog registration;
    # it should not delete the underlying Parquet files.
    if environment == "dev":
        spark.sql(
            f"DROP TABLE IF EXISTS {FULL_TABLE_NAME}"
        )

    safe_location = parquet_location.replace("'", "''")

    create_keyword = (
        "CREATE TABLE"
        if environment == "dev"
        else "CREATE TABLE IF NOT EXISTS"
    )

    create_sql = f"""
        {create_keyword} {FULL_TABLE_NAME} (
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
        LOCATION '{safe_location}'
    """

    spark.sql(create_sql)

    # Discover the country=... directories and register partitions.
    spark.sql(
        f"REPAIR TABLE {FULL_TABLE_NAME}"
    )

    # Confirm the registered location.
    actual_location = get_table_location(
        spark,
        FULL_TABLE_NAME,
    )

    if actual_location is None:
        raise RuntimeError(
            f"Could not confirm the location for {FULL_TABLE_NAME}."
        )

    if normalize_location(actual_location) != normalize_location(
        parquet_location
    ):
        raise RuntimeError(
            f"Registered table location mismatch: "
            f"expected={parquet_location}, actual={actual_location}"
        )

    logger.info(
        "External table registered: table=%s, location=%s",
        FULL_TABLE_NAME,
        actual_location,
    )


# --------------------------------------------------
# Job entry point
# --------------------------------------------------

def main() -> int:
    """Create the external table and validate its metadata and data."""

    try:
        config = load_config()

    except (OSError, json.JSONDecodeError, ValueError):
        logging.basicConfig(
            level=logging.ERROR,
            format=DEFAULT_LOG_FORMAT,
        )
        logger.exception("Unable to load project configuration.")
        return 1

    configure_logging(config)

    environment = config["environment"]
    spark = None

    logger.info(
        "Starting external table job: environment=%s, backend=%s",
        environment,
        config["storage"]["backend"],
    )

    try:
        parquet_location = resolve_parquet_location(config)

        logger.info(
            "Configured Parquet location: %s",
            parquet_location,
        )

        spark = create_spark_session(config)
        spark.sparkContext.setLogLevel("WARN")

        create_external_table(
            spark=spark,
            parquet_location=parquet_location,
            environment=environment,
        )

        logger.info("Registered partitions:")

        spark.sql(
            f"SHOW PARTITIONS {FULL_TABLE_NAME}"
        ).show(truncate=False)

        logger.info("Table data preview:")

        spark.sql(
            f"""
            SELECT
                app_id,
                track_name,
                average_user_rating,
                user_rating_count,
                country
            FROM {FULL_TABLE_NAME}
            ORDER BY country
            """
        ).show(truncate=False)

        logger.info("Table listing:")

        spark.sql(
            f"SHOW TABLES IN {DATABASE_NAME}"
        ).show(truncate=False)

        logger.info("Table schema:")

        spark.sql(
            f"DESCRIBE {FULL_TABLE_NAME}"
        ).show(truncate=False)

        logger.info("Extended table metadata:")

        spark.sql(
            f"DESCRIBE EXTENDED {FULL_TABLE_NAME}"
        ).show(truncate=False)

        table_row_count = spark.sql(
            f"SELECT COUNT(*) AS row_count FROM {FULL_TABLE_NAME}"
        ).first()["row_count"]

        logger.info(
            "External table validation completed: row_count=%d",
            table_row_count,
        )

        if table_row_count != spark.read.parquet(
            parquet_location
        ).count():
            raise RuntimeError(
                "The external table row count does not match "
                "the underlying Parquet dataset."
            )

        return 0

    except Exception:
        logger.exception("External table job failed.")
        return 1

    finally:
        if spark is not None:
            try:
                spark.stop()
                logger.info("Spark session stopped.")
            except Exception:
                logger.exception("Failed to stop the Spark session.")


if __name__ == "__main__":
    raise SystemExit(main())
