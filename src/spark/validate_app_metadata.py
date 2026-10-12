
import json
import logging
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit

# Ensure Spark uses the Python interpreter running this script.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.configuration.config_loader import (
    DEFAULT_LOG_FORMAT,
    PROJECT_ROOT,
    configure_logging,
    load_config,
)


logger = logging.getLogger(__name__)


# --------------------------------------------------
# Storage path resolution
# --------------------------------------------------

def to_spark_s3_uri(uri: str) -> str:
    """Convert an s3:// URI to the s3a:// format used by Spark."""

    parsed_uri = urlsplit(uri)

    if (
        parsed_uri.scheme != "s3"
        or not parsed_uri.netloc
        or parsed_uri.query
        or parsed_uri.fragment
    ):
        raise ValueError(f"Invalid S3 URI: {uri}")

    path = parsed_uri.path.rstrip("/")

    return f"s3a://{parsed_uri.netloc}{path}"


def resolve_parquet_path(config: dict) -> str:
    """Resolve the processed Parquet path for the current environment."""

    storage = config["storage"]

    if storage["backend"] == "local":
        parquet_path = (
            PROJECT_ROOT / storage["processed_output_dir"]
        ).resolve()

        if not parquet_path.is_dir():
            raise FileNotFoundError(
                f"Processed Parquet directory does not exist: "
                f"{parquet_path}"
            )

        return str(parquet_path)

    if storage["backend"] == "s3":
        return to_spark_s3_uri(
            storage["processed_output_uri"]
        )

    raise ValueError(
        f"Unsupported storage backend: {storage['backend']}"
    )


# --------------------------------------------------
# Spark session
# --------------------------------------------------

def create_spark_session(config: dict) -> SparkSession:
    """Create a Spark session for the current environment."""

    builder = (
        SparkSession.builder
        .appName("ValidateAppleAppMetadata")
    )

    spark_master = os.getenv("SPARK_MASTER")

    if spark_master:
        builder = builder.master(spark_master)
    elif config["environment"] == "dev":
        builder = builder.master("local[*]")

    if config["storage"]["backend"] == "s3":
        # The Hadoop S3A connector must be available on the classpath.
        builder = builder.config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark


# --------------------------------------------------
# Validation
# --------------------------------------------------

def validate_parquet(
    spark: SparkSession,
    parquet_path: str,
    config: dict,
) -> bool:
    """Validate schema, row count, contents, and country partitions."""

    logger.info("Reading Parquet from: %s", parquet_path)

    df = spark.read.parquet(parquet_path)

    # 1. Validate required columns.
    required_columns = {
        "app_id",
        "track_id",
        "track_name",
        "average_user_rating",
        "user_rating_count",
        "country",
    }

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            "Parquet dataset is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    logger.info("Parquet schema:")
    df.printSchema()

    # 2. Validate row count.
    row_count = df.count()

    logger.info("Parquet row count: %d", row_count)

    if row_count == 0:
        raise ValueError(
            "Parquet dataset contains zero rows."
        )

    # 3. Validate country values.
    country_rows = (
        df.select("country")
        .distinct()
        .collect()
    )

    actual_countries = {
        row["country"]
        for row in country_rows
        if row["country"] is not None
    }

    has_null_country = (
        df.filter(col("country").isNull())
        .limit(1)
        .count() > 0
    )

    if has_null_country:
        raise ValueError(
            "Parquet dataset contains rows with a null country."
        )

    expected_countries = {
        app["country"].strip().lower()
        for app in config["api"]["apps"]
    }

    missing_countries = expected_countries - actual_countries

    if missing_countries:
        raise ValueError(
            "Expected country partitions are missing: "
            f"{sorted(missing_countries)}"
        )

    unexpected_countries = actual_countries - expected_countries

    if unexpected_countries:
        raise ValueError(
            "Unexpected country partitions found: "
            f"{sorted(unexpected_countries)}"
        )

    logger.info(
        "Country partitions validated: %s",
        sorted(actual_countries),
    )

    # 4. Display the processed data.
    logger.info("Processed data preview:")
    df.show(truncate=False)

    logger.info("Distinct country values:")
    df.select("country").distinct().orderBy("country").show(
        truncate=False
    )

    logger.info(
        "Parquet validation passed: rows=%d, countries=%s",
        row_count,
        sorted(actual_countries),
    )

    return True


# --------------------------------------------------
# Job entry point
# --------------------------------------------------

def main() -> int:
    """Run the environment-aware Parquet validation job."""

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

    logger.info(
        "Starting Parquet validation: environment=%s, backend=%s",
        config["environment"],
        config["storage"]["backend"],
    )

    spark = None

    try:
        parquet_path = resolve_parquet_path(config)
        spark = create_spark_session(config)

        validate_parquet(
            spark=spark,
            parquet_path=parquet_path,
            config=config,
        )

        return 0

    except Exception:
        logger.exception("Parquet validation failed.")
        return 1

    finally:
        if spark is not None:
            try:
                spark.stop()
                logger.info("Spark session stopped.")
            except Exception:
                logger.exception(
                    "Failed to stop the Spark session."
                )


if __name__ == "__main__":
    raise SystemExit(main())
