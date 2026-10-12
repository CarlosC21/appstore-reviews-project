
import json
import logging
import os
import sys

# Use the Python interpreter running this script for Spark.
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

DATABASE_NAME = "appstore"
TABLE_NAME = "app_metadata"
FULL_TABLE_NAME = f"{DATABASE_NAME}.{TABLE_NAME}"


# --------------------------------------------------
# Spark session
# --------------------------------------------------

def create_spark_session(config: dict) -> SparkSession:
    """Create a Spark session for the configured environment."""

    builder = (
        SparkSession.builder
        .appName("DemonstratePartitionPruning")
    )

    spark_master = os.getenv("SPARK_MASTER")

    if spark_master:
        builder = builder.master(spark_master)
    elif config["environment"] == "dev":
        builder = builder.master("local[*]")

    # Match the local development warehouse used by the table job.
    if config["environment"] == "dev":
        warehouse_path = PROJECT_ROOT / "hive_warehouse"

        builder = builder.config(
            "spark.sql.warehouse.dir",
            warehouse_path.resolve().as_uri(),
        )

    if config["storage"]["backend"] == "s3":
        # Requires the Hadoop S3A connector and working AWS credentials.
        builder = builder.config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )

    spark = (
        builder
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# --------------------------------------------------
# Partition pruning demonstration
# --------------------------------------------------

def demonstrate_partition_pruning(
    spark: SparkSession,
    config: dict,
) -> bool:
    """Compare query plans and verify partition pruning."""

    if not spark.catalog.tableExists(FULL_TABLE_NAME):
        raise ValueError(
            f"Table {FULL_TABLE_NAME} does not exist in the "
            "current catalog. Run create_app_metadata_table first."
        )

    # Use the first configured country as the pruning filter.
    target_country = (
        config["api"]["apps"][0]["country"].strip().lower()
    )

    logger.info("Registered partitions:")

    partitions = [
        row[0]
        for row in spark.sql(
            f"SHOW PARTITIONS {FULL_TABLE_NAME}"
        ).collect()
    ]

    for partition in partitions:
        logger.info("Partition: %s", partition)

    expected_partition = f"country={target_country}"

    if expected_partition not in partitions:
        raise ValueError(
            f"Expected partition {expected_partition} was not found. "
            f"Available partitions: {partitions}"
        )

    # --------------------------------------------------
    # Query without a partition filter
    # --------------------------------------------------
    logger.info("Query WITHOUT partition filter")

    df_all = spark.sql(
        f"""
        SELECT
            app_id,
            track_name,
            country
        FROM {FULL_TABLE_NAME}
        """
    )

    df_all.show(truncate=False)

    logger.info("Physical plan WITHOUT partition filter")
    df_all.explain(mode="formatted")

    unfiltered_plan = (
        df_all._jdf.queryExecution().executedPlan().toString()
    )

    logger.info("Unfiltered executed plan:\n%s", unfiltered_plan)

    # --------------------------------------------------
    # Query with a partition filter
    # --------------------------------------------------
    logger.info(
        "Query WITH partition filter: country=%s",
        target_country,
    )

    df_filtered = (
        spark.table(FULL_TABLE_NAME)
        .select(
            "app_id",
            "track_name",
            "country",
        )
        .filter(col("country") == target_country)
    )

    df_filtered.show(truncate=False)

    logger.info("Physical plan WITH partition filter")
    df_filtered.explain(mode="formatted")

    filtered_plan = (
        df_filtered._jdf.queryExecution().executedPlan().toString()
    )

    logger.info("Filtered executed plan:\n%s", filtered_plan)

    # --------------------------------------------------
    # Verify partition pruning
    # --------------------------------------------------
    # Spark should include the country predicate in PartitionFilters.
    normalized_plan = (
        filtered_plan.lower()
        .replace(" ", "")
        .replace("'", "")
        .replace('"', "")
    )

    has_partition_filter = (
        "partitionfilters:" in normalized_plan
        and "partitionfilters:[]" not in normalized_plan
        and f"={target_country}" in normalized_plan
    )

    if not has_partition_filter:
        raise RuntimeError(
            "Partition pruning could not be verified in the "
            "filtered query's physical plan. Inspect the executed "
            "plan and confirm that country is a partition column."
        )

    logger.info(
        "Partition pruning verified for country=%s.",
        target_country,
    )

    # --------------------------------------------------
    # Display table definition
    # --------------------------------------------------
    logger.info("External table definition:")

    spark.sql(
        f"SHOW CREATE TABLE {FULL_TABLE_NAME}"
    ).show(truncate=False)

    return True


# --------------------------------------------------
# Job entry point
# --------------------------------------------------

def main() -> int:
    """Run the partition pruning demonstration."""

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
        "Starting partition pruning demonstration: "
        "environment=%s, backend=%s",
        config["environment"],
        config["storage"]["backend"],
    )

    spark = None

    try:
        spark = create_spark_session(config)

        demonstrate_partition_pruning(
            spark=spark,
            config=config,
        )

        logger.info("Partition pruning demonstration completed.")
        return 0

    except Exception:
        logger.exception(
            "Partition pruning demonstration failed."
        )
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
