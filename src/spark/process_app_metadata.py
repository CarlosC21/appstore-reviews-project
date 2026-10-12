
import logging
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit

# Ensure Spark workers use the Python interpreter running this script.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, trim, to_timestamp

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
    """Convert a configured s3:// URI to a Spark s3a:// URI."""

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


def resolve_storage_paths(config: dict) -> tuple[str, str]:
    """Resolve raw input and processed output paths for the environment."""

    storage = config["storage"]
    backend = storage["backend"]

    if backend == "local":
        raw_input_path = (
            PROJECT_ROOT / storage["raw_output_dir"]
        ).resolve()

        processed_output_path = (
            PROJECT_ROOT / storage["processed_output_dir"]
        ).resolve()

        return str(raw_input_path), str(processed_output_path)

    if backend == "s3":
        raw_input_path = to_spark_s3_uri(
            storage["raw_output_uri"]
        )

        processed_output_path = to_spark_s3_uri(
            storage["processed_output_uri"]
        )

        return raw_input_path, processed_output_path

    raise ValueError(f"Unsupported storage backend: {backend}")


# --------------------------------------------------
# Spark session
# --------------------------------------------------

def create_spark_session(config: dict) -> SparkSession:
    """Create a Spark session appropriate for the selected environment."""

    builder = (
        SparkSession.builder
        .appName("ProcessAppleAppMetadata")
    )

    # Use local Spark for development. For UAT/production clusters,
    # allow spark-submit to provide the master configuration.
    spark_master = os.getenv("SPARK_MASTER")

    if spark_master:
        builder = builder.master(spark_master)
    elif config["environment"] == "dev":
        builder = builder.master("local[*]")

    if config["storage"]["backend"] == "s3":
        # Requires the Hadoop S3A connector and compatible dependencies.
        # AWS credentials must be available to the Spark JVM.
        builder = builder.config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark


# --------------------------------------------------
# Processing job
# --------------------------------------------------

def main() -> int:
    """Read raw API JSON, transform the data, and write Parquet."""

    # 1. Load shared and environment-specific configuration.
    try:
        config = load_config()

    except (OSError, ValueError):
        logging.basicConfig(
            level=logging.ERROR,
            format=DEFAULT_LOG_FORMAT,
        )
        logger.exception("Unable to load project configuration.")
        return 1

    configure_logging(config)

    environment = config["environment"]
    backend = config["storage"]["backend"]

    logger.info(
        "Starting app metadata processing: environment=%s, backend=%s",
        environment,
        backend,
    )

    spark = None

    try:
        raw_input_path, output_path = resolve_storage_paths(config)

        logger.info("Raw input path: %s", raw_input_path)
        logger.info("Processed output path: %s", output_path)

        # Validate local input before starting Spark.
        if backend == "local":
            if not Path(raw_input_path).is_dir():
                raise FileNotFoundError(
                    f"Raw input directory does not exist: "
                    f"{raw_input_path}"
                )

        # 2. Create Spark session.
        spark = create_spark_session(config)

        # --------------------------------------------------
        # Step 1: Read raw API JSON.
        # --------------------------------------------------
        logger.info("Reading raw JSON data.")

        raw_df = (
            spark.read
            .option("multiline", "true")
            .option("basePath", raw_input_path)
            .json(raw_input_path)
        )

        required_columns = {"app_id", "country", "results"}
        missing_columns = required_columns - set(raw_df.columns)

        if missing_columns:
            raise ValueError(
                "Raw input is missing required columns: "
                f"{sorted(missing_columns)}. "
                "Check the input directory structure and JSON data."
            )

        logger.info("Raw schema:")
        raw_df.printSchema()

        # --------------------------------------------------
        # Step 2: Explode the results array.
        # --------------------------------------------------
        logger.info("Exploding the API results array.")

        exploded_df = raw_df.select(
            "app_id",
            "country",
            explode("results").alias("app"),
        )

        logger.info("Schema after explode:")
        exploded_df.printSchema()

        # --------------------------------------------------
        # Step 3: Flatten fields and cast data types.
        # --------------------------------------------------
        logger.info("Flattening and transforming application fields.")

        processed_df = exploded_df.select(
            col("app_id").cast("int").alias("app_id"),
            trim(col("country")).alias("country"),

            col("app.trackId").cast("long").alias("track_id"),
            trim(col("app.trackName")).alias("track_name"),
            trim(col("app.artistName")).alias("artist_name"),
            trim(col("app.sellerName")).alias("seller_name"),
            trim(col("app.bundleId")).alias("bundle_id"),

            col("app.primaryGenreId")
            .cast("long")
            .alias("primary_genre_id"),

            trim(col("app.primaryGenreName"))
            .alias("primary_genre_name"),

            col("app.averageUserRating")
            .cast("double")
            .alias("average_user_rating"),

            col("app.userRatingCount")
            .cast("long")
            .alias("user_rating_count"),

            col("app.averageUserRatingForCurrentVersion")
            .cast("double")
            .alias("average_user_rating_current_version"),

            col("app.userRatingCountForCurrentVersion")
            .cast("long")
            .alias("user_rating_count_current_version"),

            col("app.price")
            .cast("double")
            .alias("price"),

            trim(col("app.currency")).alias("currency"),
            trim(col("app.formattedPrice")).alias("formatted_price"),

            to_timestamp(col("app.releaseDate"))
            .alias("release_date"),

            to_timestamp(col("app.currentVersionReleaseDate"))
            .alias("current_version_release_date"),

            trim(col("app.version")).alias("version"),
            trim(col("app.minimumOsVersion")).alias("minimum_os_version"),

            col("app.fileSizeBytes")
            .cast("long")
            .alias("file_size_bytes"),

            trim(col("app.contentAdvisoryRating"))
            .alias("content_advisory_rating"),

            trim(col("app.trackContentRating"))
            .alias("track_content_rating"),

            col("app.isGameCenterEnabled")
            .cast("boolean")
            .alias("is_game_center_enabled"),

            col("app.isVppDeviceBasedLicensingEnabled")
            .cast("boolean")
            .alias("is_vpp_device_based_licensing_enabled"),

            trim(col("app.wrapperType")).alias("wrapper_type"),
        )

        logger.info("Processed schema:")
        processed_df.printSchema()

        # --------------------------------------------------
        # Step 4: Validate processed data.
        # --------------------------------------------------
        row_count = processed_df.count()

        logger.info("Processed row count: %d", row_count)

        if row_count == 0:
            raise ValueError(
                "Processing produced zero rows. Refusing to overwrite "
                "the existing Parquet dataset with empty output."
            )

        logger.info("Sample of processed data:")
        processed_df.show(truncate=False)

        # --------------------------------------------------
        # Step 5: Write partitioned Parquet.
        # --------------------------------------------------
        logger.info(
            "Writing partitioned Parquet data to %s",
            output_path,
        )

        (
            processed_df.write
            .mode("overwrite")
            .partitionBy("country")
            .parquet(output_path)
        )

        logger.info(
            "Parquet write completed successfully: %s",
            output_path,
        )

        return 0

    except Exception:
        logger.exception("App metadata processing failed.")
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
