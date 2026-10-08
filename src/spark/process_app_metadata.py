import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, trim, to_timestamp


# Make sure Spark workers use the same Python interpreter
# that is running this script.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def main():
    # Project root:
    # appstore-reviews-project/
    #   src/
    #     spark/
    #       process_app_metadata.py
    project_root = Path(__file__).resolve().parents[2]

    input_path = (
        project_root
        / "raw"
        / "api"
        / "app_metadata"
    )

    output_path = (
        project_root
        / "processed"
        / "app_metadata"
    )

    spark = (
        SparkSession.builder
        .appName("ProcessAppleAppMetadata")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading raw JSON from: {input_path}")
    print(f"Writing Parquet to: {output_path}")
    print()

    # ---------------------------------------------------------
    # 1. Read the raw API JSON
    # ---------------------------------------------------------
    raw_df = (
        spark.read
        .option("multiline", "true")
        .json(str(input_path))
    )

    print("=== Raw schema ===")
    raw_df.printSchema()
    print()

    # ---------------------------------------------------------
    # 2. Explode the results array
    # ---------------------------------------------------------
    # results is array<struct<...>>
    # After explode(), each element becomes one row.
    exploded_df = (
        raw_df
        .select(
            "app_id",
            "country",
            explode("results").alias("app")
        )
    )

    print("=== After explode ===")
    exploded_df.printSchema()
    print()

    # ---------------------------------------------------------
    # 3. Select and flatten useful application fields
    # ---------------------------------------------------------
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

        trim(col("app.currency"))
        .alias("currency"),

        trim(col("app.formattedPrice"))
        .alias("formatted_price"),

        to_timestamp(col("app.releaseDate"))
        .alias("release_date"),

        to_timestamp(col("app.currentVersionReleaseDate"))
        .alias("current_version_release_date"),

        trim(col("app.version"))
        .alias("version"),

        trim(col("app.minimumOsVersion"))
        .alias("minimum_os_version"),

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

        trim(col("app.wrapperType"))
        .alias("wrapper_type"),
    )

    # ---------------------------------------------------------
    # 4. Inspect the processed dataset
    # ---------------------------------------------------------
    print("=== Processed schema ===")
    processed_df.printSchema()
    print()

    print("=== Processed row count ===")
    print(processed_df.count())
    print()

    print("=== Processed data ===")
    processed_df.show(truncate=False)
    print()

    # ---------------------------------------------------------
    # 5. Write curated data as Parquet
    # ---------------------------------------------------------
    print("Writing Parquet...")

    (
        processed_df.write
        .mode("overwrite")
        .partitionBy("country")
        .parquet(str(output_path))
    )

    print(f"Parquet written to: {output_path}")
    print()

    spark.stop()


if __name__ == "__main__":
    main()