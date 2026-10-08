from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import size, col


def main():
    project_root = Path(__file__).resolve().parents[2]

    input_path = (
        project_root
        / "raw"
        / "api"
        / "app_metadata"
    )

    spark = (
        SparkSession.builder
        .appName("InspectAppleAppMetadata")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading JSON from: {input_path}")
    print()

    # The API responses are formatted JSON objects spanning multiple lines.
    # multiline=True allows Spark to read them correctly.
    df = (
        spark.read
        .option("multiline", "true")
        .json(str(input_path))
    )

    print("=== Schema ===")
    df.printSchema()
    print()

    print("=== Top-level columns ===")
    print(df.columns)
    print()

    print("=== Basic inspection ===")

    # resultCount tells us how many API results were returned.
    # results is the nested array containing the actual app metadata.
    inspection_df = df.select(
        "resultCount",
        size(col("results")).alias("results_array_size"),
        "app_id",
        "country",
    )

    inspection_df.show(truncate=False)

    print("=== Sample raw structure ===")
    df.select("resultCount", "results").show(
        n=2,
        truncate=False,
    )

    spark.stop()


if __name__ == "__main__":
    main()