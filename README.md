# App Store / Google Play Data Engineering Project

## Overview

This project demonstrates an end-to-end data engineering pipeline using the Apple iTunes Lookup API, Python, PySpark, Parquet, and an external Spark SQL table.

The project focuses on three core data engineering requirements:

1. Ingest data from an external API.
2. Process nested JSON data using PySpark.
3. Store processed data as partitioned Parquet and register it as an external table for SQL querying.

The project also demonstrates configuration separation, structured logging, exception handling, data validation, partition pruning, and environment-aware storage design.

**Current status:** The complete local development pipeline has been executed successfully on Windows. AWS deployment and end-to-end UAT/production execution remain to be configured and verified.

## Architecture

```text
Apple iTunes Lookup API
          |
          v
Python requests
          |
          v
Raw JSON
Local filesystem in dev
S3 destination planned for cloud environments
          |
          v
PySpark Processing
  - Read nested JSON
  - Inspect the source schema
  - explode(results)
  - Flatten and clean fields
  - Cast numeric and Boolean types
  - Convert date strings to timestamps
          |
          v
Partitioned Parquet
Partition column: country
          |
          v
External Spark SQL Table
appstore.app_metadata
          |
          v
SQL Queries
Validation
Partition Pruning
```

### Environment-aware architecture

The pipeline uses shared application configuration and environment-specific storage settings.

```text
Shared Configuration
config/app_config.json
          |
          v
Environment Selection
APP_ENV=dev | uat | prod
          |
          v
Environment Configuration
  ├── dev  -> Local filesystem
  ├── uat  -> Configured S3 locations
  └── prod -> Configured S3 locations
          |
          v
Ingestion and Spark Jobs
```

The code is designed to select environment settings without requiring edits to transformation logic. Cloud execution still requires AWS permissions, compatible Spark/Hadoop dependencies, and an appropriately configured table catalog.

## Project Structure

```text
appstore-reviews-project/
├── config/
│   ├── app_config.json
│   └── environments/
│       ├── dev.json
│       ├── uat.json
│       └── prod.json
├── raw/
│   ├── AppStore_Data.json
│   ├── GooglePlay_Data.csv
│   └── api/
│       └── app_metadata/
│           ├── app_id=310633997/
│           │   └── country=at/
│           │       └── response.json
│           └── app_id=421997825/
│               └── country=au/
│                   └── response.json
├── processed/
│   └── app_metadata/
│       ├── country=at/
│       ├── country=au/
│       └── _SUCCESS
├── src/
│   ├── __init__.py
│   ├── configuration/
│   │   ├── __init__.py
│   │   └── config_loader.py
│   ├── ingestion/
│   │   ├── __init__.py
│   │   └── fetch_app_metadata.py
│   └── spark/
│       ├── inspect_apple_api.py
│       ├── process_app_metadata.py
│       ├── validate_app_metadata.py
│       ├── create_app_metadata_table.py
│       └── demonstrate_partition_pruning.py
├── hive_warehouse/
└── README.md
```

The `hive_warehouse/` directory and local Spark catalog are used in development. Generated files and directory contents may vary by execution environment.

## Technology Stack

- Python 3.13
- `requests`
- `boto3` for AWS SDK access
- PySpark 4.2.0
- Java 17
- Apache Parquet
- Spark SQL and Hive-compatible catalog
- Amazon S3 and AWS Glue planned for cloud deployment
- PowerShell
- Windows

Use dependency versions compatible with the installed Java, Spark, Python, and Hadoop runtime.

## Configuration Management

Application and environment configuration are separated from functional code.

### Shared configuration

File: `config/app_config.json`

Contains shared API and logging settings.

Example:

```json
{
  "api": {
    "base_url": "https://itunes.apple.com/lookup",
    "timeout_seconds": 30,
    "apps": [
      {
        "app_id": 310633997,
        "country": "at"
      },
      {
        "app_id": 421997825,
        "country": "au"
      }
    ]
  },
  "logging": {
    "level": "INFO",
    "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
  }
}
```

### Environment configuration

Environment-specific storage settings are stored under `config/environments/`.

Development (`dev.json`) uses local directories:

```json
{
  "storage": {
    "backend": "local",
    "raw_output_dir": "raw/api/app_metadata",
    "processed_output_dir": "processed/app_metadata"
  }
}
```

UAT and production use separate S3 locations. Their configuration files must contain valid `s3://` URIs for both raw and processed output.

Example structure:

```json
{
  "storage": {
    "backend": "s3",
    "raw_output_uri": "s3://YOUR-UAT-BUCKET/raw/api/app_metadata",
    "processed_output_uri": "s3://YOUR-UAT-BUCKET/processed/app_metadata"
  }
}
```

Replace placeholder bucket names before attempting cloud execution. Production must use its own configured locations.

### Shared configuration loader

File: `src/configuration/config_loader.py`

Responsibilities:

- Load shared application settings.
- Select the environment using `APP_ENV`.
- Merge environment-specific storage settings.
- Validate required configuration values.
- Initialize application logging.

The ingestion and Spark scripts import the shared configuration loader instead of independently defining configuration settings.

## 1. API Ingestion

The project uses the Apple iTunes Lookup API:

`https://itunes.apple.com/lookup`

### API parameters

- `id`
- `country`
- `entity=software`

### Current app and country inputs

| App ID      | Country |
| ----------- | ------- |
| `310633997` | `at`    |
| `421997825` | `au`    |

### Raw output

The raw responses are preserved under:

```text
raw/api/app_metadata/
├── app_id=310633997/
│   └── country=at/
│       └── response.json
└── app_id=421997825/
    └── country=au/
        └── response.json
```

### Ingestion features

The ingestion job includes:

- Request timeouts.
- HTTP status validation.
- Exception handling for request failures.
- JSON response validation.
- Local filesystem or configured S3 writes.
- Structured logging.
- Failure summaries and nonzero process exit codes.

### Run ingestion

From the project root:

```powershell
$env:APP_ENV = "dev"

python -m src.ingestion.fetch_app_metadata
```

Expected result: both app responses are retrieved and saved successfully.

## 2. PySpark Processing

File: `src/spark/process_app_metadata.py`

The processing job transforms raw API responses into a curated Parquet dataset.

### Transformations

1. Read raw JSON.
2. Inspect the nested schema.
3. Explode the `results` array.
4. Flatten relevant application attributes.
5. Clean string fields using trimming.
6. Cast numeric and Boolean fields.
7. Convert release-date fields to timestamps.
8. Validate that the resulting dataset contains rows.
9. Write partitioned Parquet.

### Table grain

One row represents one application in one country.

### Run processing

```powershell
python -m src.spark.process_app_metadata
```

### Local output

```text
processed/app_metadata/
├── country=at/
│   └── part-....snappy.parquet
├── country=au/
│   └── part-....snappy.parquet
└── _SUCCESS
```

The `_SUCCESS` marker indicates that the Spark output write completed successfully. Exact Parquet filenames depend on Spark execution.

The processing job uses the configured output destination and stops the Spark session when processing finishes or fails.

## 3. External Parquet Table

File: `src/spark/create_app_metadata_table.py`

The processed Parquet dataset is registered as:

`appstore.app_metadata`

### Table design

| Property             | Design                   |
| -------------------- | ------------------------ |
| Table type           | External                 |
| Format               | Parquet                  |
| Partition column     | `country`                |
| Database             | `appstore`               |
| Table                | `app_metadata`           |
| Development location | `processed/app_metadata` |

The table schema contains application metadata fields such as identifiers, names, ratings, rating counts, pricing information, release dates, software version information, and the country partition column.

### Create the external table

```powershell
python -m src.spark.create_app_metadata_table
```

In development, the script can recreate the table definition to support schema changes. UAT and production require non-destructive catalog management and environment isolation.

The local job has been tested successfully, including partition registration and SQL queries.

### Example query

```sql
SELECT
    app_id,
    track_name,
    average_user_rating,
    user_rating_count,
    country
FROM appstore.app_metadata
ORDER BY country;
```

## 4. Data Validation

File: `src/spark/validate_app_metadata.py`

The validation job reads the processed Parquet dataset and checks:

- Required columns.
- Schema and data types.
- Row count.
- Null country values.
- Expected country values.
- Missing or unexpected country partitions.
- Data contents.

### Run validation

```powershell
python -m src.spark.validate_app_metadata
```

### Last verified local validation

| Check                    | Result     |
| ------------------------ | ---------- |
| Processed rows           | 2          |
| Country values           | `at`, `au` |
| Required columns         | Passed     |
| Null country check       | Passed     |
| Expected partition check | Passed     |

The validation script exits with code `0` when the checks pass and `1` when a failure is detected.

## 5. Partitioning and Partition Pruning

The dataset is partitioned by `country` because the column has a small number of distinct values and is useful for filtering.

### Physical layout

```text
processed/app_metadata/
├── country=at/
└── country=au/
```

The country column is a partition column rather than a regular data column in the physical Parquet partition layout. Spark discovers and exposes the partition values when reading the complete dataset.

### Demonstrating partition pruning

File: `src/spark/demonstrate_partition_pruning.py`

Run:

```powershell
python -m src.spark.demonstrate_partition_pruning
```

The script compares an unfiltered query with a query filtered by country.

Example filter:

```sql
SELECT
    app_id,
    track_name,
    country
FROM appstore.app_metadata
WHERE country = 'at';
```

### Verified physical plan

The filtered query's physical plan included:

```text
PartitionFilters: [
  isnotnull(country),
  (country = at)
]
```

The plan targeted the `country=at` directory, while the unfiltered query had no partition filter.

Partition pruning was successfully demonstrated in the local development environment.

### Bucketing decision

Bucketing was evaluated but not implemented.

The dataset currently contains only two application records. Bucketing would add complexity without a meaningful performance benefit for this data volume.

Partitioning is sufficient to demonstrate the physical table design and partition pruning.

## Logging and Exception Handling

Structured logging is configured through the shared configuration loader.

The ingestion job logs request activity, response status, saved output paths, result counts, and errors.

The Spark jobs log processing progress and report failures through exceptions and nonzero exit codes.

An invalid environment setting was tested:

```powershell
$env:APP_ENV = "invalid"
python -m src.ingestion.fetch_app_metadata
```

The application correctly rejected the unsupported environment and returned exit code `1`. Development mode was restored afterward.

Additional failure testing is still recommended for malformed input, inaccessible storage, and cloud permissions.

## Windows and Local Spark Configuration

The project runs locally on Windows for development.

Before running Spark jobs in a new PowerShell session, configure the Hadoop utilities and Python interpreter.

```powershell
$env:APP_ENV = "dev"

$env:HADOOP_HOME = "C:\hadoop\hadoop-win-utils"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"

$env:PYSPARK_PYTHON = (Get-Command python).Source
$env:PYSPARK_DRIVER_PYTHON = (Get-Command python).Source
```

### Verify Hadoop configuration

```powershell
Test-Path "$env:HADOOP_HOME\bin\winutils.exe"
```

Expected result:

```text
True
```

These environment variables are session-specific. Set them again when opening a new PowerShell session.

The Spark scripts also set the worker and driver Python interpreter to the current Python executable.

## Running the Full Local Pipeline

Run the following commands from the project root.

### 1. Configure development mode

```powershell
$env:APP_ENV = "dev"

$env:HADOOP_HOME = "C:\hadoop\hadoop-win-utils"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"

$env:PYSPARK_PYTHON = (Get-Command python).Source
$env:PYSPARK_DRIVER_PYTHON = (Get-Command python).Source
```

### 2. Run the jobs in order

```powershell
python -m src.ingestion.fetch_app_metadata
```

```powershell
python -m src.spark.process_app_metadata
```

```powershell
python -m src.spark.create_app_metadata_table
```

```powershell
python -m src.spark.validate_app_metadata
```

```powershell
python -m src.spark.demonstrate_partition_pruning
```

Check the exit code immediately after each command:

```powershell
$LASTEXITCODE
```

Expected result: `0` for each successful job.

The table-creation script should run after processing so that it registers the latest Parquet output. Partition-pruning demonstration should run after the external table exists.

### Last verified local run

On October 12, 2026, the pipeline completed successfully in the local development environment:

- Both API requests returned HTTP 200.
- Two application records were processed.
- Partitioned Parquet was written.
- The external table was registered.
- The table returned both application records.
- Validation confirmed two rows and country values `at` and `au`.
- Partition pruning was verified in Spark's physical execution plan.

This verifies local execution; it does not establish that AWS deployment is complete.

## AWS and Environment Deployment Status

The code includes configuration patterns for local storage and S3 URIs. However, the cloud environment remains to be configured and tested.

### Planned cloud architecture

```text
Apple Lookup API
      |
      v
Python ingestion
      |
      v
Amazon S3: Raw JSON
      |
      v
AWS Glue / Spark ETL
      |
      v
Amazon S3: Partitioned Parquet
      |
      v
AWS Glue Data Catalog
      |
      v
SQL querying and validation
```

### Required AWS work

1. Verify AWS CLI access,  , and region.
2. Create or select separate S3 raw and processed destinations for UAT and production.
3. Configure appropriate IAM permissions or execution roles.
4. Configure compatible Spark/Hadoop S3A dependencies for reading and writing S3.
5. Configure an external-table catalog, such as AWS Glue Data Catalog, for cloud execution.
6. Adapt job deployment and catalog settings for each environment.
7. Run and validate the pipeline in AWS, first in UAT and then in production.

S3 URIs and environment configuration files alone do not establish a working cloud deployment. AWS access, Spark dependencies, permissions, and catalog integration must all be verified.

AWS credentials must not be stored in the configuration JSON files.

## Key Concepts Demonstrated

- REST API ingestion.
- Raw data preservation.
- Configuration separation.
- Structured logging and exception handling.
- Nested JSON processing.
- PySpark DataFrames.
- `explode`.
- Data cleaning and type casting.
- Timestamp conversion.
- Parquet storage.
- Partitioned data layout.
- External tables.
- Hive-compatible metadata.
- Partition discovery.
- SQL querying.
- Schema and data validation.
- Partition pruning.
- Storage design trade-offs.
- Environment-aware configuration.
- S3 and AWS deployment design.

## Project Scope

The project began with three core requirements:

```text
API ingestion
      |
      v
PySpark processing
      |
      v
External Parquet table
```

The local implementation has been extended to demonstrate operational reliability, shared configuration, data validation, partition pruning, and environment-aware storage design.

The next milestone is to configure and verify the cloud deployment so the same pipeline can run across development, UAT, and production without modifying its core transformation logic.
