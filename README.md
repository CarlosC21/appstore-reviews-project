# App Store / Google Play Data Engineering Project

## Overview

Small end-to-end data engineering project demonstrating three core requirements:

1. Ingest data from an API
2. Process data with PySpark
3. Load processed data into an external Parquet table with appropriate table design

## Architecture

```text
Apple Lookup API
      |
      v
Python requests
      |
      v
Raw JSON
      |
      v
PySpark
  - Read JSON
  - Inspect nested schema
  - explode(results)
  - Flatten and clean fields
  - Cast data types
  - Convert dates to timestamps
      |
      v
Partitioned Parquet
      |
      v
External Spark/Hive Table
      |
      v
SQL
```

## Project Structure

```text
appstore-reviews-project/
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
│       └── country=au/
├── src/
│   ├── ingestion/
│   │   └── fetch_app_metadata.py
│   └── spark/
│       ├── inspect_apple_api.py
│       ├── process_app_metadata.py
│       ├── validate_app_metadata.py
│       ├── create_app_metadata_table.py
│       └── demonstrate_partition_pruning.py
└── README.md
```

## Technology Stack

* Python 3.13
* `requests`
* PySpark 4.2.0
* Java 17
* Parquet
* Spark SQL / Hive-compatible catalog
* PowerShell
* Windows

## 1. API Ingestion

The project uses the Apple iTunes Lookup API:

```text
https://itunes.apple.com/lookup
```

### API Parameters

```text
id
country
entity=software
```

### Current App / Country Inputs

```text
310633997 / at
421997825 / au
```

Raw API responses are preserved under:

```text
raw/api/app_metadata/
```

### Run the Ingestion Job

```powershell
python .\src\ingestion\fetch_app_metadata.py
```

## 2. Spark Processing

The `process_app_metadata.py` job performs the following transformations:

* Reads raw JSON
* Explodes the `results` array
* Selects and flattens relevant fields
* Cleans string fields
* Casts numeric and Boolean fields
* Converts date strings to timestamps
* Writes partitioned Parquet

### Table Grain

```text
One row = one application in one country
```

### Run the Processing Job

```powershell
python .\src\spark\process_app_metadata.py
```

### Output

```text
processed/app_metadata/
├── country=at/
└── country=au/
```

## 3. External Table

The processed Parquet data is registered as:

```text
appstore.app_metadata
```

### Table Design

```text
Table type:       External
Format:           Parquet
Partition column: country
Location:         processed/app_metadata
```

### Create the External Table

```powershell
python .\src\spark\create_app_metadata_table.py
```

### Example Query

```sql
SELECT
    app_id,
    track_name,
    average_user_rating,
    user_rating_count,
    country
FROM appstore.app_metadata;
```

## Partitioning

The `country` column was selected as the partition column because it is low-cardinality and useful for filtering.

### Physical Layout

```text
processed/app_metadata/
├── country=at/
└── country=au/
```

Partition pruning was verified with Spark `EXPLAIN`.

A query filtered on:

```sql
WHERE country = 'at'
```

produced a partition filter and targeted the `country=at` directory rather than scanning all partitions.

## Bucketing Decision

Bucketing was evaluated but not implemented.

The dataset currently contains only two application records, so bucketing would add complexity without providing a meaningful performance benefit.

For this dataset, partitioning is sufficient to demonstrate physical table design and partition pruning.

## Validation

The processed Parquet data was read back with Spark and validated for:

* Schema
* Row count
* Data contents
* Partition values
* Physical partition layout

### Current Validation Result

```text
Rows: 2
Partitions: at, au
```

## Windows Spark Configuration

This project runs Spark locally on Windows.

Before running any Spark job, set the following PowerShell environment variables:

```powershell
$env:HADOOP_HOME = "C:\hadoop\hadoop-win-utils"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"

$env:PYSPARK_PYTHON = (Get-Command python).Source
$env:PYSPARK_DRIVER_PYTHON = (Get-Command python).Source
```

### Verify Hadoop Configuration

Run:

```powershell
Test-Path "$env:HADOOP_HOME\bin\winutils.exe"
```

Expected result:

```text
True
```

These environment variables are session-specific.

When opening a new PowerShell session, set them again before running Spark jobs.

## Key Concepts Demonstrated

* REST API ingestion
* Raw data preservation
* Nested JSON processing
* PySpark DataFrames
* `explode`
* Data cleaning and type casting
* Parquet
* Partitioned storage
* External tables
* Hive-compatible metadata
* Partition discovery
* Partition pruning
* SQL querying
* Storage design trade-offs

## Scope

The project intentionally focuses on three core requirements:

```text
API ingestion
    ->
Spark processing
    ->
External Parquet table
```
