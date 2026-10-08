App Store / Google Play Data Engineering Project

Overview

Small end-to-end data engineering project demonstrating three core requirements:

Ingest data from an API

Process data with PySpark

Load processed data into an external Parquet table with appropriate table design

Architecture

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

Project Structure

appstore-reviews-project/
├── raw/
│ ├── AppStore_Data.json
│ ├── GooglePlay_Data.csv
│ └── api/
│ └── app_metadata/
│ ├── app_id=310633997/
│ │ └── country=at/
│ │ └── response.json
│ └── app_id=421997825/
│ └── country=au/
│ └── response.json
├── processed/
│ └── app_metadata/
│ ├── country=at/
│ └── country=au/
├── src/
│ ├── ingestion/
│ │ └── fetch_app_metadata.py
│ └── spark/
│ ├── inspect_apple_api.py
│ ├── process_app_metadata.py
│ ├── validate_app_metadata.py
│ ├── create_app_metadata_table.py
│ └── demonstrate_partition_pruning.py
└── README.md

Technology Stack

Python 3.13

requests

PySpark 4.2.0

Java 17

Parquet

Spark SQL / Hive-compatible catalog

PowerShell

1. API Ingestion

Uses the Apple iTunes Lookup API:

https://itunes.apple.com/lookup

Parameters:

id
country
entity=software

Current app/country inputs:

310633997 / at
421997825 / au

Raw API responses are preserved under raw/api/app_metadata/.

Run:

python .\src\ingestion\fetch_app_metadata.py

2. Spark Processing

process_app_metadata.py:

Reads raw JSON

Explodes results

Selects and flattens relevant fields

Cleans string fields

Casts numeric and Boolean fields

Converts date strings to timestamps

Writes partitioned Parquet

Table grain:

One row = one application in one country

Run:

python .\src\spark\process_app_metadata.py

Output:

processed/app_metadata/
├── country=at/
└── country=au/

3. External Table

The Parquet data is registered as:

appstore.app_metadata

Design:

Table type: External
Format: Parquet
Partition column: country
Location: processed/app_metadata

Create the table:

python .\src\spark\create_app_metadata_table.py

Example query:

SELECT
app_id,
track_name,
average_user_rating,
user_rating_count,
country
FROM appstore.app_metadata;

Partitioning

country was selected because it is low-cardinality and useful for filtering.

Physical layout:

processed/app_metadata/
├── country=at/
└── country=au/

Partition pruning was verified with Spark EXPLAIN. A query filtered on country = 'at' produced a partition filter and targeted the country=at directory.

Bucketing Decision

Bucketing was evaluated but not implemented. The dataset contains only two application records, so bucketing would add complexity without a meaningful performance benefit.

Validation

The processed Parquet data was read back with Spark and validated for:

Schema

Row count

Data contents

Partition values

Physical partition layout

Current result:

Rows: 2
Partitions: at, au

Windows Spark Configuration

This project runs Spark locally on Windows. Before running any Spark job, set these PowerShell environment variables:

$env:HADOOP_HOME = "C:\hadoop\hadoop-win-utils"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"

$env:PYSPARK_PYTHON = (Get-Command python).Source
$env:PYSPARK_DRIVER_PYTHON = (Get-Command python).Source

Verify Hadoop:

Test-Path "$env:HADOOP_HOME\bin\winutils.exe"

Expected:

True

These environment variables are session-specific. Set them again when opening a new PowerShell session before running Spark jobs.

Key Concepts Demonstrated

REST API ingestion

Raw data preservation

Nested JSON processing

PySpark DataFrames

explode

Data cleaning and type casting

Parquet

Partitioned storage

External tables

Hive-compatible metadata

Partition discovery

Partition pruning

SQL querying

Storage design trade-offs

Scope

The project intentionally focuses on the mentor's three requirements:

API ingestion
->
Spark processing
->
External Parquet table

Production orchestration, advanced monitoring, retry frameworks, and other unnecessary infrastructure are outside the scope of this learning project.
