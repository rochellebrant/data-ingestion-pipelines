# processTags Function

## Overview

The `processTags` function is a Spark-based utility for validating tag data between a target Parquet dataset and a source database table. The function processes specified tag names, verifies the consistency of data between source and target systems, and provides insights into mismatched counts.

## Features
- **Azure Blob Storage Integration**: Reads Parquet files stored in Azure Blob Storage.
- **Tag Filtering**: Filters data for specified tags.
- **Minimum Timestamp Validation**: Identifies the minimum timestamp (`ts_utc`) for each tag in the target data.
- **Source vs. Target Count Comparison**: Validates that record counts for each tag in the target data match those in the source table.
- **Error Handling**: Logs and handles errors gracefully for individual tags during processing.

---

## Prerequisites

1. **Apache Spark**: Ensure Spark is set up and properly configured.
2. **Azure Storage Access**: 
   - A valid Azure storage account and container.
   - A storage key with read access to the target Parquet files.
3. **Source Database Access**: Access to the source database containing the relevant table.
4. **Parquet Data**: The target data must be stored in Parquet format.
5. **Dependencies**: 
   - `org.apache.spark.sql.functions` for Spark SQL operations.

---

## Function Signature

```scala
def processTags(
  tag_names: Seq[String],
  target_storage_name: String,
  target_container_name: String,
  target_storage_key: String,
  target_file_path: String,
  source_database: String,
  source_table: String
): Unit
