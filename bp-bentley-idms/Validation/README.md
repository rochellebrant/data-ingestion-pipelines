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
```

---

## Parameters
- tag_names: A sequence of tag names to filter and process.
- target_storage_name: The name of the Azure Blob Storage account.
- target_container_name: The container name in Azure Blob Storage.
- target_storage_key: The access key for the Azure storage account.
- target_file_path: The path to the Parquet file in the container.
- source_database: The name of the source database in Spark SQL.
- source_table: The name of the source table in the database.

---

## Execution Workflow

1. **Azure Blob Storage Configuration**:
   - Configures Spark to access Azure Blob Storage using the provided storage key.

2. **Target Data Loading**:
   - Reads the Parquet data from the specified Azure Blob Storage path into a Spark DataFrame.

3. **Tag Processing**:
   - Filters the DataFrame for the specified tags.
   - For each tag:
     1. Computes the record count in the target data.
     2. Identifies the minimum `ts_utc` value for the tag in the target data.
     3. Executes a SQL query on the source database to fetch the count of records for the tag where `ts_utc` >= the minimum timestamp.
     4. Compares the source and target record counts.
     5. Logs the results, highlighting mismatches.

4. **Error Handling**:
   - Catches and logs exceptions for individual tags to ensure the process continues for the remaining tags.

---

## Example Usage

```scala
processTags(
  tag_names = Seq("Tag1", "Tag2", "Tag3"),
  target_storage_name = "mystorageaccount",
  target_container_name = "mycontainer",
  target_storage_key = "mysecretstoragekey",
  target_file_path = "data/my_parquet_file",
  source_database = "my_database",
  source_table = "my_table"
)
```

---

## Logging

The function provides detailed logs:
- For each tag, it logs:
  - Tag name.
  - Minimum timestamp (`ts_utc`) in the target data.
  - Record counts from both source and target datasets.
  - Match/mismatch status of the counts.
- Errors encountered during processing.

Example Log:
```yaml
Tag1
        Minimum tag time:       2023-01-01T00:00:00Z
        Target count:           100
        Source count:           100
        ✅ Counts match
--------------------------------------------------------------------------------------------------------
```


---

## Error Handling and Debugging

1. **Error Logging**: Any exception during processing of a tag is logged, including the exception message and stack trace.
2. **Debugging**: Use the printed stack trace to troubleshoot issues such as:
   - Incorrect Azure credentials or file paths.
   - Discrepancies in source and target data schemas.
   - SQL query failures.

---

## Notes and Recommendations
- Ensure the schemas of the source and target datasets match for seamless validation.
- Use appropriate Azure storage account permissions to avoid access issues.
- If working with large datasets, consider optimizing the Spark job (e.g., repartitioning data).

---

## License

This code is provided as-is for internal use. Ensure compliance with your organization's policies when handling sensitive data.

