# Overview
The `processTags` function is a Spark-based utility for validating tag data between a target Parquet dataset and a source database table. The function processes specified tag names, verifies the consistency of data between source and target systems, and provides insights into mismatched counts.

## Features
- Azure Blob Storage Integration: Reads Parquet files stored in Azure Blob Storage.
- Tag Filtering: Filters data for specified tags.
- Minimum Timestamp Validation: Identifies the minimum timestamp (ts_utc) for each tag in the target data.
- Source vs. Target Count Comparison: Validates that record counts for each tag in the target data match those in the source table.
- Error Handling: Logs and handles errors gracefully for individual tags during processing.
