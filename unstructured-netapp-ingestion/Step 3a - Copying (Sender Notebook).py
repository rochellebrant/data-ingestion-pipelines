"""
# Overview
This script effectively manages data processing tasks involving database management, data retrieval, and data processing using Apache Spark in Python. It leverages various utility functions and classes (`Utility`, `DatabaseManager`), along with a function `run_receiver_notebook`, to streamline the workflow and ensure data integrity and efficiency.

This documentation should provide a comprehensive understanding of how the code operates and the purpose of each component involved.

# Detailed Documentation

### 1. Utility Class
The `Utility` class contains several static methods that provide auxiliary functionality for string manipulation, chunk processing, and SQL query generation.
- `print_boundary(num_of_dashes: int)`: Prints a boundary line made up of dashes.
- `remove_leading_and_trailing_slashes(input_string: str) -> str`: Removes leading and trailing forward slash ("/") characters from a string and returns this modified string.
- `determine_num_chunks(df, chunk_size: int) -> int`: Returns the total number of chunks needed to process all rows in a DataFrame.
- `generate_chunk_queries(df, database_name: str, table_name: str, base_condition: str, extra_condition: str, chunk_size: int) -> list`: Returns a list of dynamically generated SQL queries for processing data in each chunk.

### 2. DatabaseManager Class
The `DatabaseManager` class manages database operations such as fetching data, deleting duplicate rows, and retrieving successful records. It is initialized with source and target database/table names and table format.
- Initialization (`__init__`): Initializes with source and target database/table names and table format.
- `fetch_data_to_copy(self, spark, job_group: str, job_order: str, load_type: str) -> tuple`: Constructs and executes a SQL query based on provided parameters to fetch data for copying. Returns a tuple containing a dataframe (containing the results of the executed SQL query), base condition string (used in the SQL query), and extra condition string (used in the query, if any).
- `delete_duplicate_rows_by_latest_copy(self, spark, job_group: str, job_order: str) -> int`: Deletes duplicate rows based on the latest copy timestamp. Returns the number of deleted rows.
- `fetch_latest_successful_copies(self, spark, base_condition: str, start_time_str: str) -> DataFrame`: Retrieves and returns a dataframe of successful records from the specified table.
- `transform_successes_df_for_runlog(self, successes_df: DataFrame) -> DataFrame`: Specifically transforms a successes dataframe to have the same schema as the `unstruct_metadata.runlog_unified` table.

### 3. run_receiver_notebook Function
- This function runs a Databricks notebook with the specified arguments and ensures it runs until completion without being forcibly terminated.

### 4. Main Execution
- **Widget Values:** Retrieves job-related parameters from widgets (assuming this is within a Databricks environment).
- **Execution Flow:**
  - Initialize DatabaseManager:
    - Creates an instance of `DatabaseManager` using the source and target database/table names and table format.
    - The `DatabaseManager` class handles database interactions such as fetching, updating, and deleting records.
  - Fetch Data to Copy:
    - Invokes `database_manager.fetch_data_to_copy` to construct and execute a SQL query based on the provided parameters.
    - Retrieves data from the source table and returns it as a DataFrame along with the base and extra conditions used in the query.
  - Generate Chunk Queries:
    - Uses `Utility.generate_chunk_queries` to divide the DataFrame into manageable chunks.
    - Constructs SQL queries for each chunk to process the data in parallel.
    - Each query is dynamically generated based on the base condition, extra condition, and chunk size.
  - Print Generated Queries:
    - Prints each generated chunk query to the console for logging and debugging purposes.
  - Run Receiver Notebook in Parallel:
    - Executes the `run_receiver_notebook` function for each chunk query in parallel using a `ThreadPoolExecutor`.
    - Passes necessary parameters to the notebook, including SMB client credentials, database/table names, storage account details, folder paths, and the chunk query.
    - Utilizes a maximum of 20 threads to run the notebook concurrently for each chunk query.
  - Delete Duplicate Rows:
    - Calls `database_manager.delete_duplicate_rows_by_latest_copy` to remove redundant rows from the source table based on the latest copy timestamp.
    - Ensures data consistency and prevents duplicate records.
  - Append Successful Copies to Runlog:
    - If the target storage account is the P82 Blob Storage (CGG project), it uses `database_manager.fetch_latest_successful_copies` and `database_manager.transform_successes_df_for_runlog` to retrieve and transform the data to match the schema of `unstruct_metadata.runlog_unified`.
    - The transformed data is then appended to `unstruct_metadata.runlog_unified`, partitioned by year and month.

"""
# # Databricks notebook source
# dbutils.widgets.text('jobGroup', '600')
# dbutils.widgets.text('jobOrder', '1')
# dbutils.widgets.text('fkLoadType', 'SNP')

# dbutils.widgets.text('SMBClientUsername', 'usiphdireporting@bp.com')
# dbutils.widgets.text('SMBClientPassword', 'USIPHDIREPORTING-PWD-KV')
# dbutils.widgets.text('keyVaultName', 'ZNEUDL1P48DEF-kvl01') # "ZNEUDL1P40INGing-kvl00" or "ZNEUDL1P48DEF-kvl01"

# dbutils.widgets.text('sourceDBName', 'unstruct_metadata')
# dbutils.widgets.text('sourceTblName', 'netapp_configuration')
# dbutils.widgets.text('sourceFolderRootPath', 'aadsmbuswp-895a.bp1.ad.bp.com/usw1/hou_group_005/NAX/')

# dbutils.widgets.text('tgtStorageAccount', 'zneudl1p40ingstor01') # "zneusu5p82cggdata" or "zneudl1p40ingstor01"
# dbutils.widgets.text('tgtStorageAccountKeyName', 'accessKey-PIM-zneudl1p40ingstor01') # "accessKey-CGG-zneusu5p82cggdata" or "accessKey-PIM-zneudl1p40ingstor01"
# dbutils.widgets.text('tgtContainer', 'unstructured-data') # "bptocgg"
# dbutils.widgets.text('tgtFolderRootPath', 'Nakika_Data/NetApp/NK_ILX/')

# dbutils.widgets.text('targetDBName', 'unstruct_metadata')
# dbutils.widgets.text('targetTblName', 'netapp_configuration')
# dbutils.widgets.text('runLogTable', 'runLog_unified')
# dbutils.widgets.text('fkTargetFileFormat', 'delta')
%run "./ModuleFunctions"
from pyspark.sql.functions import current_timestamp, substring_index, year, month
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import threading

# Get widget values
JOB_GROUP = dbutils.widgets.get('jobGroup')
JOB_ORDER = dbutils.widgets.get('jobOrder')
LOAD_TYPE = dbutils.widgets.get('fkLoadType')
SMB_CLIENT_NAME = dbutils.widgets.get('SMBClientUsername')
SMB_CLIENT_PWD_NAME = dbutils.widgets.get('SMBClientPassword')
KEY_VAULT = dbutils.widgets.get('keyVaultName')
SOURCE_DB = dbutils.widgets.get('sourceDBName')
SOURCE_TBL = dbutils.widgets.get('sourceTblName')
RUNLOG_TBL = dbutils.widgets.get('runLogTable')
TARGET_STORAGE = dbutils.widgets.get('tgtStorageAccount')
TARGET_STORAGE_KEY_NAME = dbutils.widgets.get('tgtStorageAccountKeyName')
TARGET_CONTAINER = dbutils.widgets.get('tgtContainer')
SOURCE_FOLDER = Utility.remove_leading_and_trailing_slashes(dbutils.widgets.get('sourceFolderRootPath')) # NetApp source folder location being copied, e.g. aadsmbuswp-895a.bp1.ad.bp.com/usw1/hou_group_005/NAX/
TARGET_FOLDER = Utility.remove_leading_and_trailing_slashes(dbutils.widgets.get('tgtFolderRootPath')) # Azure Blob Storage folder path where the data is to land, e.g. Nakika_Data/NetApp/NK_ILX/
TABLE_FORMAT = dbutils.widgets.get('fkTargetFileFormat') # Table format of the target table, e.g. delta, parquet
runReceiverNotebook = 'Step 3b - Copying (Receiver Notebook)'
startTime = datetime.now()
startTimeStr = startTime.strftime('%Y-%m-%d %H:%M:%S')
NUMBER_OF_DASHES = 117
CHUNK_SIZE = 200

# Initialize managers
database_manager = DatabaseManager(spark, SOURCE_DB, SOURCE_TBL, TABLE_FORMAT)

# Fetch data
df, base_condition, extra_condition = database_manager.fetch_data_to_copy(JOB_GROUP, JOB_ORDER, LOAD_TYPE)
dfChunkQueries = Utility.generate_chunk_queries(df, SOURCE_DB, SOURCE_TBL, base_condition, extra_condition, CHUNK_SIZE)

# Print queries
for i, query in enumerate(dfChunkQueries, start=1):
    print(f"Query {i}:\n{query}")
    
Utility.print_boundary(NUMBER_OF_DASHES)

print(f'>>> Running "{runReceiverNotebook}" for {CHUNK_SIZE} query chunks (refer to child notebooks above)')

# Run receiver notebook for each query chunk in parallel. Use a ThreadPoolExecutor with a maximum of 20 workers.
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    futures = [executor.submit(run_receiver_notebook, SMB_CLIENT_NAME, SMB_CLIENT_PWD_NAME, KEY_VAULT, SOURCE_DB, SOURCE_TBL, TARGET_STORAGE, TARGET_STORAGE_KEY_NAME, TARGET_CONTAINER, SOURCE_FOLDER, TARGET_FOLDER, TABLE_FORMAT, df_chunk_query) for df_chunk_query in dfChunkQueries]

    # Wait for all tasks to complete
    concurrent.futures.wait(futures)

Utility.print_boundary(NUMBER_OF_DASHES)

# Delete duplicate rows by latest copy
num_deleted = database_manager.delete_duplicate_rows_by_latest_copy(JOB_GROUP, JOB_ORDER)
# Append successful copies to the runlog table if the job is for the CGG project
if TARGET_STORAGE == 'zneusu5p82cggdata' or TARGET_STORAGE == 'zneudl1p40ingstor01':
    successesDf_ = database_manager.fetch_latest_successful_copies(base_condition, startTimeStr)
    successesDf = database_manager.transform_successes_df_for_runlog(successesDf_)
    successesDf.printSchema()
    display(successesDf)

    successesDf = successesDf.withColumn("jobGroup", col("jobGroup").cast("bigint")).withColumn("jobOrder", col("jobOrder").cast("bigint"))
    successesDf.write.mode('append').partitionBy('_year', '_month').saveAsTable(f'{SOURCE_DB}.{RUNLOG_TBL}')
