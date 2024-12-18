"""
# Overview
This script is used to automate the process of transferring data from an SMB file share to Azure Blob Storage using Apache Spark. It includes two main classes, `DatabaseManager` and `SMBtoAzureBlobCopyManager`, along with a utility class `Utility`. The DatabaseManager class handles database operations, while the SMBtoAzureBlobCopyManager class manages the file transfer process. The script also sets up parameters and configurations needed for these operations.

This documentation should provide a comprehensive understanding of how the code operates and the purpose of each component involved.

# Detailed Documentation

### 1. `Utility`
The `Utility` class provides static methods for various utility functions.
- `print_boundary(num_of_dashes: int)`: Prints a boundary line made up of dashes.
- `remove_leading_and_trailing_slashes(input_string: str) -> str`: Removes leading and trailing forward slash ("/") characters from a string and returns this modified string.

### 2. DatabaseManager Class
The `DatabaseManager`  class handles database operations such as appending data to a table, fetching data, and deleting duplicate rows.
- Initialization (`__init__`): Initializes with source and target database/table names and table format.
- `append_dataframe(self, df: Dataframe)`: Appends data to the partitioned target table in the specified format.
- `fetch_data_to_copy(self, spark, job_group: str, job_order: str, load_type: str) -> tuple`: Constructs and executes a SQL query based on provided parameters to fetch data for copying. Returns a tuple containing a dataframe (containing the results of the executed SQL query), base condition string (used in the SQL query), and extra condition string (used in the query, if any).
-`get_base_condition_to_copy(self, job_group, job_order)`: Constructs the base condition for fetching data.
-`get_extra_condition_to_copy(self, load_type)`: Constructs the extra condition based on the load type.
- `delete_duplicate_rows_by_latest_copy(self, spark, job_group: str, job_order: str) -> int`: Deletes duplicate rows based on the latest copy timestamp. Returns the number of deleted rows.
- `fetch_latest_successful_copies(self, spark, base_condition: str, start_time_str: str) -> DataFrame`: Retrieves and returns a dataframe of successful records from the specified table.
- `transform_successes_df_for_runlog(self, successes_df: DataFrame) -> DataFrame`: Specifically transforms a successes dataframe to have the same schema as the `unstruct_metadata.runlog_unified` table.

### 3. `SMBtoAzureBlobCopyManager` Class
The SMBtoAzureBlobCopyManager class manages the process of copying files from an SMB share to Azure Blob storage.
- `upload_file_and_log_result(self, smb_file_path)`: Copies a file from the SMB share to Azure Blob storage.
- `handle_upload_result(self, smb_file_path, azure_blob_url, success, error_message=None)`: Handles the result of the file upload process.
- `upload_updates_to_table(self)`: Uploads the updates to the database table.
- `copy_all_files_to_azure(self, df)`: Copies all files to Azure Blob storage and handles intermediate saves.
- `final_uploads_to_table(self)`: Handles the final upload of updates to the database table.


### 4. Main Execution
- **Widget Values:** Retrieves parameters such as SMB client credentials, database/table names, storage account details, and folder paths from Databricks widgets (assuming this is within a Databricks environment).
- **Execution Flow:**
  - Initialize DatabaseManager:
    - Creates an instance of `DatabaseManager` using the source and target database/table names and table format.
    - The `DatabaseManager` class manages database interactions such as fetching data to copy, appending data to tables, and deleting duplicate rows.
  - Execute SQL Query:
    - Executes the SQL query provided by the `dfChunkQuery` widget to retrieve data from the source table.
    - The query result is stored in a DataFrame with an additional `status` column initialized to `None`.
    - Displays the DataFrame for verification.
  - Initialize `SMBtoAzureBlobCopyManager`:
    - Creates an instance of `SMBtoAzureBlobCopyManager` using the source and target folder paths, Azure Blob storage account details, SMB client credentials, and the `DatabaseManager` instance.
    - The `SMBtoAzureBlobCopyManager` class handles the process of copying files from the SMB share to Azure Blob storage and updating the database accordingly.
  - Process All Files:
    - Invokes the `copy_all_files_to_azure` method of `SMBtoAzureBlobCopyManager` with the DataFrame obtained from the SQL query.
    - This method:
      - Converts the DataFrame to a dictionary of dictionaries where each key is the `sourceFilePath` and the value is a dictionary of the remaining row data.
      - Iterates through each file path in the dictionary, calling `upload_file_and_log_result` to copy the file from the SMB share to Azure Blob storage.
      - Tracks the upload status and handles any errors that occur during the upload process.
      - Periodically uploads updates to the database table to ensure data consistency.
      - After processing all files, performs a final upload of any remaining updates to the database table.
  - Upload Successful Copies to Database:
    - The `copy_all_files_to_azure` method updates the database with the status of each file (success or failure) along with any error messages.
    - Ensures that the database reflects the latest state of the file transfer process, including any failures and their reasons.
"""

%run "./ModuleFunctions"
# Read parameters
SMB_CLIENT_NAME = dbutils.widgets.get('SMBClientUsername')
SMB_CLIENT_PWD_NAME = dbutils.widgets.get('SMBClientPassword')
KEY_VAULT = dbutils.widgets.get('keyVaultName')
SOURCE_DB = dbutils.widgets.get('sourceDBName')
SOURCE_TBL = dbutils.widgets.get('sourceTblName')
TARGET_STORAGE = dbutils.widgets.get('tgtStorageAccount')
TARGET_STORAGE_KEY_NAME = dbutils.widgets.get('tgtStorageAccountKeyName')
TARGET_CONTAINER = dbutils.widgets.get('tgtContainer')
SOURCE_FOLDER = Utility.remove_leading_and_trailing_slashes(dbutils.widgets.get('sourceFolderRootPath'))
TARGET_FOLDER = Utility.remove_leading_and_trailing_slashes(dbutils.widgets.get('tgtFolderRootPath'))
TABLE_FORMAT = dbutils.widgets.get('fkTargetFileFormat')
DF_CHUNK_QUERY = dbutils.widgets.get('query')
SMB_CLIENT_PWD = dbutils.secrets.get(KEY_VAULT, SMB_CLIENT_PWD_NAME)

# Configure target Azure Blob Storage Account
TARGET_STORAGE_ACCESS_KEY = dbutils.secrets.get(scope = KEY_VAULT, key = TARGET_STORAGE_KEY_NAME)
BLOB_SERVICE_CLIENT = BlobServiceClient(account_url = f'https://{TARGET_STORAGE}.blob.core.windows.net', credential = TARGET_STORAGE_ACCESS_KEY)
CONTAINER_CLIENT = BLOB_SERVICE_CLIENT.get_container_client(TARGET_CONTAINER)                          

# Create a dictionary to store widget variables
widget_variables = {
    'SMBClientUsername': SMB_CLIENT_NAME,
    'SMBClientPassword': SMB_CLIENT_PWD_NAME,
    'keyVaultName': KEY_VAULT,
    'sourceDBName': SOURCE_DB,
    'sourceTblName': SOURCE_TBL,
    'tgtStorageAccount': TARGET_STORAGE,
    'tgtStorageAccountKeyName': TARGET_STORAGE_KEY_NAME,
    'tgtContainer': TARGET_CONTAINER,
    'sourceFolderRootPath': SOURCE_FOLDER,
    'tgtFolderRootPath': TARGET_FOLDER,
    'fkTargetFileFormat': TABLE_FORMAT,
    'dfChunkQuery': DF_CHUNK_QUERY
}

# Print all widget variables
for key, value in widget_variables.items():
    print(f"{key}: {value}")

# Initialize DatabaseManager
database_manager = DatabaseManager(spark, SOURCE_DB, SOURCE_TBL, TABLE_FORMAT)

# Execute the query and display the DataFrame
df = spark.sql(DF_CHUNK_QUERY).withColumn("status", lit(None)).drop('rn')
display(df)
# Initialize SMBtoAzureBlobCopyManager
file_processor = SMBtoAzureBlobCopyManager(
    source_folder=SOURCE_FOLDER,
    target_folder=TARGET_FOLDER,
    target_storage=TARGET_STORAGE,
    target_container=TARGET_CONTAINER,
    smb_client_name=SMB_CLIENT_NAME,
    smb_client_pwd=SMB_CLIENT_PWD,
    container_client=CONTAINER_CLIENT,
    database_manager=database_manager
)

# Process all files
file_processor.copy_all_files_to_azure(df)
