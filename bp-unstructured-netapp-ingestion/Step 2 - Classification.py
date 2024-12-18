"""
# Overview
This script effectively manages data processing tasks involving exclusion criteria, classification, and database operations using Apache Spark in Python. It leverages various utility functions and classes (`Utility`, `ExclusionManager`, `Classifier`, and `DatabaseManager`) that handle different aspects of data validation, exclusion management, classification, and database operations to streamline the workflow and ensure data integrity and efficiency.

This documentation should provide a comprehensive understanding of how the code operates and the purpose of each component involved.

# Detailed Documentation

### 1. Utility Class
The `Utility` class provides utility functions used throughout the script.
- `print_boundary(num_of_dashes: int)`: Prints a boundary line made up of dashes.

### 2. ExclusionManager Class
The `ExclusionManager` class manages exclusion criteria retrieval from a database.
- **Initialization** (`__init__`): Initializes with Spark session (`spark`), `job_group`, and `job_order`.
-  `get_selected_column(query: str)`: Extracts the selected column from a SQL query string.
- `get_exclusion_user_variable(userInput)`: Retrieves exclusion values based on user input, executing SQL queries if necessary.
- **Methods for exclusion types:** (`get_file_extns_to_exclude, get_file_names_to_exclude, get_file_paths_to_exclude, get_folder_paths_to_exclude`): Fetch exclusion criteria from specific database tables using SQL queries.

### 3. Classifier Class
The Classifier class categorizes files and folders based on exclusion criteria.
- **Initialization** (`__init__`): Initializes with exclusion criteria (`exclusions` dictionary).
- `classify_files_and_folders(rows)`: Classifies files and folders based on exclusion criteria.
- **Static methods:**
  - `item_in_collection(item, collection)`: Checks if an item is in a collection.
  - `term_in_string(terms, string)`: Checks if any term in a collection is found in a string.
  - `has_prefix(path, prefixes)`: Checks if a path contains any specified prefixes.

### 4. DatabaseManager Class
The `DatabaseManager` class manages database operations such as fetching data, deleting duplicate rows, and retrieving successful records. It is initialized with source and target database/table names and table format.
- Initialization (`__init__`): Initializes with source and target database/table names and table format.
- `fetch_data_to_classify(spark, job_group, job_order)`: Fetches data from the source database to classify based on job group and job order.
- `update_data_to_classify(spark, df, classification_dict)`: Updates data in the DataFrame (`df`) based on classification results (`classification_dict`).
- `append_dataframe(df)`: Appends data from the DataFrame to the partitioned target table.
- `delete_duplicate_rows_by_latest_exclusion(spark, job_group, job_order, start_time_str)`: Deletes duplicate rows from the source table based on the latest exclusion timestamp.

### 5. Main Execution
- **Widget Values:** Retrieves job-related parameters from widgets (assuming this is within a Databricks environment).
- **Execution Flow:**
  - Prints boundaries and logs job parameters using `Utility.print_boundary`.
  - Initializes `ExclusionManager` and `DatabaseManager`.
  - Retrieves exclusion criteria using methods from `ExclusionManager`.
  - Fetches data from the source database using `DatabaseManager.fetch_data_to_classify`.
  - Classifies files and folders using `Classifier`.
  - Updates the DataFrame with classification results using `DatabaseManager.update_data_to_classify`.
  - Appends updated data to the target table using `DatabaseManager.append_dataframe`.
  - Deletes redundant rows from the source table using `DatabaseManager.delete_duplicate_rows_by_latest_exclusion`.
"""

# dbutils.widgets.text('jobGroup', '600')
# dbutils.widgets.text('jobOrder', '1')
# dbutils.widgets.text('sourceDBName', 'unstruct_metadata')
# dbutils.widgets.text('sourceTblName', 'netapp_configuration')
# dbutils.widgets.text('fkTargetFileFormat', 'delta')
%run "./ModuleFunctions"
# Read widget values
JOB_GROUP = dbutils.widgets.get('jobGroup')
JOB_ORDER = dbutils.widgets.get('jobOrder')
SOURCE_DB = dbutils.widgets.get('sourceDBName')
SOURCE_TBL = dbutils.widgets.get('sourceTblName')
TABLE_FORMAT = dbutils.widgets.get('fkTargetFileFormat')

# Record the start time
START_TIME = datetime.now()
START_TIME_STR = START_TIME.strftime("%Y-%m-%d %H:%M:%S")

# Constants
NUMBER_OF_DASHES = 90
Utility.print_boundary(NUMBER_OF_DASHES)
print(f'>>> Assigning variables:')
print(f"\tjobGroup:       {JOB_GROUP}")
print(f"\tjobOrder:       {JOB_ORDER}")
print(f"\tsourceDBName:   {SOURCE_DB}")
print(f"\tsourceTblName:  {SOURCE_TBL}")
print(f"\ttableFormat:    {TABLE_FORMAT}")
print(f"\tstartTime:      {START_TIME}")
Utility.print_boundary(NUMBER_OF_DASHES)

# Initialize managers
exclusion_manager = ExclusionManager(spark, JOB_GROUP, JOB_ORDER)
database_manager = DatabaseManager(spark, SOURCE_DB, SOURCE_TBL, TABLE_FORMAT)

print(f'>>> Setting file extensions, file names, file paths, and folder paths to exclude')
fileExtnsToExclude = exclusion_manager.get_file_extns_to_exclude(JOB_GROUP, JOB_ORDER)
fileNamesToExclude = exclusion_manager.get_file_names_to_exclude(JOB_GROUP, JOB_ORDER)
filePathsToExclude = exclusion_manager.get_file_paths_to_exclude(JOB_GROUP, JOB_ORDER)
folderPathsToExclude = exclusion_manager.get_folder_paths_to_exclude(JOB_GROUP, JOB_ORDER)
Utility.print_boundary(NUMBER_OF_DASHES)

print(f'>>> Fetching data')
df = database_manager.fetch_data_to_classify(JOB_GROUP, JOB_ORDER)
Utility.print_boundary(NUMBER_OF_DASHES)

print(f'>>> Classifying files and folders')
rows = df.collect()
exclusions = {
    'filePathsToExclude': filePathsToExclude,
    'fileExtnsToExclude': fileExtnsToExclude,
    'fileNamesToExclude': fileNamesToExclude,
    'folderPathsToExclude': folderPathsToExclude
}
classifier = Classifier(exclusions)
classification_dict = classifier.classify_files_and_folders(rows)

df = database_manager.update_data_to_classify(df, classification_dict)
Utility.print_boundary(NUMBER_OF_DASHES)

print(f'>>> Appending updated rows to {SOURCE_DB}.{SOURCE_TBL}')
database_manager.append_dataframe(df)
Utility.print_boundary(NUMBER_OF_DASHES)

print(f'>>> Deleting redundant duplicate rows:')
database_manager.delete_duplicate_rows_by_latest_exlusion(JOB_GROUP, JOB_ORDER, START_TIME_STR)
Utility.print_boundary(NUMBER_OF_DASHES)
display(spark.sql(f'SELECT * FROM {SOURCE_DB}.{SOURCE_TBL} WHERE jobGroup = {JOB_GROUP} and jobOrder = {JOB_ORDER}'))
