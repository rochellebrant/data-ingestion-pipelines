import os
import re
from datetime import datetime, timedelta

import smbclient
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, 
    substring_index, year, month
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    TimestampType, LongType, IntegerType
)
%md
# Classes
%md
#### Schema
class Schema:
    '''
    The Schema class defines the schema structures for control and failure tables used in the NetAppListingManager class. These schemas are specified using PySpark's StructType and StructField for type enforcement and data consistency.
    '''
    # The schema for the control table, which stores metadata of files listed from the SMB share.
    CONTROL_TABLE_SCHEMA = StructType([
        StructField('sourceFilePath', StringType(), False),
        StructField('jobGroup', IntegerType(), False),
        StructField('jobOrder', IntegerType(), False),
        StructField('isAtSource', StringType(), False),
        StructField('fileSizeMB', StringType(), False),
        StructField('createdTimeStamp', TimestampType(), False),
        StructField('modifiedTimeStamp', TimestampType(), False),
        StructField('listingTimeStamp', TimestampType(), False),
        StructField('toBeExcluded', StringType(), True),
        StructField('exclusionReason', StringType(), True),
        StructField('exclusionTimeStamp', TimestampType(), True),
        StructField('targetFilePath', StringType(), True),
        StructField('status', StringType(), True),
        StructField('copyFailReason', StringType(), True),
        StructField('copyStatusTimeStamp', TimestampType(), True)
    ])

    # The schema for the failure table, which stores information about failed attempts to list files from the SMB share.
    FAILURE_TABLE_SCHEMA = StructType([
        StructField('jobGroup', IntegerType(), False),
        StructField('jobOrder', IntegerType(), False),
        StructField('folderPath', StringType(), False),
        StructField('Error', StringType(), False),
        StructField('TimeStamp', TimestampType(), False)
    ])
%md
#### SMBFileManager
class SMBFileManager:
  '''
  The SMBFileManager class provides methods to interact with files on an SMB (Server Message Block) share. It is responsible for fetching file metadata such as size, modification time, and creation time.

  Attributes:
    smb_client_name (str): The username for the SMB client.
    smb_client_pwd (str): The password for the SMB client.
  '''
  def __init__(self, smb_client_name, smb_client_pwd):
    self.smb_client_name = smb_client_name
    self.smb_client_pwd = smb_client_pwd

  def fetch_smb_file_metadata(self, item_path):
    '''
    Fetches metadata for a file located at the specified SMB path. The metadata includes the file size in megabytes, the last modified datetime, and the creation datetime.

    Parameters:
      item_path (str): The path to the file for which metadata is to be fetched.

    Returns:
      size_MB (int): The size of the file in megabytes.
      modified_datetime (datetime): The datetime when the file was last modified.
      creation_datetime (datetime): The datetime when the file was created.
    '''
    # Retrieves the file size in bytes and converts it to megabytes.
    size_MB = int(smbclient.path.getsize(item_path, username=self.smb_client_name, password=self.smb_client_pwd) / 1000000)

    # Retrieves the last modification time in seconds since the epoch and converts it to a datetime object.
    modified_time = smbclient.path.getmtime(item_path, username=self.smb_client_name, password=self.smb_client_pwd)
    modified_datetime = datetime.fromtimestamp(modified_time)

    # Retrieves the creation time in seconds since the epoch and converts it to a datetime object.
    creation_time = smbclient.path.getctime(item_path, username=self.smb_client_name, password=self.smb_client_pwd)
    creation_datetime = datetime.fromtimestamp(creation_time)
    
    return size_MB, modified_datetime, creation_datetime
%md
#### NetAppListingManager
%md
#### Utility
class Utility:
    @staticmethod
    def print_boundary(num_of_dashes: int):
        '''
        Prints a boundary line made up of a specified number of dash ("-") characters.

        Parameters:
            num_of_dashes (int): The number of dash characters to print.

        Example:
            If num_of_dashes is 5, the method prints "-----".
        '''
        print('-' * num_of_dashes)

    @staticmethod
    def remove_leading_and_trailing_slashes(input_string: str) -> str:
        '''
        Removes leading and trailing forward slash ("/") characters from a string.

        Parameters:
            input_string (str): The string from which leading and trailing slashes are to be removed.

        Returns:
            str: The modified string with leading and trailing slashes removed.

        Example:
            If input_string is "/example/string/", the function returns "example/string".
        '''
        return input_string.rstrip('/').lstrip('/')
    
    
    @staticmethod
    def determine_num_chunks(df, chunk_size):
        '''
        Determines the number of chunks needed to process all rows in a DataFrame.

        Parameters:
            df (DataFrame): The DataFrame whose rows will be processed.
            chunk_size (int): The size of each chunk (how many rows a chunk will contain).

        Returns:
            int: The total number of chunks needed to process all rows.
        '''
        total_rows = df.count()
        num_chunks = (total_rows + chunk_size - 1) // chunk_size
        return num_chunks
    
    @staticmethod
    def generate_chunk_queries(df, database_name, table_name, base_condition, extra_condition, chunk_size):
        '''
        Generates SQL queries dynamically for processing data in chunks.

        A SQL query string is dynamically constructed for each chunk.
        An inner query generates a row number (rn) for each row in the specified table ({database_name}.{table_name}), ordered by sourceFilePath.
        The inner query filters rows based on base_condition and extra_condition.
        The outer query selects rows where rn falls within the current chunk's range (rn > {offset} AND rn <= {offset + chunk_size}).

        Parameters:
            df (DataFrame): The DataFrame whose rows will be processed.
            database_name (str): The name of the database.
            table_name (str): The name of the table.
            base_condition (str): The base condition for filtering rows.
            extra_condition (str): The extra condition for filtering rows.
            chunk_size (int): The size of each chunk (how many rows a chunk will contain).

        Returns:
            list: A list of dynamically generated SQL queries for each chunk.
        '''
        num_chunks = Utility.determine_num_chunks(df, chunk_size)
        df_chunk_queries = []
        for chunk in range(num_chunks):
            offset = chunk * chunk_size
            query = f'''SELECT tmp.* FROM
            \t(SELECT *, row_number() OVER (ORDER BY sourceFilePath) AS rn
            \tFROM {database_name}.{table_name}
            \tWHERE {base_condition} {extra_condition})
            AS tmp WHERE rn > {offset} AND rn <= {offset + chunk_size}'''
            df_chunk_queries.append(query)
        return df_chunk_queries
    
    @staticmethod
    def split_dict_by_key_value(input_dict, key, value=None):
        '''
        Splits the input dictionary into two dictionaries based on the presence of a specific key-value pair.
    
        Parameters:
            input_dict (dict): The dictionary to be split.
            key (str): The key to check in the dictionary.
            value: The value to compare against. Default is None.
            
        Returns:
            tuple: A tuple containing two dictionaries. The first dictionary contains
                key-value pairs where the specified key has the specified value, and the second
                dictionary contains key-value pairs where the specified key does not have the specified value.
        '''
        match_dict = {k: v for k, v in input_dict.items() if v.get(key) == value}
        non_match_dict = {k: v for k, v in input_dict.items() if v.get(key) != value}
        return non_match_dict, match_dict
    
    @staticmethod
    def extract_file_name(path):
        '''
        UDF to extract the file name from a given file path.

        Parameters:
            path (str): The full file path from which the file name will be extracted.

        Returns:
            str: The extracted file name.
        '''
        return substring_index(path, '/', -1)
    
    @staticmethod
    def upload_blob_to_container(containerClient, azureBlobName, fileContent):
        '''
        Uploads the content of a file to a blob in an Azure Blob Storage container.

        Parameters:
            containerClient (azure.storage.blob.ContainerClient): An instance of the Azure Blob Storage container client.
            azureBlobName (str): The name of the blob in Azure Blob Storage where the file content will be uploaded.
            fileContent (bytes): The content of the file to be uploaded to the blob, provided as bytes.

        Returns:
            bool: 
                - True if the blob upload was successful.
                - False if the upload failed.
        '''
        blobClient = containerClient.get_blob_client(azureBlobName)
        blobClient.upload_blob(fileContent, overwrite=True)
        return blobClient.exists()
    
    @staticmethod
    def get_root_relative_path(path, rootPath):
        '''
        Returns the relative path with respect to the specified root path.
            
            Args:
                path (str): The full path for which the relative path is to be determined.
                rootPath (str): The root path against which the relative path is calculated.
                
            Returns:
                str: The relative path from the root path.
                
            Example:
                If full path is "wasbs://container@storage.blob.core.windows.net/Folder1/file.txt"
                and root path is "wasbs://container@storage.blob.core.windows.net/",
                the function returns "Folder1/file.txt".
        '''
        try:
            return Utility.remove_leading_and_trailing_slashes(path[len(rootPath):])
        except Exception as e:
            print(f'⚠️ Exception in get_root_relative_path() function: {e}.\n')

    @staticmethod
    def convert_dict_to_dataframe(data_dict, target_schema):
        '''
        Converts a dictionary of dictionaries into a Spark DataFrame and prepares it for appending to a target table.

        Parameters:
        - data_dict (dict): A dictionary where each key represents a row identifier and each value is another dictionary 
                            representing column values for that row.
        - target_schema (pyspark.sql.types.StructType): The schema of the target table.

        Returns:
        pyspark.sql.DataFrame: A DataFrame created from the provided dictionary of dictionaries, structured according to the specified schema.
        '''
        # Convert dictionary of dictionaries into a list of tuples, then put into DataFrame
        rows_as_tuples = [(key, *value.values()) for key, value in data_dict.items()]
        return spark.createDataFrame(rows_as_tuples, target_schema)
%md
#### ExclusionManager
class ExclusionManager:
    def __init__(self, spark, job_group, job_order):
        '''
        Initializes the ExclusionManager with the given job group and job order.

        Parameters:
            spark...
            job_group (str): The group identifier for the job.
            job_order (str): The order of the job within the group.
        '''
        self.spark = spark
        self.job_group = job_group
        self.job_order = job_order

    def get_selected_column(self, query: str):
        '''
        Extracts the selected column from a SQL query.

        Parameters:
            query (str): The SQL query string.

        Returns:
            str: The selected column in the SQL query.

        Raises:
            Exception: If the query selects all columns (*) or includes multiple columns.
        '''
        parts = query.lower().split('select ', 1)[1].split(' from', 1)
        column = parts[0].strip()

        if column == '*':
            raise Exception('⚠️ Exception in get_selected_column() function: the select query cannot be "SELECT *" - specify one column ⚠️')
        elif ',' in column:
            raise Exception(f'⚠️ Exception in get_selected_column() function: the select query cannot include commas - specify 1 column at most ⚠️')
        elif '.' in column:
            raise Exception(f'⚠️ Exception in get_selected_column() function: the select query cannot include dot notation in the column selection; remove the full stop ⚠️')
        else:
            return column

    def get_exclusion_user_variable(self, userInput):
        '''
        Retrieves a set of exclusion values based on user input.

        Parameters:
            userInput (str): The user input string.

        Returns:
            set: A set of exclusion values.

        Raises:
            Exception: If there is an error during query execution or input processing.
        '''
        try:
            userInput = userInput.lower().strip()
            if not userInput:
                return {''}
            elif userInput.startswith('select') and ' from ' in userInput:
                column = self.get_selected_column(userInput)
                self.spark.conf.set("spark.sql.execution.arrow.enabled", "false")
                originalSet = set(self.spark.sql(userInput).select(column).toPandas()[column])
                split_set = {item for s in originalSet for item in s.split(',')}
                return split_set
            else:
                return set(userInput.split(','))
        except Exception as e:
            print(f'⚠️ Exception in get_exclusion_user_variable() function: {e}.\n')
            raise

    def get_file_extns_to_exclude(self, job_group, job_order):
        query = f"""
        SELECT extension FROM unstruct_metadata.exclusion_extension 
        WHERE jobGroup = '{self.job_group}' AND jobOrder = '{self.job_order}'
        """
        return self.get_exclusion_user_variable(query)
    
    def get_file_names_to_exclude(self, job_group, job_order):
        query = f"""
        SELECT abbreviation FROM unstruct_metadata.exclusion_abbreviation 
        WHERE jobGroup = '{self.job_group}' AND jobOrder = '{self.job_order}'
        """
        return self.get_exclusion_user_variable(query)

    def get_file_paths_to_exclude(self, job_group, job_order):
        query = f"""
        SELECT filePath FROM unstruct_metadata.exclusion_filepath 
        WHERE jobGroup = '{self.job_group}' AND jobOrder = '{self.job_order}'
        """
        return self.get_exclusion_user_variable(query)
    
    def get_folder_paths_to_exclude(self, job_group, job_order):
        query = f"""
        SELECT folderPath FROM unstruct_metadata.exclusion_folderpath 
        WHERE jobGroup = '{self.job_group}' AND jobOrder = '{self.job_order}'
        """
        return  self.get_exclusion_user_variable(query) 
%md
#### Classifier
class Classifier:
    def __init__(self, exclusions):
        '''
        Initializes the Classifier with exclusion criteria.

        Parameters:
            exclusions (dict): A dictionary of exclusion criteria.
        '''
        self.filePathsToExclude = exclusions['filePathsToExclude']
        self.fileExtnsToExclude = exclusions['fileExtnsToExclude']
        self.fileNamesToExclude = exclusions['fileNamesToExclude']
        self.folderPathsToExclude = exclusions['folderPathsToExclude']

    def classify_files_and_folders(self, rows):
        '''
        Classifies files and folders based on exclusion criteria.

        Parameters:
            rows (list): A list of rows to classify.

        Returns:
            dict: A dictionary with file paths as keys and classification reasons as values.
        '''
        classification_dict = {row["sourceFilePath"]: None for row in rows}

        for path in classification_dict:
            if self.item_in_collection(path, self.filePathsToExclude):
                classification_dict[path] = 'file path'
                continue

            name = path.split('/')[-1]
            fileExtn = os.path.splitext(name)[-1].lstrip('.')
            if self.item_in_collection(fileExtn, self.fileExtnsToExclude):
                classification_dict[path] = 'file extension'
                continue

            if self.has_prefix(path, self.folderPathsToExclude):
                classification_dict[path] = 'folder path'
                continue

            fileName = name.split('.' + fileExtn)[0]
            if self.term_in_string(self.fileNamesToExclude, fileName):
                classification_dict[path] = 'file name'

        return classification_dict

    @staticmethod
    def item_in_collection(item, collection):
        '''
        Checks if an item is in a collection.

        Parameters:
            item: The item to check.
            collection: The collection to check against.

        Returns:
            bool: True if the item is in the collection, False otherwise.
        '''
        try:
            collectionSet = set(collection)
            itemSet = set([item])
            return bool(itemSet & collectionSet)
        except Exception as e:
            print(f'⚠️ Exception in item_in_collection() function: {e}.\n')

    @staticmethod
    def term_in_string(terms, string):
        '''
        Checks if any term in a collection is found in a given string.

        Parameters:
            collection (set): The set of terms to check.
            string (str): The string to check against.

        Returns:
            bool: True if any term in the collection is found in the string, False otherwise.
        '''
        try:
            if terms in ([''], {''}, ''):
                return False
            elif len(terms) > 0 and re.compile('|'.join(terms), re.IGNORECASE).search(string):
                return True
            else:
                return False
        except Exception as e:
            print(f'⚠️ Exception in term_in_string() function: {e}.\n')

    @staticmethod
    def has_prefix(path, prefixes):
        '''
        Checks if the given path contains any of the specified prefixes.

        Parameters:
            path (str): The string to check.
            prefixes (set): A set of prefix folder paths.

        Returns:
            bool: True if the path contains any of the prefixes, False otherwise.
        '''
        for prefix in prefixes:
            if path.startswith(prefix):
                return True
        return False
%md
#### DatabaseManager
class DatabaseManager:
    '''
    The DatabaseManager class is a comprehensive utility for managing data operations in a Spark environment, focusing on tasks related to fetching, updating, appending, and deleting data within a specified database and table, ensuring that operations are performed efficiently and based on specified criteria. This class is especially useful for handling large datasets that require classification, exclusion, and copy operations in a Spark context.
    '''
    def __init__(self, spark, database, table, table_format):
        '''
        Initializes the DatabaseManager with the specified database and table configurations.

        Parameters:
            spark: The Spark session object.
            database (str): The name of the database.
            table (str): The name of the table.
            table_format (str): The format of the table.
        '''
        self.spark = spark
        self.database = database
        self.table = table
        self.table_format = table_format

    def fetch_data_to_classify(self, job_group, job_order):
        '''
        Constructs and executes a SQL query to fetch data for classification based on job group and job order.

        Parameters:
            job_group (str): The job group identifier.
            job_order (str): The job order identifier.

        Returns:
            DataFrame: A DataFrame containing rows to exclude based on the job group and job order.
        '''
        select_query = f'''
            SELECT *
            FROM {self.database}.{self.table}
            WHERE jobGroup = {job_group}
            AND jobOrder = {job_order}
            AND isAtSource = 'Y'
        '''
        return self.spark.sql(select_query)
    
    def update_data_to_classify(self, df, classification_dict):
        '''
        Updates  the given DataFrame to classify based on the classification dictionary and adds relevant columns for exclusion reasons and timestamps.

        Parameters:
            df (DataFrame): The DataFrame containing data to update.
            classification_dict (dict): A dictionary containing classification data.

        Returns:
            DataFrame: The updated DataFrame with exclusion reasons and timestamps.
        '''
        classification_schema = StructType([
            StructField('sourceFilePath', StringType(), nullable=False),
            StructField('new_exclusionReason', StringType(), nullable=True)
        ])
        classification_df = self.spark.createDataFrame(classification_dict.items(), classification_schema)

        df = df.join(classification_df, 'sourceFilePath', 'left_outer') \
            .withColumn('exclusionReason', F.when(F.col('new_exclusionReason').isNotNull(), F.col('new_exclusionReason')).otherwise(F.lit(None))) \
            .withColumn('exclusionTimeStamp', F.current_timestamp()) \
            .drop('new_exclusionReason')

        df = df.withColumn('toBeExcluded', F.when(F.col('exclusionReason').isNotNull() & (F.trim(F.col('exclusionReason')) != ''), 'Y').otherwise('N'))
        return df

    def append_dataframe(self, df):
        '''
        Appends data in a Dataframe to the partitioned target table in the specified format.

        Parameters:
            df (DataFrame): The DataFrame containing data to append.
        '''
        df = df.withColumn("jobGroup", col("jobGroup").cast("bigint")).withColumn("jobOrder", col("jobOrder").cast("bigint"))
        df.write.format(self.table_format).mode('append').partitionBy('jobGroup', 'jobOrder').saveAsTable(f"{self.database}.{self.table}")

    def delete_duplicate_rows_by_latest_exlusion(self, job_group, job_order, start_time_str):
        '''
        Constructs and executes a SQL query to delete duplicate rows based on the latest exclusion timestamp.

        Parameters:
            job_group (str): The job group identifier.
            job_order (str): The job order identifier.
            start_time_str (str): The start time string to filter by.

        Returns:
            None
        '''
        delete_query = f'''
            DELETE FROM {self.database}.{self.table}
            WHERE jobGroup = {job_group}
            AND jobOrder = {job_order}
            AND isAtSource = 'Y'
            AND (exclusionTimeStamp < '{start_time_str}' OR exclusionTimeStamp IS NULL)
        '''
        print(delete_query)
        self.spark.sql(delete_query)

    def fetch_data_to_copy(self, job_group, job_order, load_type):
        '''
        Constructs and executes a SQL query to fetch data for copying based on the provided parameters.

        This function constructs the base and extra conditions for the SQL query using the provided job group, job order,
        load type, database name, and table name. It then executes the constructed query against the specified database and table.
        The results of the query are returned as a DataFrame, along with the base and extra conditions used in the query.

        Parameters:
            job_group (str): A unique identifier for the job group.
            job_order (str): The order of the job within its group.
            load_type (str): The load type of the data copying process (e.g., "SNP" for a full copy, "INC" for incremental copies).

        Returns:
            tuple: A tuple containing:
                - DataFrame: A DataFrame containing the results of the executed SQL query.
                - str: The base condition used in the query.
                - str: The extra condition used in the query (if any).

        Raises:
            Exception: If there is an error during query execution, the exception is caught and None is returned.

        Example:
            >>> result_df, base_cond, extra_cond = fetch_data_to_copy("group1", "order1", "FAILURES_ONLY", "myDB", "myTable")
        '''
        base_condition = self.get_base_condition_to_copy(job_group, job_order)
        extra_condition = self.get_extra_condition_to_copy(load_type)

        select_query = f'''SELECT * FROM {self.database}.{self.table}
            WHERE {base_condition} {extra_condition}'''

        try:
            print(f'>>> Initialised select query:\n\t{select_query}')
            Utility.print_boundary(NUMBER_OF_DASHES)
            print(f'>>> Executing select query')
            df = self.spark.sql(select_query)
            Utility.print_boundary(NUMBER_OF_DASHES)
            return df, base_condition, extra_condition
        except Exception as e:
            print(f'⚠️ Exception in fetch_data_to_copy() function: {e}\n')
            return None

    def get_base_condition_to_copy(self, job_group, job_order):
        '''
        Constructs the base condition for the SQL query.

        Parameters:
            job_group (str): The job group identifier.
            job_order (str): The job order identifier.

        Returns:
            str: The base condition string for the SQL query.
        '''
        return f'''jobGroup = '{job_group}'
            AND jobOrder = '{job_order}'
            AND toBeExcluded = 'N'
            AND isAtSource = 'Y' '''

    def get_extra_condition_to_copy(self, load_type):
        '''
        Constructs the extra condition for the SQL query based on the load type.

        Parameters:
            load_type (str): The load type of the data copying process.

        Returns:
            str: The extra condition string for the SQL query.
        '''
        if load_type == 'FAILURES_ONLY':
            return '''AND status = 'F' '''
        elif load_type == 'INC':
            return '''
            AND NOT ((status = 'S' AND copyStatusTimeStamp > modifiedTimeStamp)
            AND status IS NOT NULL AND copyStatusTimeStamp IS NOT NULL AND modifiedTimeStamp IS NOT NULL)'''
        return ''
        
    def delete_duplicate_rows_by_latest_copy(self, job_group, job_order):
        '''
        Constructs and executes a SQL query to delete duplicate rows based on the latest copy timestamp.

        Parameters:
            job_group (str): The job group identifier.
            job_order (str): The job order identifier.

        Returns:
            int: The number of deleted rows.
        '''
        delete_duplicates_query = f'''
        WITH duplicates AS (
            SELECT sourceFilePath, 
                   copyStatusTimeStamp,
                   RANK() OVER (PARTITION BY sourceFilePath ORDER BY copyStatusTimeStamp DESC) AS rank_num
            FROM {self.database}.{self.table}
            WHERE jobGroup = {job_group}
            AND jobOrder = {job_order}
        )
        DELETE FROM {self.database}.{self.table}
        WHERE sourceFilePath IN (
            SELECT sourceFilePath
            FROM duplicates
            WHERE rank_num > 1
        )
        AND (copyStatusTimeStamp IN (
            SELECT copyStatusTimeStamp
            FROM duplicates
            WHERE rank_num > 1
        ) OR copyStatusTimeStamp IS NULL)
        '''
        try:
            deleted_rows = self.spark.sql(delete_duplicates_query)
            deleted_rows_num = deleted_rows.collect()[0][0]
            print(f">>> Deleted {deleted_rows_num} redundant duplicate rows")
            Utility.print_boundary(NUMBER_OF_DASHES)
            return deleted_rows_num
        except Exception as e:
            print(f'⚠️ Exception in delete_duplicate_rows_by_latest_copy() function: {e}.\n')
            return 0

    def fetch_latest_successful_copies(self, base_condition, start_time_str):
        '''
        Constructs and executes a SQL query to retrieve successful records based on the provided conditions.

        Parameters:
            base_condition (str): The base condition for filtering records.
            start_time_str (str): The start time for filtering records.

        Returns:
            DataFrame: A DataFrame containing successful records.

        Example:
            >>> successes_df = fetch_latest_successful_copies("jobGroup = 599 AND jobOrder = 2", "2024-05-30 00:00:00")
        '''
        successes_query = f'''SELECT * FROM {self.database}.{self.table}
        \tWHERE {base_condition}
        \tAND status = "S"
        \tAND copyStatusTimeStamp > "{start_time_str}"'''

        try:
            successes_df = self.spark.sql(successes_query)
            print(f'>>> Retrieving successful records:\n\t{successes_query}')
            Utility.print_boundary(NUMBER_OF_DASHES)
            return successes_df
        except Exception as e:
            print(f'⚠️ Exception in retrieve_success_records() function: {e}\n')
            return None
        
    def transform_successes_df_for_runlog(self, successes_df):
        '''
        Transforms the successes DataFrame to include additional columns necessary for logging the run details.

        Parameters:
            successes_df (DataFrame): The DataFrame containing successful records.

        Returns:
            DataFrame: The transformed DataFrame with additional columns for run log.
        '''
        transformed_df = successes_df.select(
            col('jobGroup'),
            col('jobOrder'),
            col('copyStatusTimeStamp').alias('startTime'),
            current_timestamp().alias('endTime'),
            col('status'),
            col('sourceFilePath'),
            col('targetFilePath'),
            Utility.extract_file_name(col('sourceFilePath')).alias('fileName'),
            col('fileSizeMB'),
            year(current_timestamp()).alias('_year'),
            month(current_timestamp()).alias('_month')
        ).withColumn('errorMessage', lit(''))
        return transformed_df
%md
#### SMBtoAzureBlobCopyManager
class SMBtoAzureBlobCopyManager:
    '''
    The SMBtoAzureBlobCopyManager class is designed to manage the process of copying files from an SMB server to Azure Blob Storage. It handles file transfers, keeps track of the transfer status, and logs updates to a database.
    '''
    def __init__(self, source_folder, target_folder, target_storage, target_container, smb_client_name, smb_client_pwd, container_client, database_manager):
        '''
        Initialize SMBtoAzureBlobCopyManager.

        Parameters:
            source_folder (str): The source folder path on the SMB server.
            target_folder (str): The target folder path on Azure Blob Storage.
            target_storage (str): The name of the Azure Storage account.
            target_container (str): The name of the container in Azure Blob Storage.
            smb_client_name (str): The SMB client username.
            smb_client_pwd (str): The SMB client password.
            container_client (ContainerClient): The Azure Blob Storage container client.
            database_manager (DatabaseManager): The database manager object for logging and updating file transfer status.

        Attributes:
            dict_of_dicts_remaining (dict): A dictionary of dictionaries that keeps track of files and their metadata, status, and any errors during the copying process.
            processed (int): Counter for the number of files successfully processed.
            timer (datetime): Time tracker for periodic updates to the table.
        '''
        self.source_folder = source_folder
        self.target_folder = target_folder
        self.target_storage = target_storage
        self.target_container = target_container
        self.smb_client_name = smb_client_name
        self.smb_client_pwd = smb_client_pwd
        self.container_client = container_client
        self.database_manager = database_manager
        self.dict_of_dicts_remaining = {}
        self.processed = 0
        self.timer = datetime.now()

    def copy_all_files_to_azure(self, df):
        '''
        Copies all SMB files listed in the input DataFrame from SMB Source to Azure Blob Storage. Periodically uploads updates to the table if more than an hour has passed. Updates dict_of_dicts_remaining with files and their metadata.

        Parameters:
            df (DataFrame): A DataFrame containing file paths and metadata.

        Returns:
            None

        Example:
            file_processor.copy_all_files_to_azure(file_dataframe)
        '''
        rows = df.rdd.map(lambda row: row.asDict()).collect()
        self.dict_of_dicts_remaining = {row['sourceFilePath']: {k: v for k, v in row.items() if k != 'sourceFilePath'} for row in rows}

        for smb_file_path in self.dict_of_dicts_remaining.keys():
            try:
                self.upload_file_and_log_result(smb_file_path)
                if (datetime.now() - self.timer) >= timedelta(hours=1):
                    self.upload_updates_to_table()
                print()
            except Exception as e:
                print(f'⚠️ Exception in copy_all_files_to_azure() function: {e}')
                self.handle_upload_result(smb_file_path, None, False, 'Exception in copy_all_files_to_azure() function.' + str(e))

        self.final_uploads_to_table()

    def upload_file_and_log_result(self, smb_file_path):
        '''
        Copies a single file from the SMB server to Azure Blob Storage. Updates dict_of_dicts_remaining with the file's target path and timestamp. Handles any exceptions that occur during the copy process.

        Parameters:
            smb_file_path (str): The path of the file on the SMB server.

        Returns:
            None
        '''
        azure_blob_url = None
        try:
            root_relative_path = Utility.get_root_relative_path(smb_file_path, self.source_folder)
            azure_blob_name = f'{self.target_folder}/{root_relative_path}'
            azure_blob_url = f'https://{self.target_storage}.blob.core.windows.net/{self.target_container}/{azure_blob_name}'

            self.dict_of_dicts_remaining[smb_file_path]['targetFilePath'] = azure_blob_url
            self.dict_of_dicts_remaining[smb_file_path]['copyStatusTimeStamp'] = datetime.now()

            with smbclient.open_file(smb_file_path, mode='rb', username=self.smb_client_name, password=self.smb_client_pwd) as file:
                upload_success = Utility.upload_blob_to_container(self.container_client, azure_blob_name, file.read())
                self.handle_upload_result(smb_file_path, azure_blob_url, upload_success)
        except Exception as e:
            print(f'⚠️ Exception in upload_file_and_log_result() function:\n     ↳ {e}')
            self.handle_upload_result(smb_file_path, azure_blob_url, False, 'Exception in upload_file_and_log_result() function.' + str(e))

    def handle_upload_result(self, smb_file_path, azure_blob_url, success, error_message=None):
        '''
        Handles the result of an upload operation. Updates dict_of_dicts_remaining with the status and any errors.

        Parameters:
            smb_file_path (str): The path of the file on the SMB server.
            azure_blob_url (str): The URL of the file in Azure Blob Storage.
            success (bool): Whether the upload was successful.
            error_message (str, optional): The error message if the upload failed.

        Returns:
            None

        Example:
            self.handle_upload_result('/path/to/smb/file.txt', 'https://example.blob.core.windows.net/container/file.txt', True)
        '''
        try:
            if success:
                print(f"✅ {azure_blob_url}")
                self.dict_of_dicts_remaining[smb_file_path]['status'] = 'S'
            else:
                print(f"🚫 {azure_blob_url}")
                self.dict_of_dicts_remaining[smb_file_path]['status'] = 'F'
                self.dict_of_dicts_remaining[smb_file_path]['copyFailReason'] = error_message or 'Failed to upload blob'
        except Exception as e:
            print(f'\n⚠️ Exception in handle_upload_result(): {e}')

    def upload_updates_to_table(self):
        '''
        Uploads updates of dict_of_dicts_remaining to the control table in the database. Splits the dictionary into processed and remaining files, increments the processed counter, and resets the timer.

        Parameters:
            None

        Returns:
            None

        Example:
            self.upload_updates_to_table()
        '''
        try:
            dict_of_dicts_processed, self.dict_of_dicts_remaining = Utility.split_dict_by_key_value(self.dict_of_dicts_remaining, 'status')
            self.processed += len(dict_of_dicts_processed)

            updated_df = Utility.convert_dict_to_dataframe(dict_of_dicts_processed, Schema.CONTROL_TABLE_SCHEMA)
            self.database_manager.append_dataframe(updated_df)
            print(f'\n{"☰"*23}   {self.processed} processed, {len(self.dict_of_dicts_remaining)} remaining   {"☰"*23}')
            self.timer = datetime.now()
        except Exception as e:
            print(f'\n⚠️ Exception in upload_updates_to_table() function during intermediate save: {e}')

    def final_uploads_to_table(self):
        if len(self.dict_of_dicts_remaining) > 0:
            try:
                dict_of_dicts_processed = self.dict_of_dicts_remaining
                self.processed += len(dict_of_dicts_processed)
                updated_df = Utility.convert_dict_to_dataframe(dict_of_dicts_processed, Schema.CONTROL_TABLE_SCHEMA)
                self.database_manager.append_dataframe(updated_df)
                print(f'\n{"☰"*23}   {self.processed} processed, 0 remaining   {"☰"*23}')
            except Exception as e:
                print(f'\n⚠️ Exception in upload_updates_to_table() function during final save: {e}')
%md
# Functions
%md
#### run_receiver_notebook
def run_receiver_notebook(SMBClientUsername, SMBClientPassword, keyVaultName, sourceDBName, sourceTblName, targetDBName, targetTblName, tgtStorageAccount, tgtStorageAccountKeyName, tgtContainer, sourceFolderRootPath, tgtFolderRootPath, fkTargetFileFormat, query):
    '''
    Runs a Databricks notebook (parameter: runReceiverNotebook) with the specified arguments.
    The notebook is executed with a timeout of 0 seconds, meaning it will run until completion without being forcibly terminated.
    
    Arguments:
    - jobGroup: A unique identifier for the job group.
    - jobOrder: The order of the job within its group.
    - fkLoadType: A foreign key representing the type of data load.
    - SMBClientUsername: The username for accessing the SMB client.
    - SMBClientPassword: The password for accessing the SMB client.
    - keyVaultName: The name of the key vault used for secure storage.
    - sourceDBName: The name of the source database.
    - sourceTblName: The name of the source table.
    - targetDBName: The name of the target database.
    - targetTblName: The name of the target table.
    - runLogTable: The table used for logging the run.
    - tgtStorageAccount: The storage account for the target.
    - tgtStorageAccountKeyName: The key name for the target storage account.
    - tgtContainer: The container name for the target storage.
    - sourceFolderRootPath: The root path of the source folder.
    - tgtFolderRootPath: The root path of the target folder.
    - fkTargetFileFormat: A foreign key representing the file format for the target data.
    - query: The SQL query to be executed in the notebook.
    '''
    
    dbutils.notebook.run(
        runReceiverNotebook,
        timeout_seconds=0,
        arguments={
            'SMBClientUsername': SMBClientUsername,
            'SMBClientPassword': SMBClientPassword,
            'keyVaultName': keyVaultName,
            'sourceDBName': sourceDBName,
            'sourceTblName': sourceTblName,
            'targetDBName': targetDBName,
            'targetTblName': targetTblName,
            'tgtStorageAccount': tgtStorageAccount,
            'tgtStorageAccountKeyName': tgtStorageAccountKeyName,
            'tgtContainer': tgtContainer,
            'sourceFolderRootPath': sourceFolderRootPath,
            'tgtFolderRootPath': tgtFolderRootPath,
            'fkTargetFileFormat': fkTargetFileFormat,
            'query': query
        }
    )
