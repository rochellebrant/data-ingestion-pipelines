%md
# Overview
This script performs a series of operations to fetch file metadata from an SMB (Server Message Block) share, processes this metadata, and updates a target database table in a Spark environment. This process includes recursively listing files in an SMB folder, handling errors, and integrating the fetched metadata with existing data.

This documentation should provide a comprehensive understanding of how the code operates and the purpose of each component involved.

# Detailed Documentation

### 1. fetch_smb_file_metadata Function
The `fetch_smb_file_metadata` function fetches and returns metadata (file size, modification time, and creation time) for a file from the SMB path.

### 2. list_netapp_content_into_dataframe Function
The `list_netapp_content_into_dataframe` function recursively lists contents of an SMB folder, fetches metadata for each file, and appends the results to `listing_successes` which finally get written to a Spark DataFrame. If `recursive` is True, the function will list contents of subdirectories recursively. The function handles errors and updates the `failures` list in case of exceptions.

### 3. Main Execution
- **Widget Values:** Retrieves job-related parameters from widgets (assuming this is within a Databricks environment).
- **Execution Flow:**
  - Initializes an empty DataFrame `new_listing_df` with the specified schema for new listings.
  - Checks if a failure table exists; exits if true (to prevent proceeding with unresolved access errors).
  - Calls `list_netapp_content_into_dataframe` to recursively fetch metadata and update `new_listing_df`.
  - Uses a temporary table to hold the old listing data of the specified jobGroup and jobOrder in `existing_listing_df` to handle updates and optimizations. This old listing data is removed from the target table.
  - Performs an outer join between the existing data and the new data.
  - Uses `COALESCE` to update fields with new values where available.
  - Updates the `isAtSource` column based on specific conditions.
  - Writes the final DataFrame to the target table, optimizes the target table, and drops the temporary table.
# dbutils.widgets.text('jobGroup', '600')
# dbutils.widgets.text('jobOrder', '20'

# dbutils.widgets.text('SMBClientUsername', 'usiphdireporting@bp.com')
# dbutils.widgets.text('SMBClientPassword', 'USIPHDIREPORTING-PWD-KV')
# dbutils.widgets.text('keyVaultName', 'ZNEUDL1P48DEF-kvl01') # "ZNEUDL1P40INGing-kvl00" or "ZNEUDL1P48DEF-kvl01"

# dbutils.widgets.text('sourceURL', 'aadsmbuswp-895a.bp1.ad.bp.com/usw2/hou_GoM_SPU/Resource/AREA/OBO_Great_White/Great_White/Fieldwide/Presentations_Meetings/Project/Asset_Mgmt/Meetings/2008_JV_HW+PRH_Mtgs')

# dbutils.widgets.text('targetDBName', 'unstruct_metadata')
# dbutils.widgets.text('targetTblName', 'netapp_configuration')
# dbutils.widgets.text('failuresTableName', 'unstructured_failedlog_recursive_nl')

# dbutils.widgets.text('fkTargetFileFormat', 'delta')
# dbutils.widgets.text('comments', '{"recursive":True}')
# !pip install smbprotocol
%run "./ModuleFunctions"
# import pandas as pd
import json
from pyspark.sql import SparkSession
import bp.dwx.tableUtils._

JOB_GROUP = int(dbutils.widgets.get('jobGroup'))                 # 599
JOB_ORDER = int(dbutils.widgets.get('jobOrder'))                 # 17 Olympus, 18 Mars, 19 Great White, 20 Ursa Princess
SMB_CLIENT_NAME = dbutils.widgets.get('SMBClientUsername')       # r'usiphdireporting@bp.com'
SMB_CLIENT_PWD_NAME = dbutils.widgets.get('SMBClientPassword')    # 'USIPHDIREPORTING-PWD-KV'
SMB_FOLDER = dbutils.widgets.get('sourceURL')
                                                                # r'aadsmbuswp-895a.bp1.ad.bp.com/usw2/hou_GoM_SPU/Resource/AREA/OBO_Olympus'
                                                                # r'aadsmbuswp-895a.bp1.ad.bp.com/usw2/hou_GoM_SPU/Resource/AREA/OBO_Mars'
                                                                # r'aadsmbuswp-895a.bp1.ad.bp.com/usw2/hou_GoM_SPU/Resource/AREA/OBO_Ursa_Princess'
TARGET_DB = dbutils.widgets.get('targetDBName')                     # 'unstruct_metadata'
TARGET_TBL = dbutils.widgets.get('targetTblName')                   # "netapp_configuration"
FAILURE_TBL = f'listing_failures_{JOB_GROUP}_{JOB_ORDER}'
TABLE_FORMAT = dbutils.widgets.get('fkTargetFileFormat')         # "delta"
KEY_VAULT = dbutils.widgets.get('keyVaultName')                    # "ZNEUDL1P40INGing-kvl00" or "ZNEUDL1P48DEF-kvl01"

IS_RECURSIVE = json.loads(dbutils.widgets.get('comments').replace("'", '"').replace('True', 'true').replace('False', 'false'))['recursive']  # Use json.loads() to convert the string to a dictionary
SMB_CLIENT_PWD = dbutils.secrets.get(scope = KEY_VAULT, key = SMB_CLIENT_PWD_NAME)
TEMP_TBL = f'netapp_configuration_{JOB_GROUP}_{JOB_ORDER}_tmp'

def fetch_smb_file_metadata(item_path, smb_client_name, smb_client_pwd):
    '''
    Fetches metadata for a file located at the specified SMB path. The metadata includes the file size in megabytes, the last modified datetime, and the creation datetime.

    Parameters:
      item_path (str): Path to the file for which metadata is to be fetched.
      smb_client_name (str): SMB client username.
      smb_client_pwd (str): SMB client password.

    Returns:
      tuple: File size in megabytes (int), last modified datetime (datetime), creation datetime (datetime).
    '''
    size_bytes = smbclient.path.getsize(item_path, username=smb_client_name, password=smb_client_pwd)
    size_MB = round(size_bytes / 1_000_000, 3)

    modified_time = smbclient.path.getmtime(item_path, username=smb_client_name, password=smb_client_pwd)
    modified_datetime = datetime.fromtimestamp(modified_time)

    creation_time = smbclient.path.getctime(item_path, username=smb_client_name, password=smb_client_pwd)
    creation_datetime = datetime.fromtimestamp(creation_time)
    
    return size_MB, modified_datetime, creation_datetime



def process_batch(listing_successes, df):
    file_chunk_df = spark.createDataFrame(data=listing_successes, schema=Schema.CONTROL_TABLE_SCHEMA)
    df = df.union(file_chunk_df)
    listing_successes.clear()
    return df


def list_netapp_content_into_dataframe(listing_successes, failures, df, smb_folder_path, SMBClientUsername, SMBClientPassword, job_group, job_order, recursive = True, log_interval = 10000, chunk_size = 50000):
  '''
    Recursively lists the contents of an SMB folder and writes the metadata to a table.
  
    Parameters:
        listing_successes (list): List to append successful metadata retrieval results.
        failures (list): List to append failures.
        df (DataFrame): DataFrame to which the metadata will be added.
        smb_folder_path (str): The SMB folder path to list.
        smb_client_username (str): The SMB client username.
        smb_client_password (str): The SMB client password.
        job_group (str): The job group identifier.
        job_order (str): The job order identifier.
        recursive (bool): Whether to list contents recursively.

    Returns:
      DataFrame: The updated DataFrame with new metadata.
  '''

  try:
    items = smbclient.listdir(smb_folder_path, username = SMBClientUsername, password = SMBClientPassword)
  except Exception as e:
    failures.append([job_group, JOB_ORDER, smb_folder_path, str(e), datetime.now()])
    return df
  
  for i in items:
    item_path = os.path.join(smb_folder_path, i)

    if not recursive:
      size_MB, modified_datetime, creation_datetime = fetch_smb_file_metadata(item_path, SMBClientUsername, SMBClientPassword)
      listing_successes.append([
          item_path, job_group, job_order, 'Y', size_MB, creation_datetime, modified_datetime, datetime.now(), None, None, None, None, None, None, None
          ])
      

    else:
      # FOLDERS
      if smbclient.path.isdir(item_path, username = SMBClientUsername, password = SMBClientPassword):
        df = list_netapp_content_into_dataframe(listing_successes, failures, df, item_path, SMBClientUsername, SMBClientPassword, job_group, job_order, recursive)

      # FILES
      elif smbclient.path.isfile(item_path, username = SMBClientUsername, password = SMBClientPassword):
        # print(f'\t➤ {item_path}')
        size_MB, modified_datetime, creation_datetime = fetch_smb_file_metadata(item_path, SMBClientUsername, SMBClientPassword)
        listing_successes.append([
            item_path, job_group, job_order, 'Y', size_MB, creation_datetime, modified_datetime, datetime.now(), None, None, None, None, None, None, None
            ])
        
        if len(listing_successes) % log_interval == 0:
          print("--------------------------------------------" + str(len(listing_successes)) + "------------------------------------------")

        if len(listing_successes) >= chunk_size:
          df = process_batch(listing_successes, df)

  return df
new_listing_df = spark.createDataFrame([], schema=Schema.CONTROL_TABLE_SCHEMA)
listing_successes, failures = [], []

if spark._jsparkSession.catalog().tableExists(TARGET_DB, FAILURE_TBL):
  dbutils.notebook.exit(f'Delete {TARGET_DB}.{FAILURE_TBL} and make sure all the access denied errors are dealt with before proceeding')

new_listing_df = list_netapp_content_into_dataframe(listing_successes, failures, new_listing_df, SMB_FOLDER, SMB_CLIENT_NAME, SMB_CLIENT_PWD, JOB_GROUP, JOB_ORDER, IS_RECURSIVE)

if listing_successes: # put final elements of listing_successes into a dataframe
  new_listing_df = process_batch(listing_successes, new_listing_df)

if failures: # put final elements of failures into a dataframe
  failures_df = spark.createDataFrame(data = failures, schema = Schema.FAILURE_TABLE_SCHEMA)
  tableUtils.saveAsTable(
                          df = failures_df,
                          fullTargetTableName = s"${TARGET_DB}.${FAILURE_TBL}",
                          writeMode = "overwrite",
                          debug = true,
                          dryRun = false
                          )
  print(f"Look into failure table: {TARGET_DB}.{FAILURE_TBL}")

# Rename columns to avoid conflicts during later join
new_listing_df = new_listing_df.select([col(c).alias(f'new_{c}') for c in new_listing_df.columns])
print(f"new_listing_df: {new_listing_df.count()}")
display(new_listing_df)
existing_listing_df = spark.sql(f'SELECT * FROM {TARGET_DB}.{TARGET_TBL} WHERE jobGroup={JOB_GROUP} and jobOrder={JOB_ORDER}')

# Force null values in columns that are to be updated
columns_to_update = ['listingTimeStamp', 'modifiedTimeStamp', 'fileSizeMB', 'createdTimeStamp', 'isAtSource']
for col_name in columns_to_update:
    existing_listing_df = existing_listing_df.withColumn(col_name, when((col('jobGroup') == JOB_GROUP) & (col('jobOrder') == JOB_ORDER), lit(None)).otherwise(col(col_name)))

# Save the modified dataframe to a temporary table & reload the data from the temporary table
tableUtils.saveAsTable(
                        df = existing_listing_df,
                        fullTargetTableName = s"${TARGET_DB}.${TEMP_TBL}",
                        writeMode = "overwrite",
                        debug = true,
                        dryRun = false
                        )
existing_listing_df = spark.sql(f'SELECT * FROM {TARGET_DB}.{TEMP_TBL}')
display(existing_listing_df)
# Delete the old entries from the original table and vacuum it
spark.sql(f'DELETE FROM {TARGET_DB}.{TARGET_TBL} WHERE jobGroup={JOB_GROUP} and jobOrder={JOB_ORDER}')
spark.sql(f'VACUUM {TARGET_DB}.{TARGET_TBL}')
# Perform an outer join between the master dataframe and the updated dataframe
joined_df = existing_listing_df.join(new_listing_df, 
                          (existing_listing_df.sourceFilePath == new_listing_df.new_sourceFilePath) &
                          (existing_listing_df.jobGroup == new_listing_df.new_jobGroup) &
                          (existing_listing_df.jobOrder == new_listing_df.new_jobOrder), 
                          'outer') 
print(f"joined_df: {joined_df.count()}")
display(joined_df)
# Use COALESCE to update the original dataframe with new values where available
coalesce_df = joined_df.selectExpr(
    'COALESCE(new_sourceFilePath, sourceFilePath) AS sourceFilePath',
    'COALESCE(new_jobGroup, jobGroup) AS jobGroup',
    'COALESCE(new_jobOrder, jobOrder) AS jobOrder',
    'COALESCE(new_isAtSource, isAtSource) AS isAtSource',
    'COALESCE(new_fileSizeMB, fileSizeMB) AS fileSizeMB',
    'COALESCE(new_createdTimeStamp, createdTimeStamp) AS createdTimeStamp',
    'COALESCE(new_modifiedTimeStamp, modifiedTimeStamp) AS modifiedTimeStamp',
    'COALESCE(new_listingTimeStamp, listingTimeStamp) AS listingTimeStamp',
    'COALESCE(new_toBeExcluded, toBeExcluded) AS toBeExcluded',
    'COALESCE(new_exclusionReason, exclusionReason) AS exclusionReason',
    'COALESCE(new_exclusionTimeStamp, exclusionTimeStamp) AS exclusionTimeStamp',
    'COALESCE(new_targetFilePath, targetFilePath) AS targetFilePath',
    'COALESCE(new_status, status) AS status',
    'COALESCE(new_copyFailReason, copyFailReason) AS copyFailReason',
    'COALESCE(new_copyStatusTimeStamp, copyStatusTimeStamp) AS copyStatusTimeStamp'
)

# Update the 'isAtSource' column based on specific conditions
coalesce_df = coalesce_df.withColumn('isAtSource', when(
    (col('createdTimeStamp').isNull()) & 
    (col('modifiedTimeStamp').isNull()) & 
    (col('listingTimeStamp').isNull()) & 
    (col('fileSizeMB').isNull()), 'N').otherwise('Y'))

# display(coalesce_df)

# Reorder the columns as specified
desiredColumnOrder = ['sourceFilePath', 'jobGroup', 'jobOrder', 'isAtSource', 'fileSizeMB', 'createdTimeStamp', 'modifiedTimeStamp', 'listingTimeStamp', 'toBeExcluded', 'exclusionReason', 'exclusionTimeStamp', 'targetFilePath', 'status', 'copyFailReason', 'copyStatusTimeStamp']
final_df = coalesce_df.select(*desiredColumnOrder)
print(f"final_df: {final_df.count()}")
display(final_df)
# Append the final dataframe to the target table
tableUtils.saveAsTable(
                        df = final_df,  
                        fullTargetTableName = s"${TARGET_DB}.${TARGET_TBL}",
                        writeMode = "overwrite",  
                        debug = true,  
                        dryRun = false 
                        )

# Optimize the target table and drop the temporary table
spark.sql(f'OPTIMIZE {TARGET_DB}.{TARGET_TBL}')
spark.sql(f'DROP TABLE {TARGET_DB}.{TEMP_TBL}')
display(spark.sql(f'SELECT * FROM {TARGET_DB}.{TARGET_TBL} WHERE jobGroup = {JOB_GROUP} and jobOrder = {JOB_ORDER}'))
