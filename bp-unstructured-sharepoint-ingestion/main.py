"""
### USEFUL NOTES:
This unstructured file ingestion process enables copying data from SharePoint to an Azure Blob Storage account while maintaining the folder structure. You need to specify a SharePoint URL path and a target location in Azure Blob Storage.

Below are the key variables involved in this process, categorized into generic, SharePoint, and Azure Blob Storage related groups.

------------------------------------------------------------

#### Generic Variables
- **all_files_copied** (Boolean): True if all copies were successful.

------------------------------------------------------------

#### SharePoint Related Paths
- **source_URL**
  - This is the full SharePoint URL path of the folder *or* file that is to be copied to Azure Blob Storage. If it is a folder location that is to be copied *recursively*, then everything within the folder will be copied (example 1). If it is a folder location that is NOT to be copied recursively, then only all files (not folders) within the folder will be copied only (example 2).
  - example 1a: `https://bp365.sharepoint.com/sites/GDN_NorthSea/Clair/206_8-11Z/PLT/206_8-11z  Flow Anlysis Log.pdf`
  - example 2a: `https://bp365.sharepoint.com/sites/GDN_NorthSea/Clair/01 Petrophysics/`
- **server_rel_url**
  - This is the SharePoint URL path of the folder relative to the server of the file that is to be copied to Azure Blob Storage. It is the source_URL but without the 'https://bp365.sharepoint.com' at the start.
  - example 1b: `/sites/GDN_NorthSea/Clair/206_8-11Z/PLT/206_8-11z  Flow Anlysis Log.pdf`
  - example 2b: `/sites/GDN_NorthSea/Clair/01 Petrophysics/`
- **server_rel_root**
  - *This is only used for the recursive copying of a path.* It is the SharePoint URL path of the *root* folder that is to be recursively copied.
  - example 2c: `/sites/GDN_NorthSea/Clair/206_8-11Z/PLT/`
  
------------------------------------------------------------

#### Azure Storage Related Paths
- **tgt_container_rel_file_path**
  - This is the target file path relative to the Azure Blob Storage container.
  - example 1d: `Clair_Data/Clair/206_8-11Z/PLT/206_8-11z  Flow Anlysis Log.pdf`
  - example 2d: `Clair_Data/Clair/Clair/01 Petrophysics/`
- **az_strg_root_path**
  - The Azure Storage root folder path within which Blobs are to be created.
  - example 1e: `wasbs://bptocgg@zneusu5p82cggdata.blob.core.windows.net/Clair_Data/Clair/206_8-11Z/PLT/`
  - example 2e: `abfss://udl-container@zneudl1p33lakesstor.blob.core.windows.net/Folder1/Folder2/`
- **az_strg_file_path**
  - This is the Azure Storage path for the Blob file being created (i.e. the target location).
  - example 1f: `wasbs://bptocgg@zneusu5p82cggdata.blob.core.windows.net/Clair_Data/Clair/206_8-11Z/PLT/206_8-11z  Flow Anlysis Log.pdf`
  - example 2f: `abfss://udl-container@zneudl1p33lakesstor.blob.core.windows.net/Folder1/Folder2/file.txt`

------------------------------------------------------------

For best viewing, zoom out!
"""

%run ./ModuleFunctions
%run ./EmailNotifications
import re
import json
import time
import pandas as pd
from math import ceil
from datetime import datetime, timedelta
from office365.sharepoint.files.file import File
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings



# Constants or Global Variables
RUN_LOG_COLS = ['jobGroup', 'jobOrder', 'startTime', 'endTime', 'status', 'errorMessage', 'fileName', 'fileSizeMB', 'sourceFilePath', 'targetFilePath', '_year', '_month']
NUMBER_OF_DASHES = 117
RE_AUTH_TIME_HOURS = 22
SLEEPTIME_BETWEEN_FOLDERS = 0
SLEEPTIME_AFTER_ERROR = 60

# Specific to RECURSIVE copying block
SPECIAL_CHARS = ['%', '%25', '#', '%23', '⅓' , '%E2%85%93', '⅕' , '%E2%85%95', '⅙' , '%E2%85%99', '⅛' , '%E2%85%9B', '⅔' , '%E2%85%94', '⅖' , '%E2%85%96', '⅚' , '%E2%85%9A', '⅜' , '%E2%85%9C', '¾' , '%C2%BE', '⅗' , '%E2%85%97', '⅝' , '%E2%85%9D', '⅞' , '%E2%85%9E', '⅘' , '%E2%85%98', '¼' , '%C2%BC', '⅐' , '%E2%85%90', '⅑' , '%E2%85%91', '⅒' , '%E2%85%92']

# Get job details
JOB_GROUP = dbutils.widgets.get("jobGroup")
JOB_ORDER = dbutils.widgets.get("jobOrder")
JOB_NUM = dbutils.widgets.get("jobNum")
METADATA_FIELDS = ["pktblJobQueue", "fkloadType", "jobGroup", "jobOrder", "jobNum", "filterQuery", "sourceURL", "keyVaultName", "intermediateStoreConnID", "intermediateStoreSecretName", "inscopeColumnList", "targetFilePath", "targetStorageAccount", "targetContainer", "targetStoreConnID", "failureNotificationEmailIDs"]

# Print job details
print(f'jobGroup: {JOB_GROUP}\tjobOrder: {JOB_ORDER}\tjobNum: {JOB_NUM}')



queryDF = get_metadata(JOB_GROUP, JOB_ORDER, JOB_NUM, METADATA_FIELDS)
display(queryDF)
def main():
  all_files_copied = True
  start_time = datetime.now()
  size_MB, updated_ingestion_time, file_status, err_msg, source_URL, file_name, targetFilePath, run_df = '','','','','','','', pd.DataFrame()
  
  for record in queryDF:
    try:
      load_type, filter_query, source_URL, kv_name, intermediate_conn_id, intermediate_secret_name, inscopeColumnList, tgt_storage, tgt_conn_id, tgt_container, targetBaseFilePath, failure_emails = assign_to_variables(record)
      web, ctx, ctx_auth_ts = SPO_authentication(source_URL, kv_name, intermediate_conn_id, intermediate_secret_name)
      run_status = 'R'
      tgt_conn_key = dbutils.secrets.get(scope = kv_name, key = tgt_conn_id)
      source_df = spark.sql(f"{filter_query}").toPandas()

      folderListToCopy = list(source_df['docHubPath'])
      
      if load_type not in ['SNP', 'INC']:
        raise Exception('⚠️ load_type must be SNP or INC ⚠️')

      for folder in folderListToCopy:
        is_recursive = source_df.loc[source_df['docHubPath'] == folder, 'isRecursive'].iloc[0]
        last_run_ts = source_df.loc[source_df['docHubPath'] == folder, 'LastIngestionDate'].iloc[0]
        last_run_ts = datetime.fromisoformat(str(last_run_ts))
        print_folder_info(folder, last_run_ts, load_type)
        server_rel_url, server_rel_root, targetFilePath, file_names_to_exc, file_names_to_inc, file_extns_to_include = get_folder_info(source_df, folder)

        if is_recursive == "Y":
          print_boundary(NUMBER_OF_DASHES)
          print('>>> Beginning recursive copying')
          print_boundary(NUMBER_OF_DASHES)
          invalid_folder_names = []
          run_df, all_files_copied, recursion_complete, web, ctx, ctx_auth_ts, invalid_folder_names = recursive_fun(server_rel_url, server_rel_root, targetFilePath, web, ctx, ctx_auth_ts, is_recursive, last_run_ts, run_status, file_names_to_exc, file_names_to_inc, file_extns_to_include, tgt_container, tgt_storage, tgt_conn_key, load_type, invalid_folder_names)
          
          if recursion_complete == False:
            print_boundary(NUMBER_OF_DASHES)
            print(f'>>> ❗ Re-processing folder : {folder}')
            time.sleep(SLEEPTIME_AFTER_ERROR)
            print_boundary(NUMBER_OF_DASHES)
            run_df, all_files_copied, recursion_complete, web, ctx, ctx_auth_ts, invalid_folder_names = recursive_fun(server_rel_url, server_rel_root, targetFilePath, web, ctx, ctx_auth_ts, is_recursive, last_run_ts, run_status, file_names_to_exc, file_names_to_inc, file_extns_to_include, tgt_container, tgt_storage, tgt_conn_key, load_type, invalid_folder_names)
          
          print_boundary(NUMBER_OF_DASHES)
          update_unstructpathmappings_lastingestiondate(folder) if all_files_copied else print(f'⚠️ Failed to process all folders ({load_type} load). {err_msg} ⚠️\n')

          if len(invalid_folder_names) > 0:
            print(f'\n\t\tILLEGAL FOLDERS  -  {len(invalid_folder_names)} folder names need changing due to containing illegal characters:')
            for f in invalid_folder_names:
              print(f'\t\t\t➜ {f}')
            notify_invalid_folders(invalid_folder_names, failure_emails)

        else:
          file_list_response = get_file_list(web, ctx, server_rel_url)
          if file_list_response['success']:
            file_list = file_list_response['result']  # this portion only runs if the API call was successful
          else: 
            continue # if the API call failed then we skip to the next folder
          for file in file_list:
            inc, exc = whether_to_include(file.name, file_names_to_inc, file_extns_to_include), whether_to_exclude(file.name, file_names_to_exc)

            if not(inc[0]) or exc[0]:
              print(f'\t\t❗ Skipping  ⮞  {file.name}\t\t{inc[1]}{exc[1]}')
              continue

            elif load_type == 'INC' and (file.properties.get('TimeLastModified') < last_run_ts) and (file.properties.get('TimeCreated') < last_run_ts):
              print(f'\t• Not modified ⮞ {file.name}\t\tLast checked ⮞ {last_run_ts}')
              continue

            else:
              time_last_modified = file.properties.get('TimeLastModified')
              print(f'\t• Processing ⮞ {file.name}\t\tFile modified ⮞ {time_last_modified}')
              start_time, size_MB, source_URL, targetFilePath = get_processing_metadata_NON_RECURSIVE(file, folder, source_df)
              file_status = 'S'
              all_files_copied, err_msg = copy_file(ctx, tgt_conn_key, file.properties.get('ServerRelativeUrl'), tgt_container, tgt_storage, targetFilePath[targetFilePath.index('net')+4:])
              if not all_files_copied:
                file_status = 'F'
              time.sleep(SLEEPTIME_BETWEEN_FOLDERS)
              run_df = append_to_runLog_df(start_time, file_status, err_msg, file.name, size_MB, source_URL, targetFilePath, run_df)

          print()
          update_unstructpathmappings_lastingestiondate(folder) if all_files_copied else print(f'⚠️ Failed to process all folders ({load_type} load). {err_msg} ⚠️\n')
          

    except Exception as e:
      print('\n⚠️ Exception occurred: ' + str(e) + ' ⚠️')
      file_status, run_status = 'F', 'F'
      err_msg = str(e)
      run_df = append_to_runLog_df(start_time, file_status, err_msg, file_name, size_MB, source_URL, targetFilePath, run_df)

    finally:
      print(f'>>> Final append to runLog')
      print_boundary(NUMBER_OF_DASHES)
      append_to_ADLS(run_df)

  return run_status
run_status = main()
if run_status == 'F':
  raise Exception("Job failed. Please check DBX run for more details. ")
