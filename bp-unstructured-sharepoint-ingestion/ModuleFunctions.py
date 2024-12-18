from msal import ConfidentialClientApplication
import datetime
from pyspark.sql.types import *
def print_boundary(num_of_dashes: int):
  print('-'*num_of_dashes)

def print_folder_info(folder, last_run_ts, load_type):
  print(f'>>> Master Folder: {folder}')
  print(f'\n\t\t☛ Last ingested : {last_run_ts}                     Load type : {load_type} ☚\n')
  
ENV_TenantId = "ea80952e-a476-42d4-aaf4-5457852b0f7e"
ENV_Authority = "https://login.windows.net/" + ENV_TenantId
ENV_ResourceAppIdURI = "https://database.windows.net/.default"
ENV_AuditDBSPKey = "spid-BI-zne-udl1-p-12-udl-rsg" 
ENV_AuditDBSPKeyPwd = "sppwd-BI-zne-udl1-p-12-udl-rsg"  
ENV_KeyVault = "ZNEUDL1P40INGing-kvl00"

ENV_ServicePrincipalId = dbutils.secrets.get(scope = ENV_KeyVault, key = ENV_AuditDBSPKey)
ENV_ServicePrincipalPwd = dbutils.secrets.get(scope = ENV_KeyVault, key = ENV_AuditDBSPKeyPwd)

JDBC_url = "jdbc:sqlserver://zneudl1p12sqlsrv002.database.windows.net:1433;database=zneudl1p12sqldb001"


def get_connection():
  """
  Establishes a secure connection to a database using Microsoft Azure Active Directory authentication.

  Returns:
      dict: A dictionary containing connection properties necessary for establishing a secure connection 
            to the database. The dictionary includes the following key-value pairs:
            - 'accessToken': Access token acquired for the client application.
            - 'hostNameInCertificate': The expected host name in the SSL certificate.
            - 'encrypt': A flag indicating whether encryption is enabled (set to 'true').
  """
  context = ConfidentialClientApplication(ENV_ServicePrincipalId, ENV_ServicePrincipalPwd, ENV_Authority)
  newtoken = context.acquire_token_for_client(ENV_ResourceAppIdURI)
  connectionProperties = {"accessToken" : newtoken.get('access_token'), "hostNameInCertificate" : "*.database.windows.net", "encrypt" : "true"}
  return connectionProperties 


def get_metadata(job_group, job_order, job_num, columns: list):
  """
  Retrieves metadata from a database table based on specified conditions.

  Parameters:
      job_group (str): The job group identifier.
      job_order (int): The job order number.
      job_num (int): The job number.
      columns (list): A list of column names to retrieve from the database table.

  Returns:
      list: A list containing the metadata retrieved from the database. Each element in the list
            represents a row of metadata, formatted as a dictionary where keys correspond to 
            column names and values correspond to column values.

  Note:
      This function assumes the existence of a table named 'audit.tblJobQueue' in the database.
      The function constructs a SQL query based on the provided conditions and columns, then
      executes the query using Apache Spark's JDBC API. The connection properties required
      for establishing the database connection are obtained through the 'get_connection()' function.
  """
  condition = [f"jobGroup={job_group}", f"isActive='Y'"]
  if int(job_order) > 0:
      condition.append(f"jobOrder={job_order}")
  if int(job_num) > 0:
      condition.append(f"jobNum={job_num}")
  query = f"(SELECT {', '.join(columns)} FROM audit.tblJobQueue WHERE {' AND '.join(condition)}) AS tab"
  query_df = spark.read.jdbc(url=JDBC_url, table=query, properties=get_connection())    
  return query_df.collect()


def assign_to_variables(record):
  """
  Assigns DataFrame records, typically extracted from the 'P12 jobQueue' table, to variables for further processing.

  Parameters:
      record (DataFrame): A DataFrame record representing a row of data from the 'P12 jobQueue' table.

  Returns:
      tuple: A tuple containing various attributes extracted from the DataFrame record, including:
              - fkloadType: The type of load associated with the job.
              - filterQuery: The filter query applied to the data.
              - sourceURL: The URL of the data source.
              - keyVaultName: The name of the Azure Key Vault storing secrets.
              - intermediateStoreConnID: The connection ID for intermediate storage.
              - intermediateStoreSecretName: The name of the secret in the intermediate store.
              - inscopeColumnList: A list of columns in scope for processing.
              - targetStorageAccount: The storage account for the target.
              - targetStoreConnID: The connection ID for the target storage.
              - targetContainer: The container for the target storage.
              - targetFilePath: The file path for the target.
              - failureNotificationEmailIDs: Email IDs for failure notification.

  Note:
      This function is designed to process DataFrame records extracted from the 'P12 jobQueue' table.
      It prints out the configuration details of the audit table and returns specific attributes from the record
      as a tuple for further usage.
  """
  print_boundary(NUMBER_OF_DASHES)
  print('>>> Audit table configuration:')
  print('\tpkTblJobQueue :', record.pktblJobQueue)
  print('\tload_type :', record.fkloadType)
  print('\tfilter_query :', record.filterQuery)
  print('\tsource_URL :', record.sourceURL)
  print('\tkv_name :', record.keyVaultName)
  print('\tintermediate_conn_id :', record.intermediateStoreConnID)
  print('\tintermediate_secret_name :', record.intermediateStoreSecretName)
  print('\ttgt_storage :', record.targetStorageAccount)
  print('\ttgt_conn_id :', record.targetStoreConnID)
  print('\ttgt_container :', record.targetContainer)
  print('\ttargetBaseFilePath :', record.targetFilePath)
  print('\tfailure_emails :', record.failureNotificationEmailIDs)
  print_boundary(NUMBER_OF_DASHES)
  return record.fkloadType, record.filterQuery, record.sourceURL, record.keyVaultName, record.intermediateStoreConnID, record.intermediateStoreSecretName, record.inscopeColumnList, record.targetStorageAccount, record.targetStoreConnID, record.targetContainer, record.targetFilePath, record.failureNotificationEmailIDs
def equivalent_type(f):
  """
  Determines the equivalent Spark SQL data type for a given Pandas DataFrame data type.

  Parameters:
      f (str): The Pandas DataFrame data type.

  Returns:
      DataType: The equivalent Spark SQL data type corresponding to the input Pandas data type.

  Note:
      This function is used to convert Pandas DataFrame data types to Spark SQL data types for compatibility
      when working with mixed dataframes in PySpark. It returns the corresponding Spark SQL data type for
      common Pandas data types such as 'datetime64[ns]', 'int64', 'int32', 'float64', and defaults to StringType
      for any other data type.
  """
  if f == 'datetime64[ns]': return TimestampType()
  elif f == 'int64': return LongType()
  elif f == 'int32': return IntegerType()
  elif f == 'float64': return FloatType()
  else: return StringType()


def define_structure(string, format_type):
  """
  Defines the structure for a single field in a Spark DataFrame based on a specified format type.

  Parameters:
      string (str): The name of the field.
      format_type (str): The format type of the field.

  Returns:
      StructField: A StructField object defining the structure for the field with the specified name
                    and format type.

  Note:
      This function is typically used in conjunction with 'pandas_to_spark' to convert Pandas DataFrame
      columns to Spark DataFrame columns. It attempts to determine the equivalent Spark SQL data type for
      the given format type and creates a StructField object accordingly. If no equivalent type is found,
      it defaults to StringType.
  """
  try: typo = equivalent_type(format_type)
  except: typo = StringType()
  return StructField(string, typo)


def pandas_to_spark(pandas_df):
  """
  Converts a Pandas DataFrame to a Spark DataFrame.

  Parameters:
      pandas_df (DataFrame): The Pandas DataFrame to be converted.

  Returns:
      DataFrame: A Spark DataFrame converted from the input Pandas DataFrame.

  Note:
      This function converts a Pandas DataFrame to a Spark DataFrame by creating a schema based on
      the data types of the Pandas DataFrame columns. It iterates over the columns and their corresponding
      data types, defines the structure for each column using the 'define_structure' function, and creates
      a StructType schema. Finally, it creates a Spark DataFrame using the defined schema and the data from
      the Pandas DataFrame.
  """
  try:
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
  except Exception as e:
    print('⚠️ Exception occurred in pandas_to_spark() function: ' + str(e) + ' ⚠️')
  finally:
    return sqlContext.createDataFrame(pandas_df, p_schema)
def SPO_authentication(source_URL, kv_name, sp_store_conn_id, sp_store_secret_name):
  '''
  Authenticates to SharePoint site using SPN credentials.

  Parameters:
      source_URL (str): The URL of the SharePoint site.
      kv_name (str): The name of the Azure Key Vault.
      sp_store_conn_id (str): The ID of the SharePoint storage connection.
      sp_store_secret_name (str): The name of the secret for the SharePoint storage connection.

  Returns:
      tuple: A tuple containing the authenticated web object, the client context, and the current timestamp.
  '''
  # Get the client ID and secret from the Azure Key Vault
  app_principal = {
  'client_id': dbutils.secrets.get(kv_name, sp_store_conn_id),
  'client_secret': dbutils.secrets.get(kv_name, sp_store_secret_name)
  }
  
  # Authenticate to SharePoint site
  ctx_auth = AuthenticationContext(source_URL)

  # Authenticate to SharePoint site
  try:
    if ctx_auth.acquire_token_for_app(client_id=app_principal['client_id'], client_secret=app_principal['client_secret']):
      ctx = ClientContext(source_URL, ctx_auth)
      web = ctx.web
      ctx.load(web)
      ctx.execute_query()
      print('>>> Authenticated to SharePoint site: ' , web.properties['Title'])
      return web, ctx, datetime.now()
  except Exception as e:
    raise Exception(f'⚠️ Exception occurred in SPO_authentication() function for source_URL {source_URL}: ' + str(e) + ' ⚠️')
  finally:
    print_boundary(NUMBER_OF_DASHES)
    


def get_file_list(web, ctx, folder_relative_path, max_retries=3):
  '''
  Returns a list of files at the specified URL relative to the server.

  Parameters:
      web: The authenticated web object.
      ctx: The client context.
      folder_relative_path (str): The relative path of the folder.

  Returns:
      dict: A dictionary indicating success status and the list of files.
  '''
  try:
    libRoot = ctx.web.get_folder_by_server_relative_url(folder_relative_path)
    files = libRoot.files
    ctx.load(files)
    ctx.execute_query()
    return {'success': True, 'result': files}
  except Exception as e:
    if max_retries > 0:
      time.sleep(60)
      return get_file_list(web, ctx, folder_relative_path, max_retries-1)
    else:
      return {'success': False, 'result': 'get_file_list(): ' + str(e)}



def get_folder_list(web, ctx, folder_relative_path):
  '''
  Returns a list of folders at the specified URL relative to the server.

  Parameters:
      folder_relative_path (str): The relative path of the folder.

  Returns:
      dict: A dictionary indicating success status and the list of folders.
  '''
  try:
    libRoot = ctx.web.get_folder_by_server_relative_url(folder_relative_path)
    folders = libRoot.folders
    ctx.load(folders)
    ctx.execute_query()
    return {'success': True, 'result': folders}
  except Exception as e:
    return {'success': False, 'result': 'get_folder_list(): ' + str(e)}



def copy_file(ctx, tgt_conn_key, server_rel_url, tgt_container, tgt_storage, tgt_container_rel_file_path):
  '''
  Copies a file from a SharePoint folder to a Storage Account.

  This function retrieves a file from a specified location in a SharePoint folder and uploads it to a designated container within a Storage Account. It handles the process of replacing special characters in the file URL, reading the file content from SharePoint, and uploading it to the specified container in the Storage Account.

  Parameters:
      ctx: The client context for SharePoint, providing access to SharePoint resources.
      tgt_conn_key: The key used for authentication to the target storage account.
      server_rel_url (str): The server-relative URL of the SharePoint directory.
      tgt_container (str): The name of the container within the target storage account where the file will be uploaded.
      tgt_container_rel_file_path (str): The path of the file relative to the target container.

  Returns:
      tuple: A tuple containing a boolean flag indicating whether the file copy was successful (`True` for success, `False` for failure) and an error message (if any) describing the reason for failure.
  '''
  try:
    # Replace special characters in the URL
    server_rel_url = server_rel_url.replace('%', '%25').replace('#', '%23').replace('$', '%24').replace('&', '%26').replace("'", '%27').replace('%20', ' ')

    # Read the file from SharePoint
    with File.open_binary(ctx, server_rel_url) as resp:
        file_content = resp.content

    # Create a BlobService object for the target storage account
    account_url = 'https://' + tgt_storage + '.blob.core.windows.net'
    blobService = BlobClient(account_url = account_url, container_name = tgt_container, blob_name = tgt_container_rel_file_path, credential = tgt_conn_key)

    # Upload the file to the storage account
    blobService.upload_blob(resp.content, overwrite = True)                                             #★★★★★
    
    return True, '' # File copy successful
  except Exception as e:
    return False, f'copy_file(): {str(e)}' # File copy failed with error message



def whether_to_exclude(file_name, list_of_files_to_exclude):
  '''
    Determines whether a file should be excluded based on its name.

    Parameters:
        file_name (str): The name of the file to check.
        list_of_files_to_exclude (list): A list of file names or patterns to exclude.

    Returns:
        tuple: A tuple containing a boolean value indicating whether to exclude the file,
               and a message indicating the reason if it should be excluded.
    '''
  try:
    if list_of_files_to_exclude == '' or list_of_files_to_exclude == ['']:
      return (False,'')
    elif len(list_of_files_to_exclude) > 0 and re.compile('|'.join(list_of_files_to_exclude),re.IGNORECASE).search(file_name):
      return (True, 'Classified file name')
    else:
      return (False, '')
  except Exception as e:
    print('⚠️ Exception occurred in whether_to_exclude() function: ' + str(e) + ' ⚠️')

  

def file_name_allowed(file_name, file_names_to_inc):
  '''
    Determines whether a file should be copied over based on its name. Skips the file if the file name doesn't contain file_names_to_inc substring.

    Parameters:
        file_name (str): The name of the file to check.
        file_names_to_inc (list): A list of file names or patterns to include.

    Returns:
        tuple: A tuple containing a boolean value indicating whether to process the file,
               and a message indicating the reason if it should be skipped.
    '''
  try:
    if file_names_to_inc == [] or (len(file_names_to_inc) > 0 and re.compile('|'.join(file_names_to_inc),re.IGNORECASE).search(file_name)):
      return (True, '') # process file
    else:
      return (False, 'Do not include file name') # skip file
  except Exception as e:
    print('⚠️ Exception occurred in file_name_allowed() function: ' + str(e) + ' ⚠️')

  

def file_ext_allowed(file_name, file_extns_to_include):
  '''
  Determines whether a file should be copied over based on its extension.

  Parameters:
      file_name (str): The name of the file to check.
      file_extns_to_include (list): A list of allowed file extensions.

  Returns:
      tuple: A tuple containing a boolean value indicating whether to process the file,
              and a message indicating the reason if it should be skipped.
  '''
  try:
    if bool(set([file_name.split('.')[-1]]) & set(file_extns_to_include.split(','))):
      return (True, '') # process
    else:
      return (False, 'Classified file extension') # skip
  except Exception as e:
    print('⚠️ Exception occurred in file_ext_allowed() function: ' + str(e) + ' ⚠️')
  

  
def whether_to_include(file_name, file_names_to_inc, file_extns_to_include):
  '''
  Determines whether a file should be copied over based on its name and extension.

  Parameters:
      file_name (str): The name of the file to check.
      file_names_to_inc (str): A comma-separated string of allowed file names or patterns.
      file_extns_to_include (str): A comma-separated string of allowed file extensions.

  Returns:
      tuple: A tuple containing a boolean value indicating whether to process the file,
              and a message indicating the reason if it should be skipped.
  '''
  name_allowed, extn_allowed, msg = False, False, ''
  try:
    file_name, file_names_to_inc, file_extns_to_include = file_name.lower(), file_names_to_inc.lower(), file_extns_to_include.lower()
    
    if not file_names_to_inc:
      name_allowed = True
    else:
      name_allowed, msg = file_name_allowed(file_name, file_names_to_inc)

    if not file_extns_to_include:
      extn_allowed = True
    else:
      extn_allowed, msg = file_ext_allowed(file_name, file_extns_to_include)

    if name_allowed and extn_allowed:
      return (True, '') # process file
    else:
      return (False, msg) # skip file
  except Exception as e:
    print('⚠️ Exception occurred in whether_to_include() function: ' + str(e) + ' ⚠️')
def get_processing_metadata_NON_RECURSIVE(file, folder_path, source_df):
  '''
  Retrieves metadata of files being processed non-recursively.

  Parameters:
      file (object): The file object.
      folder_path (str): The source folder path.
      source_df (DataFrame): The DataFrame containing source file information.

  Returns:
      tuple: A tuple containing the start time, file size in MB, source URL,
              and target file path.
  '''
  try:
      start_time = datetime.now()
      size_in_MB = round(int(file.properties.get('Length')) / (1024 * 1024), 4)
      source_URL = 'https://bp365.sharepoint.com' + file.properties.get('ServerRelativeUrl')
      target_file_path = source_df.loc[source_df['docHubPath'] == folder_path, 'targetFilePath'].iloc[0] + file.name
      target_file_path = target_file_path.replace('%20', ' ')
      print(f'• Processing ⮞ {file.name}')

      return (start_time, str(size_in_MB), source_URL, target_file_path)

  except Exception as e:
      print('⚠️ Exception occurred in get_processing_metadata_NON_RECURSIVE() function: ' + str(e) + ' ⚠️')





def append_to_ADLS(pandas_df):
    """
    Appends a DataFrame to the runLog table (unstructured_runlog) under the unstruct_metadata schema.

    Parameters:
        pandas_df (DataFrame): The Pandas DataFrame to save.

    Returns:
        None
    """
    try:
        if len(pandas_df) > 0:
            spark_run_df = pandas_to_spark(pandas_df)
            spark_run_df = spark_run_df.withColumn('_year', spark_run_df['_year'].cast('integer')) \
                           .withColumn('_month', spark_run_df['_month'].cast('integer'))
            spark_run_df.write.format('delta').mode('append').partitionBy('_year', '_month') \
            .saveAsTable('unstruct_metadata.runlog_unified')
        print(f'>>> Appended {len(pandas_df)} row(s) to unstruct_metadata.runlog_unified 💾')
    except Exception as e:
        print(f'⚠️ Exception occurred in append_to_ADLS() function: {e} ⚠️')





def intermediate_append_to_ADLS(run_df):
  '''
  Intermediately saves the run table DataFrame (run_df) to the runLog table (unstructured_runlog) under the unstruct_metadata schema.
  Deletes the DataFrame to avoid memory cache issues.

  Parameters:
      run_df (DataFrame): The DataFrame to save.

  Returns:
      DataFrame: An empty DataFrame with columns specified by the constant RUN_LOG_COLS.
  '''
  try:
    # Calculate the sum of file sizes in MB
    sum_MB = round(sum(pd.to_numeric(run_df['fileSizeMB'])), 3)

    # Print status message
    msg1 = f' {sum_MB} MB proccessed - intermediate write to runLog...'
    msg2 = '═'*(len(msg1)+2)
    print(f'\n{msg2}\n{msg1}')

    # Append DataFrame to runLog Table (in ADLS) and clear DataFrame from memory
    append_to_ADLS(run_df)
    del run_df
    print(f'{msg2}\n')
    return pd.DataFrame(columns = RUN_LOG_COLS)
  
  except Exception as e:
    print('⚠️ Exception occurred in intermediate_append_to_ADLS() function: ' + str(e) + ' ⚠️')




def append_to_runLog_df(start_time, file_status, err_msg, file_name, size_MB, source_URL, target_file_path, run_df):
    """
    Updates the metadata in the run table DataFrame (run_df) to include the current file being processed.
    Metadata logged includes job group, job order, run start time, run end time, run status, error message,
    file name, file size in MB, file's source path, and file's target path.
    Once 10,000 rows have accumulated, the run table DataFrame is written to the runLog and then truncated to avoid memory cache issues.

    Parameters:
        start_time (datetime): The start time of the current file processing.
        file_status (str): The copy status of the file.
        err_msg (str): The error message, if any.
        file_name (str): The name of the file being processed.
        size_MB (float): The size of the file in MB.
        source_URL (str): The source SharePoint URL path of the file.
        target_file_path (str): The target path of the file.
        run_df (DataFrame): The DataFrame containing metadata for the run table.

    Returns:
        DataFrame: The updated DataFrame containing metadata for the run table.
    """
    try:
        end_time = datetime.now()
        job_df = pd.DataFrame(
            [[int(JOB_GROUP), int(JOB_ORDER), start_time, end_time, file_status, err_msg, file_name, size_MB, source_URL, target_file_path, start_time.year, start_time.month]],
            columns=RUN_LOG_COLS
        )
        run_df = pd.concat([job_df, run_df])

        if len(run_df) > 10000:
            run_df = intermediate_append_to_ADLS(run_df)

        return run_df

    except Exception as e:
        print(f'⚠️ Exception occurred in append_to_runLog_df() function: {e} ⚠️')




def update_unstructpathmappings_lastingestiondate(sp_doc_hub_path):
  '''
  Updates the unstruct_metadata.unstructpathmappings table's last ingestion date column to the current time.

  Parameters:
      sp_doc_hub_path (str): The SharePoint document hub path.

  Returns:
      None
  '''
  try:
    time_now = datetime.now() - timedelta(hours = 1)
    query = f"""
            UPDATE unstruct_metadata.unstructpathmappings 
            SET LastIngestionDate = '{time_now}' 
            WHERE jobQueueReference = '{JOB_GROUP}'
            AND docHubPath = '{sp_doc_hub_path}'
    """
    spark.sql(query)                                                          #★★★★★
    print(f'>>> Updated unstruct_metadata.unstructpathmappings - LastIngestionDate = {time_now}')

  except Exception as e:
    print('⚠️ Exception occurred in update_unstructpathmappings_lastingestiondate() function: ' + str(e) + ' ⚠️')

  finally:
      print_boundary(NUMBER_OF_DASHES)
      
def clean_recursive_args(server_rel_url, server_rel_root, az_strg_root_path):
  '''
  Prepares specific arguments for recursive copying.

  Parameters:
      server_rel_url (str): The server-relative URL of the SharePoint directory.
      server_rel_root (str): The server-relative root path of the SharePoint site.
      az_strg_root_path (str): The Azure Storage root folder path within which Blobs are to be created.

  Returns:
      tuple: A tuple containing cleaned versions of the provided arguments.
  '''
  try:
    server_rel_url = server_rel_url.replace('%20', ' ').rstrip('/')
    server_rel_root = server_rel_root.replace('%20', ' ').rstrip('/')
    az_strg_root_path = az_strg_root_path.replace('%20', ' ').rstrip('/') + '/'
    return server_rel_url, server_rel_root, az_strg_root_path
  
  except Exception as e:
    print('⚠️ Exception occurred in clean_recursive_args() function: ' + str(e) + ' ⚠️')


def get_folder_info(source_df, folder_path):
  '''
  Retrieves folder information needed for processing.

  Parameters:
      source_df (DataFrame): The DataFrame containing information from the unstructmetadata.unstructpathmappings table.
      folder_path (str): The SharePoint folder path.

  Returns:
      tuple: A tuple containing server_rel_url, target_file_path, file_names_to_exc, file_names_to_inc, and file_extns_to_include.
  '''
  try:
    folder_info = source_df[source_df['docHubPath'] == folder_path].iloc[0]

    server_rel_url = folder_path.split('https://bp365.sharepoint.com')[1].replace('%20',' ')
    target_file_path = folder_info['targetFilePath'].replace('%20', ' ')
    file_names_to_exc = folder_info['FilesToExclude'].split(',')
    files_to_include = folder_info['FilesToInclude']
    file_names_to_inc = json.loads(files_to_include).get('fileName')
    file_extns_to_include = json.loads(files_to_include).get('fileExt')
    
    return server_rel_url, server_rel_url, target_file_path, file_names_to_exc, file_names_to_inc, file_extns_to_include
  
  except Exception as e:
    print('⚠️ Exception occurred in get_folder_info() function: ' + str(e) + ' ⚠️')
def recursive_fun(server_rel_url, server_rel_root, az_strg_root_path, web, ctx, ctx_auth_ts, is_recursive, last_run_ts, run_status, file_names_to_exc, file_names_to_inc, file_extns_to_include, tgt_container, tgt_storage, tgt_conn_key, load_type, invalid_folder_names):
  '''
    Recursively processes files and folders in a SharePoint directory.

    Parameters:
        server_rel_url (str): The server-relative URL of the SharePoint directory.
        server_rel_root (str): The server-relative root path of the SharePoint site.
        az_strg_root_path (str): The Azure Storage root folder path within which Blobs are to be created.
        web: The authenticated web object.
        ctx: The client context for SharePoint, providing access to SharePoint resources.
        ctx_auth_ts: The time of the last authentication to SharePoint.
        invalid_folder_names: A list to collect folder names containing illegal characters.

    Returns:
        tuple: A tuple containing a DataFrame with run log information, a flag indicating whether all files were copied successfully,
               a flag indicating whether the recursion was completed, the updated web object, the updated client context, 
               the updated authentication timestamp, and the list of invalid folder names.
    '''
  
  server_rel_url, server_rel_root, az_strg_root_path = clean_recursive_args(server_rel_url, server_rel_root, az_strg_root_path)  
  file_status, err_msg, size_MB, source_URL = 'S','','',''
  print(f'>>> Current folder: https://bp365.sharepoint.com{server_rel_url}')
  
  try:
    run_df = pd.DataFrame()
    all_files_copied = True
    retries = 1

    fileListResp = get_file_list(web, ctx, server_rel_url)
    if not fileListResp['success']:
      raise Exception(f"⚠️ Exception occured inside recursive_fun().get_file_list() function: {fileListResp['result']}")
    files = fileListResp['result']

    for file in files:
      file_name = file.name.replace('%20',' ')
      if load_type == 'INC' and ((file.properties.get('TimeLastModified') < last_run_ts) and (file.properties.get('TimeCreated') < last_run_ts)):
        print('\t• Not modified ⮞ ', file.name, '\t\tLast checked ⮞ ', last_run_ts)
        continue

      inc, exc = whether_to_include(file.name, file_names_to_inc, file_extns_to_include), whether_to_exclude(file.name, file_names_to_exc)
      if not(inc[0]) or exc[0]:
        print(f'\t❗ Skipping  ⮞  {file_name} ❗ {inc[1]} {exc[1]}')
        continue

      size_MB = round(int(file.properties.get('Length'))/1024/1024, 4)
      start_time, file_status, err_msg = datetime.now(), 'S', ''
      source_URL = 'https://bp365.sharepoint.com' + file.properties.get('ServerRelativeUrl')
      rootRelativeFile = az_strg_root_path + file.properties.get('ServerRelativeUrl')[len(server_rel_root)+1:]
      az_strg_file_path = az_strg_root_path + file.properties.get('ServerRelativeUrl')[len(server_rel_root)+1:]
      print(f'\t• Processing ⮞ {file.name}')
      all_files_copied, err_msg = copy_file(ctx, tgt_conn_key, file.properties.get('ServerRelativeUrl'), tgt_container, tgt_storage, az_strg_file_path[az_strg_file_path.index('net')+4:])
      if all_files_copied == False:
        file_status = 'F'

      run_df = append_to_runLog_df(start_time, file_status, err_msg, file.name, str(size_MB), source_URL, rootRelativeFile, run_df)

    if len(run_df) > 0:
      run_df = intermediate_append_to_ADLS(run_df)
      file_name, size_MB, targetFilePath = '', '', '', ''
    
    get_folder_list_resp = get_folder_list(web, ctx, server_rel_url)
    if not get_folder_list_resp['success']:
      raise Exception(f"⚠️ Exception occured in recursive_fun().get_folder_list() function: {get_folder_list_resp['result']}")
    folders = get_folder_list_resp['result']

    ctx_auth_ts_hours = (datetime.now() - ctx_auth_ts).total_seconds() / 3600
    if ctx_auth_ts_hours > RE_AUTH_TIME_HOURS:
      print('⌛ Hours elapsed since last SPO authentication: ', round(ctx_auth_ts_hours, 3), '  ➜  Re-authenticating...')
      web, ctx, ctx_auth_ts = SPO_authentication(source_URL, kv_name, intermediate_conn_id, intermediate_secret_name)
      ctx_auth_ts = datetime.now()
      print()

    for folder in folders:    
      folderName = folder.name
      if any(char in folderName for char in SPECIAL_CHARS):
        print(f'   ❗  Skipped Folder - https://bp365.sharepoint.com{server_rel_url}/{folderName} ❗ 𝙞𝙡𝙡𝙚𝙜𝙖𝙡 𝙛𝙤𝙡𝙙𝙚𝙧 𝙣𝙖𝙢𝙚 𝙘𝙝𝙖𝙧𝙖𝙘𝙩𝙚𝙧(𝙨)')
        invalid_folder_names.append(f'https://bp365.sharepoint.com{server_rel_url}/{folderName}')
        continue

      time.sleep(SLEEPTIME_BETWEEN_FOLDERS)
      tempdf, all_files_copied, recursion_complete, web, ctx, ctx_auth_ts, invalid_folder_names = recursive_fun(server_rel_url + '/' + folder.properties["Name"], server_rel_root, az_strg_root_path, web, ctx, ctx_auth_ts, is_recursive, last_run_ts, run_status, file_names_to_exc, file_names_to_inc, file_extns_to_include, tgt_container, tgt_storage, tgt_conn_key, load_type, invalid_folder_names)
      if recursion_complete == False and retries > 0:
        print(f' ❗ Re-processing folder : {folder.properties["Name"]}')
        retries -= 1
        time.sleep(SLEEPTIME_AFTER_ERROR)
        tempdf, all_files_copied, recursion_complete, web, ctx, ctx_auth_ts, invalid_folder_names = recursive_fun(server_rel_url + '/' + folder.properties["Name"], server_rel_root, az_strg_root_path, web, ctx, ctx_auth_ts, is_recursive, last_run_ts, run_status, file_names_to_exc, file_names_to_inc, file_extns_to_include, tgt_container, tgt_storage, tgt_conn_key, load_type, invalid_folder_names)
      run_df = pd.concat([run_df, tempdf])
    
    return run_df, all_files_copied, True, web, ctx, ctx_auth_ts, invalid_folder_names
  
  except Exception as e:
    print('\n⚠️ Exception occurred in recursive_fun() function: ' + str(e) + ' ⚠️')
    return run_df, all_files_copied, False, web, ctx, ctx_auth_ts, invalid_folder_names
def notify_invalid_folders(invalid_folder_names, failure_emails):
    '''
    Notifies about folders with illegal characters and sends an email to notify.

    Parameters:
        invalid_folder_names (list): A list of folder names containing illegal characters.
        failure_emails (list): A list of email addresses to notify about the issue.
    '''
    if len(invalid_folder_names) > 0:
        msg1 = f'Hello,<br><br>{len(invalid_folder_names)} folder(s) were not copied to P82 CGG Blob Storage as their folder names contain illegal character(s). Please modify their folder names accordingly. The SharePoint URL links are given below.<br><br>'
        for i, folder in enumerate(invalid_folder_names, 1):
            parent_folder = '/'.join(folder.split('/')[:-1]).replace(' ', '%20')
            illegal_folder_name = folder.split('/')[-1]
            msg1 += f'{i})<br><b>Parent Folder : </b>{parent_folder} <br><b>Illegal Folder Name : </b>{illegal_folder_name}<br>'

        msg2 = '<br>Thank you,<br>Wells & Subsurface Squad<br><em><br><i>(This is an automated email - please do not respond.)</i></em>'
        finalMsg = msg1 + msg2

        print(f'{len(invalid_folder_names)} folder(s) were not copied to P82 CGG Blob Storage as their folder names contain illegal character(s)')
        
        subject = f'CGG Unstructured Ingestion - Request to Rename {len(invalid_folder_names)} SharePoint Folder(s)'
        
        sendEmail(failure_emails, subject, finalMsg)
        print(f'Email sent to: {failure_emails} with subject: {subject}.')
