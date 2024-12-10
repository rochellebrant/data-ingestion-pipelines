from office365.runtime.auth.authentication_context import AuthenticationContext as ACX
from office365.sharepoint.client_context import ClientContext

from msal import ConfidentialClientApplication

import re
import json
import pandas as pd

from datetime import datetime
from pyspark.sql.functions import *

from tableUtils import tableUtils as t
jobGroup = dbutils.widgets.get("jobGroup")
jobOrder = dbutils.widgets.get("jobOrder")
runID = dbutils.widgets.get("runID")
pipelineTriggerTime = dbutils.widgets.get("pipelineTriggerTime")

TenantId = "ea80952e-a476-42d4-aaf4-5457852b0f7e" 
authority = "https://login.windows.net/" + TenantId 
resourceAppIdURI = "https://database.windows.net/.default"
ServicePrincipalId = dbutils.secrets.get(scope = "ZNEUDL1P40INGing-kvl00", key = "spid-BI-zne-udl1-p-12-udl-rsg")
ServicePrincipalPwd = dbutils.secrets.get(scope = "ZNEUDL1P40INGing-kvl00", key = "sppwd-BI-zne-udl1-p-12-udl-rsg")

def getConnection(): 
  context = ConfidentialClientApplication(ServicePrincipalId, ServicePrincipalPwd, authority) 
  newtoken = context.acquire_token_for_client([resourceAppIdURI])
  connectionProperties = {"accessToken" : newtoken.get('access_token'), "hostNameInCertificate" : "*.database.windows.net", "encrypt" : "true"}
  return connectionProperties

jdbcURL="jdbc:sqlserver://zneudl1p12sqlsrv002.database.windows.net:1433;database=zneudl1p12sqldb001"
query = "(select pktblJobQueue, jobGroup, jobOrder, jobNum, jobStepNum, fkLoadType, sourceURL, keyVaultName, sourceDBConnString, sourceTblName, targetDBName, targetTblName, fkTargetFileFormat, inscopeColumnList, transformedColumnsInTarget, autoInferSchema, additionalColumnsInTarget from audit.tblJobQueue where jobGroup="+jobGroup+" and jobOrder ="+jobOrder+" and isActive='Y') as tab"

query_df = spark.read.jdbc(url=jdbcURL, table=query, properties=getConnection())
query_df = query_df.na.fill("")
display(query_df)
from pyspark.sql.types import *

# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return spark.createDataFrame(pandas_df, p_schema)

  
def make_list():
  temp_list = list()
  for i in columnList.split('|'):
    temp_list.append(i)
  return temp_list


def preprocess_dataframe(df):
  col_names = []
  for _ in df.columns:
    if 'field' in _:
      col_names.append(_.split('_')[1])
    else:
      col_names.append(_)    
  df.columns = col_names
  col_list = list()
  if columnList != '':
    col_list=make_list()
  
  unwanted_cols = set(df.columns) - set(col_list)
  df.drop(unwanted_cols, axis=1, inplace=True)
  df.columns = [re.sub('\W', '_', c) for c in df.columns]
  return df


def rename_columns(df, transformedColumnsInTarget):
  rename_values = transformedColumnsInTarget.split('|')
  for _ in rename_values:
    df = df.withColumnRenamed(_.split('&')[0], _.split('&')[1].replace(' ', '_'))
  return df


def query_large_list(target_list):
  """
  :type target_list: office365.sharepoint.lists.list.List
  """
  output = []
#     paged_items = target_list.items.paged(15, page_loaded=print_progress).get().execute_query()
  paged_items = target_list.items.paged(500).get().execute_query()
  all_items = [item for item in paged_items]
  return all_items


def saveAdls(final_df, dbxTableFormat, targetDBName, targetTblName):
  if dbxTableFormat.lower() == 'delta':
    t.saveAsTable(df = final_df, write_mode = "overwrite", full_target_table_name = f'{targetDBName}.{targetTblName}', debug = True)
  else:
    raise ValueError('Invalid fkTargetFileFormat. Please check inputs..!!')
runStatus = 'R'
run_table_df = pd.DataFrame()
for recNum in range(query_df.count()):
  final_df = pd.DataFrame()
  startTime = datetime.now()

  pktblJobQueue = query_df.collect()[recNum]['pktblJobQueue']
  jobNum = query_df.collect()[recNum]['jobNum']
  jobStepNum = query_df.collect()[recNum]['jobStepNum']
  siteUrl = query_df.collect()[recNum]['sourceURL']
  keyVaultName = query_df.collect()[recNum]['keyVaultName']
  client_id = query_df.collect()[recNum]['sourceDBConnString'].split(';')[0]
  client_secret = query_df.collect()[recNum]['sourceDBConnString'].split(';')[1]
  list_name = query_df.collect()[recNum]['sourceTblName']
  targetDBName = query_df.collect()[recNum]['targetDBName']
  targetTblName = query_df.collect()[recNum]['targetTblName']
  columnList = query_df.collect()[recNum]['inscopeColumnList']
  transformedColumnsInTarget = query_df.collect()[recNum]['transformedColumnsInTarget']
  autoInferSchema = query_df.collect()[recNum]['autoInferSchema']
  dbxTableFormat = query_df.collect()[recNum]['fkTargetFileFormat']
  additionalColumnsInTarget = query_df.collect()[recNum]['additionalColumnsInTarget']

  app_principal = {'client_id': dbutils.secrets.get(scope = keyVaultName, key = client_id),
                   'client_secret': dbutils.secrets.get(scope = keyVaultName, key = client_secret)}
  
  tableStatus, processingDetails, errorMsg, recFailed, recInSource = 'R', '', '', 0, 0
  
  try:
    ctx_auth = ACX(siteUrl)
    if ctx_auth.acquire_token_for_app(client_id=app_principal['client_id'], client_secret=app_principal['client_secret']):
      ctx = ClientContext(siteUrl, ctx_auth)
      web = ctx.web
      ctx.load(web)
      ctx.execute_query()
      print('Authentication Successful for: ',web.properties['Title'])
      
      spList = ctx.web.lists.get_by_title(list_name)
      sp_data = query_large_list(spList)
      listItems = []
      for item in sp_data:
        listItems.append(item.properties)
      print('  >>>  Fetched {0} items from {1} ..'.format(len(listItems), list_name))      
      
      spark.conf.set("spark.sql.execution.arrow.enabled","true")

      df = pd.DataFrame(listItems)

      if len(df) == 0:
          raise ValueError('throwing exception.')

      else:
        df = preprocess_dataframe(df)
        if autoInferSchema == 'false':
          for _ in df.columns:
            df[_] = df[_].apply(str)
        
        final_df = pandas_to_spark(df)
        if transformedColumnsInTarget != '':
          final_df = rename_columns(final_df, transformedColumnsInTarget)
        
        if 'load_ts' in additionalColumnsInTarget:
          final_df = final_df.withColumn('load_ts',lit(pipelineTriggerTime).cast("timestamp"))

        saveAdls(final_df, dbxTableFormat, targetDBName, targetTblName)

        recInSource = final_df.count()
        if dbxTableFormat == 'DELTA':
          spark.sql(f"OPTIMIZE {targetDBName}.{targetTblName}")
        
        processingDetails = f'Successfully ingested {recInSource} records into {targetDBName}.{targetTblName}!'
        print('  >>>  ' + processingDetails + '\n')
        tableStatus = 'S'

  except Exception as e:
    print('  !!!  Error occurred in Authenticating/ fetching/ updating DBX table -- ', str(e))
    tableStatus, runStatus, recFailed, recInSource = 'F', 'F', 0, 0
    errorMsg = str(e)

  finally:
    endTime = datetime.now()
    tbldict = {
      'runID': runID,
      'fkJobQueue': pktblJobQueue,
      'jobGroup': jobGroup,
      'jobNum': jobNum,
      'jobOrder': jobOrder,
      'jobStepNum': jobStepNum,
      'startTime': str(startTime),
      'endTime': str(endTime),
      'status': tableStatus,
      'processingDetails': processingDetails,
      'errorMsg' : errorMsg,
      'recInSource' : recInSource,
      'recIngested' : recInSource,
      'recProcessed' : recInSource,
      'recFailed' : recFailed}
    
    job_df = pd.DataFrame([tbldict])
    run_table_df = pd.concat([job_df, run_table_df])
sparkdf = spark.createDataFrame(run_table_df)
sparkdf = sparkdf.withColumn("startTime",col("startTime").cast("timestamp")).withColumn("endTime",col("endTime").cast("timestamp"))

display(sparkdf)

sparkdf.write.mode("append").jdbc(url=jdbcURL,table="audit.TblRunLog", properties=getConnection())
if runStatus == 'F':
  raise ValueError('Ingestion failed..!!')

print('SharePoint lists successfully ingested for {0} tables'.format(query_df.count()))
