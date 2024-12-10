import re
import msal
import json
import requests
import pandas as pd
from datetime import datetime

from msal import ConfidentialClientApplication

from pyspark.sql.types import *
from pyspark.sql.functions import *

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Future

from tableUtils import tableUtils as t
jobGroup = dbutils.widgets.get("jobGroup")
jobOrder = dbutils.widgets.get("jobOrder")
tenantId = 'ea80952e-a476-42d4-aaf4-5457852b0f7e'
authority = "https://login.windows.net/" + tenantId 
resourceAppIdURI = "https://database.windows.net/.default"
auditSPID = dbutils.secrets.get(scope = "ZNEUDL1P40INGing-kvl00", key = "spid-BI-zne-udl1-p-12-udl-rsg")
auditSPPWD = dbutils.secrets.get(scope = "ZNEUDL1P40INGing-kvl00", key = "sppwd-BI-zne-udl1-p-12-udl-rsg")
auditSQLServer = 'zneudl1p12sqlsrv002.database.windows.net'
auditSQLDB = 'zneudl1p12sqldb001'
jdbcURL = "jdbc:sqlserver://" + auditSQLServer + ":1433;database=" + auditSQLDB
tokenURL = 'https://login.microsoftonline.com/' + tenantId + '/oauth2/token'
def getConnection():
  context = ConfidentialClientApplication(auditSPID,auditSPPWD,authority) 
  newtoken = context.acquire_token_for_client(resourceAppIdURI)
  connectionProperties = {"accessToken" : newtoken.get('access_token'), "hostNameInCertificate" : "*.database.windows.net", "encrypt" : "true"}
  return connectionProperties

query = "(select jq.pktblJobQueue,jq.jobGroup,jq.jobOrder,jq.jobNum,jq.jobStepNum,jg.jobGroupName,jq.sourceURL,jq.keyVaultName,jq.sourceDBUser,jq.sourceDBPwdSecretName,jq.sourceDBName,jq.sourceTblName,jq.targetDBName,jq.targetTblName,jq.inscopeColumnList,jq.additionalColumnsInTarget,jq.fkLoadType,jq.fkTargetFileFormat,jg.successNotificationEmailIds from audit.tblJobQueue jq,audit.tblJobGroup jg  where jg.pkTblJobGroup=jq.jobGroup  and jobGroup="+jobGroup+" and jobOrder = "+jobOrder+"  and isActive='Y') as tab"

df = spark.read.jdbc(url=jdbcURL, table=query, properties=getConnection())
display(df)
spID = dbutils.secrets.get(scope=df.collect()[0]['keyVaultName'], key=df.collect()[0]['sourceDBUser'])
spPWD = dbutils.secrets.get(scope=df.collect()[0]['keyVaultName'], key=df.collect()[0]['sourceDBPwdSecretName'])
collection  = df.select('pktblJobQueue','jobGroup','jobOrder','jobNum','jobStepNum','jobGroupName','sourceURL','sourceDBUser','sourceDBPwdSecretName','sourceDBName','sourceTblName','targetDBName','targetTblName','inscopeColumnList','additionalColumnsInTarget','fkLoadType','fkTargetFileFormat','successNotificationEmailIds').toPandas()
jobGroupName = df.collect()[0]['jobGroupName']
toEmailAddress = df.collect()[0]['successNotificationEmailIds']
toEmailList = toEmailAddress.split(',')
sourceDBName = list(collection['sourceDBName'])
sourceTblName = list(collection['sourceTblName'])
sourceURL = list(collection['sourceURL'])
targetDBName = list(collection['targetDBName'])
targetTblName = list(collection['targetTblName'])
pktblJobQueue = list(collection['pktblJobQueue'])
inscopeColumnList = list(collection['inscopeColumnList'])
additionalColumnsInTarget = list(collection['additionalColumnsInTarget'])
fkLoadType = list(collection['fkLoadType'])
fkTargetFileFormat = list(collection['fkTargetFileFormat'])

zippedlist = list(zip(pktblJobQueue,sourceDBName,sourceTblName,sourceURL,targetDBName,targetTblName,inscopeColumnList,additionalColumnsInTarget,fkLoadType,fkTargetFileFormat))
from pyspark.sql.types import *
from pyspark.sql.functions import *


def equivalent_type(f):
  if f == 'datetime64[ns]': 
    return TimestampType()
  elif f == 'int64': 
    return LongType()
  elif f == 'int32': 
    return IntegerType()
  elif f == 'float64': 
    return FloatType()
  else: 
    return StringType()

def define_structure(string, format_type):
  try: 
    typo = equivalent_type(format_type)
  except: 
    typo = StringType()
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

def getAccessToken(spID, spPWD):
  
  payload = {
    'grant_type' : 'client_credentials',
    'client_id' : spID ,
    'client_secret' : spPWD,
    'resource' : 'https://analysis.windows.net/powerbi/api'
  }
  
  headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
  }
  
  response = requests.get(url=tokenURL, headers=headers, data=payload)
  accessToken = response.json()['access_token']
  
  return accessToken
  
def executeDatasetQuery(datasetName, datasetUrl):
  
  try:
    payload = json.dumps({
      "queries": [
        {
          "query": "EVALUATE '{}'".format(datasetName)
        }
      ],
      "serializerSettings": {
        "includeNulls": False
      }
    })
    
    headers = {
      'Authorization':'Bearer '+getAccessToken(spID=spID, spPWD=spPWD),
      'Content-Type':'application/json'
    }
    
    response = requests.post(url=datasetUrl, headers=headers, data=payload)
    
  except Exception as e:
    print("Exception occured in executeDatasetQuery: {}".format(str(e)))
    
  return response

def saveADLS(sparkdf,fkLoadType,targetDbName,targetTblName,fkTargetFileFormat):

  if fkLoadType == 'SNP':
    t.saveAsTable(df = sparkdf, write_mode = "overwrite", full_target_table_name = f'{targetDbName}.{targetTblName}', debug = True)
  
  elif fkLoadType == 'APPEND':
    # sparkdf.write.format(fkTargetFileFormat).mode("append").insertInto(targetDbName + '.' + targetTblName)
    t.saveAsTable(df = sparkdf, write_mode = "append", full_target_table_name = f'{targetDbName}.{targetTblName}', debug = True)
  
  else:
    raise Exception('Exception in saveADLS(): Incorrect fkLoadType provided!!')
tbldict = {}

def processDatasets(zippedlist):
  
  pktblJobQueue = zippedlist[0]
  datasetID = zippedlist[1]
  datasetName = zippedlist[2]
  datasetUrl = zippedlist[3]
  targetDbName = zippedlist[4]
  targetTblName = zippedlist[5]
  inscopeColumnList = zippedlist[6].split(',')
  additionalColumnsInTarget = zippedlist[7]
  fkLoadType = zippedlist[8]
  fkTargetFileFormat = zippedlist[9]
  
  startTime = datetime.now()
  
  print('Dataset processing started for: {0}.{1}'.format(targetDbName,targetTblName))
  
  try:
    
    response = executeDatasetQuery(datasetName=datasetName, datasetUrl=datasetUrl)
    
    if response.status_code == 200:
      data = json.loads(response.content.decode('utf-8-sig'))
      dataset  = data['results'][0]['tables'][0]['rows']
      df = pd.DataFrame(dataset)
      df.columns = df.columns.str.replace(datasetName, "")
      df.columns = df.columns.str.replace("[", "",regex=True)
      df.columns = df.columns.str.replace("]", "",regex=True)
      # df = df.dropna(subset=['Name'])
      df = df[inscopeColumnList]
      
      if additionalColumnsInTarget :
        df['load_ts'] = datetime.utcnow()
      
      sparkdf = pandas_to_spark(df)
      saveADLS(sparkdf,fkLoadType,targetDbName,targetTblName,fkTargetFileFormat)
      
      recInSource = sparkdf.count()
      status = "S"    
      recFailed = 0
      errorMsg = 'N/A'
      processingDetails = '{}.{} Dataset processing is successful.'.format(targetDbName,targetTblName)
      
    else:
      print('Execute DatasetQuery Failed with: '+str(response.status_code))
      raise Exception
    
    
  except Exception as e:
    status = "F"
    recInSource = 0
    recFailed = 0
    errorMsg = str(e)
    processingDetails = '{}.{} Dataset processing has failed.'.format(targetDbName,targetTblName)
  
  endTime = datetime.now()
  
  tbldict = json.dumps({
      'pktblJobQueue': pktblJobQueue,
      'targetTblName': targetTblName,
      'startTime': str(startTime),
      'endTime': str(endTime),
      'status': status,
      'processingDetails': processingDetails,
      'errorMsg' : errorMsg,
      'recInSource' : recInSource,
      'recIngested' : recInSource,
      'recProcessed' : recInSource,
      'recFailed' : recFailed})

  
  return tbldict
def tryFuture(future:Future):
  return json.loads(future.result())


def executetables(zippedlist, maxParallel:int):
   
  print(f"Processing {len(zippedlist)} tables with a maxParallel of {maxParallel}")
  with ThreadPoolExecutor(max_workers=maxParallel) as executor:
 
    results = [executor.submit(processDatasets, table) for table in zippedlist ]
   
  return [tryFuture(r) for r in results]
results = executetables(zippedlist,3)
logdf = pd.DataFrame(results)
JobURL = ''

notebook_details_json = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()).get('tags')

if None == notebook_details_json.get('jobId'):
    # print('Executed from interactive cluster.')
    JobURL = 'https://' + notebook_details_json.get('browserHostName') + '/?o=' + notebook_details_json.get(
        'orgId') + '#' + '/'.join(notebook_details_json.get('httpTarget').split('/')[1:])
else:
    # print('Executed from job cluster.')
    JobURL = 'https://northeurope.azuredatabricks.net?o=' + notebook_details_json.get(
        'orgId') + '#job/' + notebook_details_json.get('jobId') + '/run/' + notebook_details_json.get('runId')
finaldf = pd.merge(logdf,collection,on='pktblJobQueue',how='left')
finaldf['runID'] = JobURL
tabledf = finaldf[['runID','pktblJobQueue','jobGroup','jobNum','jobOrder','jobStepNum','startTime','endTime','status','processingDetails','errorMsg','recInSource','recIngested','recProcessed','recFailed']].rename({'pktblJobQueue':'fkJobQueue'},axis=1)
fTableList = list(finaldf[finaldf['status']=='F']['targetDBName'].str.cat(finaldf['targetTblName_y'].astype(str), sep='.'))
import pyspark
from pyspark.sql import SparkSession
 
spark = SparkSession.builder.getOrCreate()
 
sparkdf = spark.createDataFrame(tabledf)
display(sparkdf)
sparkdf = sparkdf.withColumn("startTime",col("startTime").cast("timestamp")).withColumn("endTime",col("endTime").cast("timestamp"))
sparkdf.write.mode("append").jdbc(url=jdbcURL,table="audit.tblrunlog", properties=getConnection())
if fTableList:
  raise Exception(str(len(fTableList))+ " out of "+str(len(tabledf))+" tables Failed.")
