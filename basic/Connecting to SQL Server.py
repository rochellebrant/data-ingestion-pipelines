from msal import ConfidentialClientApplication

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
  context = ConfidentialClientApplication(ENV_ServicePrincipalId, ENV_ServicePrincipalPwd, ENV_Authority)
  newtoken = context.acquire_token_for_client(ENV_ResourceAppIdURI)
  connectionProperties = {"accessToken" : newtoken.get('access_token'), "hostNameInCertificate" : "*.database.windows.net", "encrypt" : "true"}
  return connectionProperties


def get_metadata(jobGroup, jobOrder, jobNum, columns: list):
   
    condition = [f"jobGroup={jobGroup}", f"isActive='Y'"]
    if int(jobOrder) > 0:
        condition.append(f"jobOrder={jobOrder}")
    if int(jobNum) > 0:
        condition.append(f"jobNum={jobNum}")
    query = f"(SELECT {', '.join(columns)} FROM audit.tblJobQueue WHERE {' AND '.join(condition)}) AS tab"
    query_df = spark.read.jdbc(url=JDBC_url, table=query, properties=get_connection())    
    return query_df.collect()
metadataFields = ["pktblJobQueue", "fkLoadType", "jobGroup", "jobOrder", "jobNum", "filterQuery", "sourceURL", "keyVaultName", "intermediateStoreConnID", "intermediateStoreSecretName", "inscopeColumnList", "targetFilePath", "targetStorageAccount", "targetContainer", "targetStoreConnID", "failureNotificationEmailIDs"]

jobGroup = 524
jobOrder = 1
jobNum = 0
queryDF = get_metadata(jobGroup, jobOrder, jobNum, metadataFields)
display(queryDF)
