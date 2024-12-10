%run ./ModuleFunctions

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer
import java.text._
import java.util.Properties
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager

val start_time = java.sql.Timestamp.valueOf(LocalDateTime.now)
val workspace_id= spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
val job_id = dbutils.notebook.getContext.tags("jobId").toString()
val url_return = "https://northeurope.azuredatabricks.net/?o="+workspace_id+"#job/"+job_id+"/run/1"
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true") //Resolve Full load issue of notebook detach while running
var credentials = scala.collection.mutable.Map[String, String]()
var recInSrc:Long = 0
var recFail:Long = 0
var recIng: Long = 0L
var operation =""
var errMsg = ""
var STATUS="R" 
var processingDetails = ""
var query = ""
var fail = ""
var subject = ""
var tableDF: org.apache.spark.sql.DataFrame = null

var auditDBSPId =""
var auditDBSPPwd = ""
var auditDBAccessToken = ""
var currentRow:org.apache.spark.sql.DataFrame = null

//Capture data from parent notebook
val pkTblJobQueue = dbutils.widgets.get("pkTblJobQueue").toInt
val jobGroup = dbutils.widgets.get("jobGroup").toInt
val jobOrder = dbutils.widgets.get("jobOrder").toInt
val jobNum = dbutils.widgets.get("jobNum").toInt
val jobStepNum = dbutils.widgets.get("jobStepNum").toInt
val failureNotificationEmailIDs = dbutils.widgets.get("failureNotificationEmailIDs")
val keyVaultName = dbutils.widgets.get("keyVaultName")
val pipelineName = dbutils.widgets.get("pipelineName")
val dataFactoryName = dbutils.widgets.get("dataFactoryName")

auditDBSPId = dbutils.secrets.get(scope = keyVaultName, key = ENV_AuditDBSPKey)
auditDBSPPwd = dbutils.secrets.get(scope = keyVaultName, key = ENV_AuditDBSPKeyPwd)
// auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)

//JDBC Connection
var connectionProperty:Properties = null
var connection : Connection = null 
var preparedStatement : PreparedStatement  = null

try
{    
  auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
  connectionProperty = getJDBCConnectionProperty(auditDBAccessToken, "36000")
  connection = DriverManager.getConnection(JDBC_url,connectionProperty)
  operation = "JOB_STARTED"
  query = "EXEC [audit].[SP_LOG_RUN_DETAILS_NEW] @operation = N'"+ operation +"', @runID = N'"+job_id+"',@fkJobQueue = "+pkTblJobQueue+", @jobGroup = "+jobGroup+", @jobNum = "+jobNum+", @jobOrder = "+jobOrder+", @jobStepNum = "+jobStepNum+", @status = N'"+STATUS+"', @processingDetails = N'"+processingDetails+"', @errorMsg = N'"+errMsg+"', @recInSource = NULL, @recIngested = NULL, @recProcessed = NULL, @recFailed = NULL"
println(query)
//   sqlContext.sqlDBQuery( sqlDBquery(ENV_AuditTableServer, ENV_AuditTableDB, query, auditDBAccessToken)) 
  preparedStatement = connection.prepareStatement(query)
  preparedStatement.executeUpdate()
}catch{
  case ex: Exception => errMsg = "Azure Data Factory "+dataFactoryName + ", pipeline "+ pipelineName+".\n\nLocation: The child notebook is failing for API Ingestion of jobGroup "+jobGroup+" and jobOrder"+jobOrder+ "table while inserting records in runlog. \n\n Please click link for further details of error: \n" +ex;
        STATUS = "F"
        subject += " is Failing"
        sendEmail(errMsg, subject, failureNotificationEmailIDs);
}

val selectQuery="(select jq.pkTblJobQueue, jq.jobGroup, jq.jobNum, jq.jobOrder, jq.jobStepNum, jq.fkJobType, jq.fkJobStep, jq.fkLoadType, jq.fkSourceApplication, jq.sourceTblName, jq.sourcePKCols, jq.sourceChangeKeyCols, jq.excludeColumns, jq.targetDBName, jq.targetExtTblSchemaName, jq.targetTblName, jq.targetStorageAccount, jq.targetContainer, jq.extTbDataSource, jq.forceTblRecreate, jq.targetFilePath, jq.targetFileName, jq.fkTargetFileFormat, jq.successNotificationEmailIDs, jq.failureNotificationEmailIDs, jq.IsActive,  jq.keyVaultName, jq.inscopeColumnList, jq.sourceURL, jq.sourceFilePath , jq.sourceFileName, jq.additionalColumnsInTarget, jg.tokenURL, jq.filterQuery, jg.tokenCredentialKeyList, jg.tokenCredentialKVNameList, jg.fkTokenRequestMethod, jg.fkTokenAuthType, jg.fkTokenAuthParamPassingMethod, jg.tokenRespContentType, jg.srcAuthCredentialKeyList, jg.srcAuthCredentialKVNameList, jg.fkSrcRequestMethod, jg.fkSrcAuthType, jg.fkSrcAuthParamPassingMethod, jg.fkSrcResponseType, jg.fkSrcResponseFormat, jg.srcRespContentType, jg.hasPagination, jg.paginationURLKeyword, jg.paginationURLLocation, jg.paginationType, jg.paginationAdditionalParams, jq.sourceChgKeyLatestValues, jq.deleteOperationTableName, jq.XMLNodesTillDataNode, jg.APIPostRequestBody, jq.sourceTimestampFormat, jq.targetStoreKeyName, jq.target2StorageAccount, jq.target2Container, jq.target2FilePath, jq.target2FileName, jq.transformedColumnsInTarget FROM  audit.tblJobQueue jq, audit.tblJobQueueExtn jg WHERE jg.fkJobQueue=jq.pkTblJobQueue AND jq.jobGroup="+jobGroup+" AND jq.jobOrder = "+jobOrder+" AND jobNum="+jobNum+" AND jq.isActive='Y') as test"
currentRow = spark.read.jdbc(url=JDBC_url, table=selectQuery, properties=connectionProperty)
//   currentRow = sqlContext.read.sqlDB(sqlDBTable(ENV_AuditTableServer, ENV_AuditTableDB, selectQuery, auditDBAccessToken))
display(currentRow)

val successNotificationEmailIDs = currentRow.select("successNotificationEmailIDs").take(1)(0)(0).toString()
val pkJobQueue = currentRow.select("pkTblJobQueue").take(1)(0)(0).toString()
val fkJobType = currentRow.select("fkJobType").take(1)(0)(0).toString()
val fkJobStep = currentRow.select("fkJobStep").take(1)(0)(0).toString()
val loadType = currentRow.select("fkLoadType").take(1)(0)(0).toString()
val fkSourceApplication = currentRow.select("fkSourceApplication").take(1)(0)(0).toString()
val sourceTblName = currentRow.select("sourceTblName").take(1)(0)(0).toString()
val sourcePKCols = currentRow.select("sourcePKCols").take(1)(0)(0).toString()
val selectCols = currentRow.select("sourceChangeKeyCols").take(1)(0)(0).toString()
val sourceChangeKeyCols = currentRow.select("sourceChangeKeyCols").take(1)(0)(0).toString()
val excludeColumns = currentRow.select("excludeColumns").take(1)(0)(0).toString()
val TARGET_DATABASE = currentRow.select("targetDBName").take(1)(0)(0).toString().toUpperCase()
val targetExtTblSchemaName = currentRow.select("targetExtTblSchemaName").take(1)(0)(0).toString()
val TARGET_TABLE = currentRow.select("targetTblName").take(1)(0)(0).toString()
val targetStorageAccount = currentRow.select("targetStorageAccount").take(1)(0)(0).toString()
val targetContainer = currentRow.select("targetContainer").take(1)(0)(0).toString()
val extTbDataSource = currentRow.select("extTbDataSource").take(1)(0)(0).toString()
val forceTblRecreate = currentRow.select("forceTblRecreate").take(1)(0)(0).toString()
val targetFilePath = currentRow.select("targetFilePath").take(1)(0)(0).toString()
val targetFileName = currentRow.select("targetFileName").take(1)(0)(0).toString()
val fkTargetFileFormat = currentRow.select("fkTargetFileFormat").take(1)(0)(0).toString()
val IsActive = currentRow.select("IsActive").take(1)(0)(0).toString()
val keyVaultName = currentRow.select("keyVaultName").take(1)(0)(0).toString()
val inscopeColumnList= currentRow.select("inscopeColumnList").take(1)(0)(0).toString()
val tokenURL = currentRow.select("tokenURL").take(1)(0)(0).toString()
val tokenClientId = currentRow.select("tokenCredentialKeyList").take(1)(0)(0).toString()
val tokenClientCredential = currentRow.select("tokenCredentialKVNameList").take(1)(0)(0).toString()
val fkTokenRequestMethod = currentRow.select("fkTokenRequestMethod").take(1)(0)(0).toString()
val fkTokenAuthType = currentRow.select("fkTokenAuthType").take(1)(0)(0).toString()
val fkTokenAuthParamPassingMethod = currentRow.select("fkTokenAuthParamPassingMethod").take(1)(0)(0).toString()
val fkTokenRespContentType = currentRow.select("tokenRespContentType").take(1)(0)(0).toString()
var sourceURL = currentRow.select("sourceURL").take(1)(0)(0).toString()
val apiAuthId = currentRow.select("srcAuthCredentialKeyList").take(1)(0)(0).toString()
val apiTokens = currentRow.select("srcAuthCredentialKVNameList").take(1)(0)(0).toString()
val apiReqMethod = currentRow.select("fkSrcRequestMethod").take(1)(0)(0).toString()
val apiAuthType = currentRow.select("fkSrcAuthType").take(1)(0)(0).toString()
val apiAuthWay = currentRow.select("fkSrcAuthParamPassingMethod").take(1)(0)(0).toString()
val apiResponseType = currentRow.select("fkSrcResponseType").take(1)(0)(0).toString()
val apiResponseFormat = currentRow.select("fkSrcResponseFormat").take(1)(0)(0).toString()
val apiReqContentType = currentRow.select("srcRespContentType").take(1)(0)(0).toString()
val hasPagination = currentRow.select("hasPagination").take(1)(0)(0).toString()
val paginationURLKeyword = currentRow.select("paginationURLKeyword").take(1)(0)(0).toString()
val paginationURLLocation = currentRow.select("paginationURLLocation").take(1)(0)(0).toString()
val paginationType = currentRow.select("paginationType").take(1)(0)(0).toString()
val additionalParams = currentRow.select("paginationAdditionalParams").take(1)(0)(0).toString()
val filePath = currentRow.select("sourceFilePath").take(1)(0)(0).toString()
val fileName = currentRow.select("sourceFileName").take(1)(0)(0).toString()
val columnChangeSchema = currentRow.select("additionalColumnsInTarget").take(1)(0)(0).toString()
val sourceChgKeyLatestValues = currentRow.select("sourceChgKeyLatestValues").take(1)(0)(0).toString()
val incQueryPrama = currentRow.select("tokenRespContentType").take(1)(0)(0).toString()
val transformedColumnsInTarget = currentRow.select("transformedColumnsInTarget").take(1)(0)(0).toString()
val sourceTimestampFormat = currentRow.select("sourceTimestampFormat").take(1)(0)(0).toString()
val secondaryBlobStorageKey = currentRow.select("targetStoreKeyName").take(1)(0)(0).toString()
val target2StorageAccount = currentRow.select("target2StorageAccount").take(1)(0)(0).toString()
val target2Container = currentRow.select("target2Container").take(1)(0)(0).toString()
val target2FilePath = currentRow.select("target2FilePath").take(1)(0)(0).toString()
val target2FileName = currentRow.select("target2FileName").take(1)(0)(0).toString()
val deleteTable = currentRow.select("deleteOperationTableName").take(1)(0)(0).toString()

try{
if(apiAuthId.contains(";") && apiTokens.contains(";")){
  val keys =apiAuthId.split(";")
  val vals = apiTokens.split(";")
  println("Both true")
  if(keys.length == vals.length){
  for(i <- 0 until vals.length) {
    if(vals(i).contains("-KV"))
      credentials += (keys(i) -> dbutils.secrets.get(keyVaultName.toString(), vals(i)))
    else if(vals(i).contains("guid"))
      credentials += (keys(i) -> java.util.UUID.randomUUID().toString())
    else
      credentials += (keys(i) -> vals(i))
  }
 }
}else{
  if(apiAuthId != "" && apiAuthId != null){
    println("ONE Parameter")
    if(apiTokens.contains("-KV")){ credentials += (apiAuthId-> dbutils.secrets.get(keyVaultName.toString(), apiTokens)) }
    else{ credentials += (apiAuthId -> apiTokens) }
  }
}

for ((k,v) <- credentials){println(k+" = "+ v)}

if("OAuth2.0".equals(apiAuthType)){
  println(tokenURL)
  var t = ""
  if(tokenClientId.contains("-KV") && tokenClientCredential.contains("-KV")){    
    t = getAPIAccessToken(tokenURL, dbutils.secrets.get(keyVaultName.toString(),tokenClientId),dbutils.secrets.get(keyVaultName.toString(),tokenClientCredential))
  }
  else{ t = getAPIAccessToken(tokenURL, tokenClientId, tokenClientCredential) }
  val bearer_token = "Bearer " + t
  credentials += ("Authorization" -> bearer_token)
}

subject = "Child Job of Databricks pipleline with JobGroup=" + jobGroup +" and JobOrder="+ jobOrder +" and jobNum= "+jobNum +" "
  
// val strg_key = dbutils.secrets.get(keyVaultName.toString(), ENV_storageACKeySecretName)
// spark.conf.set("fs.azure.account.key."+ENV_storagACName,strg_key)
} catch{
  case ex: Exception => errMsg = "Azure Data FActory "+dataFactoryName + " pipeline "+ pipelineName+".\n\nLocation: The child notebook is failing for API Ingestion of "+TARGET_DATABASE+"."+TARGET_TABLE+ " table while processing API Credentials. \n\n Please click link for further details of error: \n" +ex;
        STATUS = "F"
        operation = "JOB_FAILED"
        subject += " is Failing"
        sendEmail(errMsg, subject, failureNotificationEmailIDs);
//         exitNotebook(operation, job_id, pkTblJobQueue, jobGroup, jobNum, jobOrder, jobStepNum, STATUS, processingDetails, errMsg, recInSrc.toInt, recIng.toInt, recInSrc.toInt, recFail.toInt, JDBC_url, connectionProperty)
} 

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

try{
if("Body".equals(apiResponseType) && "JSON".equals(apiResponseFormat)){
  processingDetails = "JSON body of "+ TARGET_TABLE+" of "+fkSourceApplication+"."
  
  if("INC".equals(loadType)){
    if(!incQueryPrama.contains("N/A") && spark.catalog.tableExists(TARGET_DATABASE+"."+TARGET_TABLE)){ // header contains key name 'fromdate'
      val timeQuery="(select fkJobQueue, min(startTime) as maxTime from audit.tblRunLog where fkJobQueue = "+pkTblJobQueue+" group by(fkJobQueue)) as tab"
      val max_time = spark.read.jdbc(url=JDBC_url, table=timeQuery, properties=connectionProperty).select("maxTime").take(1)(0)(0)
      var maxtime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:sss'Z'").format(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(max_time.toString()))
      sourceURL = sourceURL + "&"+incQueryPrama+"'"+maxtime+"'"
    }
  }
  
  println("Source URL    -> " + sourceURL + "\nTarget db.table -> " + TARGET_DATABASE + "." + TARGET_TABLE + "\nCredentials  -> " + credentials.toMap + "\n")
  val resp = getResp(sourceURL, apiReqMethod, apiAuthType, apiAuthWay, additionalParams, credentials.toMap, apiReqContentType)
//   println("   Resp: ", resp)
  if(!resp.isSuccess){println("❌ Status: " + resp.statusLine)}
  if(resp.isSuccess){
    println("✔️ Status: " + resp.statusLine)
    val json = resp.body
    tableDF = flattenDataframe(spark.read.json(Seq(json).toDS).select(selectCols))
//     val tableDF_str = tableDF.select(tableDF.columns.map(c => col(c).cast(StringType)) : _*)
    
    if("Y".equals(hasPagination)){
      val dfs = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], flattenDataframe(tableDF).schema)
      println("BEFORE getAllPagesData()")
      tableDF = getAllPagesData(sourceURL, apiReqMethod, apiAuthType, apiAuthWay, additionalParams, credentials, apiReqContentType, selectCols:String, paginationType, paginationURLKeyword, paginationURLLocation, excludeColumns, dfs)
      println("AFTER getAllPagesData()")
    }
      
  for(col <- tableDF.columns){  tableDF = tableDF.withColumnRenamed(col,col.replaceAll("\\s", "_"))  } // Replace space with _ in column name
    
  println("-----------------------------------------------------------------") 
    
  //Change Type of Column in Dataframe [Eg. All are StringType]
  if(columnChangeSchema!=null && !columnChangeSchema.trim.isEmpty){
    println("BEFORE changeColumnSchema()") 
    tableDF = changeColumnSchema(tableDF, columnChangeSchema)
    println("AFTER changeColumnSchema()")
    println("-----------------------------------------------------------------") 
  }
    
  recInSrc = tableDF.count()
  println("Table count: " + recInSrc)
  println("-----------------------------------------------------------------") 
    
  println("BEFORE saveADLS()")
  saveADLS(tableDF, loadType, excludeColumns, TARGET_DATABASE, TARGET_TABLE, sourcePKCols, deleteTable)
  println("AFTER saveADLS()")

  println("-----------------------------------------------------------------") 
    
  if("INC".equals(loadType)){
    println("🕒 Updating timestamp for INC Load")
    if(apiAuthId.contains("fromdate") && spark.catalog.tableExists(TARGET_DATABASE+"."+TARGET_TABLE)){ // header contains key name 'fromdate'
      val ids = apiAuthId.split(";")
      val vals = apiTokens.split(";")
      for(i <- 0 until ids.length){
        if("fromdate".equals(ids(i))) vals(i) = DateTimeFormatter.ofPattern(sourceTimestampFormat).format(LocalDateTime.now) 
      }
      val tokens = vals.mkString(";")
      println(tokens)
      val updateQuery = "update audit.tblJobQueueExtn set srcAuthCredentialKVNameList = "+tokens+" where fkJobQueue = "+pkJobQueue
      val noOfRowsUpdated = connection.prepareStatement(updateQuery).executeUpdate()
      println("Number Of Rows Updated "+noOfRowsUpdated)
      println("-----------------------------------------------------------------") 
    }    
  }
    
  recIng = recInSrc
  operation = "JOB_SUCCESSFUL"
  STATUS = "S"
  } else throw new Exception(resp.statusLine)
  
}
}catch{
  case ex: Exception =>  errMsg = "Azure Data Factory: " + dataFactoryName + "\nPipeline: " + pipelineName + ".\n\nFailing Location: An API ingestion (JSON response) child notebook is failing. Please check the link for more info.\n" + url_return + "\n" + ex;
        STATUS = "F"
        operation = "JOB_FAILED"
        subject += " is Failing"       
    recFail= recInSrc
    sendEmail(errMsg, subject, failureNotificationEmailIDs);          //     SendEmail()
}

exitNotebook(operation, job_id, pkTblJobQueue, jobGroup, jobNum, jobOrder, jobStepNum, STATUS, processingDetails, errMsg, recInSrc.toInt, recIng.toInt, recInSrc.toInt, recFail.toInt, JDBC_url, connectionProperty)
