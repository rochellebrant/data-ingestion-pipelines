// Java standard library imports
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text._
import java.util.Properties
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

// Scala standard library imports
import scala.collection.mutable.ArrayBuffer
%md # Run ModuleFunctions
%run ./ModuleFunctions
%md # Declare Global Variables
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
var tableDF: org.apache.spark.sql.DataFrame = null
val separatorLength = 85

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
%md # Insert in RunLog Table
try
{ auditDBSPId = dbutils.secrets.get(scope = keyVaultName, key = auditDBConfig.spKey)
  auditDBSPPwd = dbutils.secrets.get(scope = keyVaultName, key = auditDBConfig.spKeyPwd)  
  var auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
  var connectionProperty = getJDBCConnectionProperty(auditDBAccessToken, "36000")
  var connection = DriverManager.getConnection(JDBC_url, connectionProperty)
  operation = "JOB_STARTED"
  query = "EXEC [audit].[SP_LOG_RUN_DETAILS_NEW] @operation = N'"+ operation +"', @runID = N'"+url_return+"',@fkJobQueue = "+pkTblJobQueue+", @jobGroup = "+jobGroup+", @jobNum = "+jobNum+", @jobOrder = "+jobOrder+", @jobStepNum = "+jobStepNum+", @status = N'"+STATUS+"', @processingDetails = N'"+processingDetails+"', @errorMsg = N'"+errMsg+"', @recInSource = NULL, @recIngested = NULL, @recProcessed = NULL, @recFailed = NULL"
println(query)
  var preparedStatement = connection.prepareStatement(query)
  preparedStatement.executeUpdate()
}catch{
  case ex: Exception => errMsg = "Azure Data Factory "+dataFactoryName + ", pipeline "+ pipelineName+".\n\nLocation: The child notebook is failing for API Ingestion of jobGroup "+jobGroup+" and jobOrder"+jobOrder+ "table while inserting records in runlog. \n\n Please click link for further details of error: \n" +ex;
        STATUS = "F"
}
%md # Parameters from Parent
auditDBSPId = dbutils.secrets.get(scope = keyVaultName, key = auditDBConfig.spKey)
auditDBSPPwd = dbutils.secrets.get(scope = keyVaultName, key = auditDBConfig.spKeyPwd)
var auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
var connectionProperty = getJDBCConnectionProperty(auditDBAccessToken, "36000")

val selectQuery="(select jq.pkTblJobQueue, jq.jobGroup, jq.jobNum, jq.jobOrder, jq.jobStepNum, jq.fkJobType, jq.fkJobStep, jq.fkLoadType, jq.fkSourceApplication, jq.sourceTblName, jq.sourcePKCols, jq.sourceChangeKeyCols, jq.excludeColumns, jq.targetDBName, jq.targetExtTblSchemaName, jq.targetTblName, jq.targetStorageAccount, jq.targetContainer, jq.extTbDataSource, jq.forceTblRecreate, jq.targetFilePath, jq.targetFileName, jq.fkTargetFileFormat, jq.successNotificationEmailIDs, jq.failureNotificationEmailIDs, jq.IsActive,  jq.keyVaultName, jq.inscopeColumnList, jq.sourceURL, jq.sourceFilePath , jq.sourceFileName, jq.additionalColumnsInTarget, jg.tokenURL, jq.filterQuery, jg.tokenCredentialKeyList, jg.tokenCredentialKVNameList, jg.fkTokenRequestMethod, jg.fkTokenAuthType, jg.fkTokenAuthParamPassingMethod, jg.tokenRespContentType, jg.srcAuthCredentialKeyList, jg.srcAuthCredentialKVNameList, jg.fkSrcRequestMethod, jg.fkSrcAuthType, jg.fkSrcAuthParamPassingMethod, jg.fkSrcResponseType, jg.fkSrcResponseFormat, jg.srcRespContentType, jg.hasPagination, jg.paginationURLKeyword, jg.paginationURLLocation, jg.paginationType, jg.paginationAdditionalParams, jq.sourceChgKeyLatestValues, jq.deleteOperationTableName, jq.XMLNodesTillDataNode, jg.APIPostRequestBody, jq.sourceTimestampFormat, jq.targetStoreKeyName, jq.target2StorageAccount, jq.target2Container, jq.target2FilePath, jq.target2FileName, jq.transformedColumnsInTarget FROM  audit.tblJobQueue jq, audit.tblJobQueueExtn jg WHERE jg.fkJobQueue=jq.pkTblJobQueue AND jq.jobGroup="+jobGroup+" AND jq.jobOrder = "+jobOrder+" AND jobNum="+jobNum+" AND jq.isActive='Y') as test"
currentRow = spark.read.jdbc(url=JDBC_url, table=selectQuery, properties=connectionProperty).na.fill("")
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
val formatType = currentRow.select("fkTargetFileFormat").take(1)(0)(0).toString()
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
// val commandType = currentRow.select("commandType").take(1)(0)(0).toString()
val filePath = currentRow.select("sourceFilePath").take(1)(0)(0).toString()
val fileName = currentRow.select("sourceFileName").take(1)(0)(0).toString()
val columnChangeSchema = currentRow.select("additionalColumnsInTarget").take(1)(0)(0).toString()
val sourceChgKeyLatestValues = currentRow.select("sourceChgKeyLatestValues").take(1)(0)(0).toString()
val incQueryPrama = currentRow.select("tokenRespContentType").take(1)(0)(0).toString()
val transformedColumnsInTarget = currentRow.select("transformedColumnsInTarget").take(1)(0)(0).toString() // "WellId:Int;Sizes:Double" 
val sourceTimestampFormat = currentRow.select("sourceTimestampFormat").take(1)(0)(0).toString()
val secondaryBlobStorageKey = currentRow.select("targetStoreKeyName").take(1)(0)(0).toString()
val target2StorageAccount = currentRow.select("target2StorageAccount").take(1)(0)(0).toString()
val target2Container = currentRow.select("target2Container").take(1)(0)(0).toString()
val target2FilePath = currentRow.select("target2FilePath").take(1)(0)(0).toString()
val target2FileName = currentRow.select("target2FileName").take(1)(0)(0).toString()
val deleteTable = currentRow.select("deleteOperationTableName").take(1)(0)(0).toString()
%md # Initialize Global Variables
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
      println("ONE Parameter")
    if(apiTokens.contains("-KV"))
      credentials += (apiAuthId-> dbutils.secrets.get(keyVaultName.toString(), apiTokens))
    else
      credentials += (apiAuthId -> apiTokens)
  }

  for ((k,v) <- credentials){println(k+" = "+ v)}

  if("OAuth2.0".equals(apiAuthType)){
    println(tokenURL)
    var t = ""
    if(tokenClientId.contains("-KV") && tokenClientCredential.contains("-KV"))
      t = getAPIAccessToken(tokenURL, dbutils.secrets.get(keyVaultName.toString(),tokenClientId), dbutils.secrets.get(keyVaultName.toString(),tokenClientCredential))
    else
      t = getAPIAccessToken(tokenURL, tokenClientId, tokenClientCredential)
    val bearer_token = "Bearer "+ t
    credentials += ("Authorization" -> bearer_token)
}
} catch{
  case ex: Exception => errMsg = "Azure Data FActory "+dataFactoryName + " pipeline "+ pipelineName+".\n\nLocation: The child notebook is failing for API Ingestion of "+TARGET_DATABASE+"."+TARGET_TABLE+ "table while processing API Credentials. \n\n Please click link for further details of error: \n" +ex;
        STATUS = "F"
        operation = "JOB_FAILED"
} 
%md # Get Data
%md ##### JSON Response Body
try {
  if("Body".equals(apiResponseType) && "JSON".equals(apiResponseFormat)) {
    processingDetails = "JSON body of "+ TARGET_TABLE+" of "+fkSourceApplication+"."
    if("INC".equals(loadType)){
      if(!incQueryPrama.contains("N/A") && spark.catalog.tableExists(TARGET_DATABASE+"."+TARGET_TABLE)){ // header contains key name 'fromdate'
        val timeQuery="(select fkJobQueue, min(startTime) as maxTime from audit.tblRunLog where fkJobQueue = "+pkTblJobQueue+" group by(fkJobQueue)) as tab"
        val max_time = spark.read.jdbc(url=JDBC_url, table=timeQuery, properties=connectionProperty).select("maxTime").take(1)(0)(0)
        var maxtime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:sss'Z'").format(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(max_time.toString()))
        sourceURL = sourceURL + "&"+incQueryPrama+"'"+maxtime+"'"
      }
    }

    println(
      f"""|Source URL       ➜ $sourceURL
          |Target db.table  ➜ $TARGET_DATABASE.$TARGET_TABLE
          |Credentials      ➜ ${credentials.toMap}
          |${printSeparator("═", separatorLength)}
          |>>> Testing initial connection with API...""".stripMargin
      )

    val resp = getResp(
      sourceURL, apiReqMethod, apiAuthType, apiAuthWay, additionalParams, credentials.toMap, apiReqContentType
      )

    if(!(resp.isSuccess)) {
      errorMsg = s"API response: ${resp.statusLine} ❌"
      throw new Exception(errorMsg)
    }

    if(resp.isSuccess){
      println(s"\t• API response: ${resp.statusLine} ✅")
      val json = resp.body
      df = spark.read.json(Seq(json).toDS).select(selectCols)
      
    // Handle different tables (Unique to Rushmore Reviews) - do not use flattenDataframe() for time_depth & casing tables otherwise cross-join occurs
    TARGET_TABLE match {  
      case "well_cpr" | "well_dpr" =>
        val df_ex = removingColumns(df, excludeColumns)
        tableDF = flattenDataframe(df_ex, TARGET_TABLE)

      case "time_depth" =>
        tableDF = getTimeDepth(df, TARGET_TABLE)

      case "casing" =>
        tableDF = getCasingData(df)
    }
    
    println(">>> Table schema inferred")

    if("Y".equals(hasPagination)) {
      println(">>> Beginning pagination...")
      val dfs = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], tableDF.schema)
      tableDF = getAllPagesData(
        sourceURL, apiReqMethod, apiAuthType, apiAuthWay, additionalParams, credentials, apiReqContentType, selectCols:String, paginationType, paginationURLKeyword, paginationURLLocation, excludeColumns, dfs, TARGET_TABLE
        )
      println(s"${printSeparator("═", separatorLength)}")
      println(">>> End of pagination")
    }
          
    for(col <- tableDF.columns){
        tableDF = tableDF.withColumnRenamed(col,col.replaceAll("\\s", "_"))
        }
    println(">>> Replaced spaces with underscores in column names")
    
    tableDF = tableDF.withColumn("row_create_date",current_timestamp().as("row_create_date"))
    println(">>> Added row creation timestamp")
          
    if(transformedColumnsInTarget!=null && !transformedColumnsInTarget.trim.isEmpty) {
      tableDF = changeColumnSchema(tableDF, transformedColumnsInTarget)
      println(">>> Updated column schema")
    }
      
    if ("casing".equals(TARGET_TABLE)) {
      tableDF = tableDF.filter(col("Size").isNotNull && trim(col("Size")) =!= "" && trim(col("Size")) =!= "null")
      println(">>> Filtered invalid 'Size' values for 'casing' table")
    }
      
    recInSrc = tableDF.count()
    println(s">>> Final table count ➜ $recInSrc")

    saveADLS(tableDF, loadType, excludeColumns, TARGET_DATABASE, TARGET_TABLE, sourcePKCols, deleteTable)
    
    recIng = recInSrc
    operation = "JOB_SUCCESSFUL"
    STATUS = "S"
    } else throw new Exception(resp.statusLine)
  }
} catch {
  case ex: Exception =>
    val errorMsg = s"""
      |ADF: $dataFactoryName
      |PIPELINE: $pipelineName
      |LOCATION: Child notebook in Databricks API Ingestion is failing for $TARGET_DATABASE.$TARGET_TABLE
      |LINK: $url_return
      |EXCEPTION: ${ex}
    """.stripMargin.trim
    STATUS = "F"
    operation = "JOB_FAILED"
    recFail = recInSrc
    throw new Exception(errorMsg, ex)
    }
if(!("well_dpr".equals(TARGET_TABLE))){
  spark.sql("OPTIMIZE "+TARGET_DATABASE+"."+TARGET_TABLE) 
}
%md # Exit Notebook
exitNotebook(operation, url_return, pkTblJobQueue, jobGroup, jobNum, jobOrder, jobStepNum, STATUS, processingDetails, errMsg, recInSrc.toInt, recIng.toInt, recInSrc.toInt, recFail.toInt, JDBC_url)
