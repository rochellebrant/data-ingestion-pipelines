%run ./ModuleFunctions
%md # Declare Global Variables
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer
import java.text._
import java.util.Properties
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager

val start_time = java.sql.Timestamp.valueOf(LocalDateTime.now)
val workspace_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
val job_id = dbutils.notebook.getContext.tags("jobId").toString()
val url_return = s"https://northeurope.azuredatabricks.net/?o=$workspace_id#job/$job_id/run/1"
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true") // Resolve Full load issue of notebook detach while running
var recInSrc: Long = 0
var recFail: Long = 0
var recIng: Long = 0L
var operation = ""
var errorMsg = ""
var STATUS = "R" 
var processingDetails = ""
var query = ""
var fail = ""
var tableDF: org.apache.spark.sql.DataFrame = _

var auditDBSPId = ""
var auditDBSPPwd = ""
var auditDBAccessToken = ""
var currentRow:org.apache.spark.sql.DataFrame = _

// Capture data from parent notebook
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

// JDBC Connection
var connectionProperty:Properties = _
var connection : Connection = _ 
var preparedStatement : PreparedStatement  = _
%md # Insert start to Runlog Table
try {
  // Acquire access token and connection properties
  auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
  connectionProperty = getJDBCConnectionProperty(auditDBAccessToken, "36000")

  // Establish connection to the database
  connection = DriverManager.getConnection(JDBC_url, connectionProperty)

  // Set operation status and prepare the query
  operation = "JOB_STARTED"
  query = s"""
    EXEC [audit].[SP_LOG_RUN_DETAILS_NEW] 
    @operation = N'$operation', 
    @runID = N'$url_return',
    @fkJobQueue = $pkTblJobQueue, 
    @jobGroup = $jobGroup, 
    @jobNum = $jobNum, 
    @jobOrder = $jobOrder, 
    @jobStepNum = $jobStepNum, 
    @status = N'$STATUS', 
    @processingDetails = N'$processingDetails', 
    @errorMsg = N'$errorMsg', 
    @recInSource = NULL, 
    @recIngested = NULL, 
    @recProcessed = NULL, 
    @recFailed = NULL
  """

  println(query)

  // Execute the query
  preparedStatement = connection.prepareStatement(query)
  preparedStatement.executeUpdate()
  
} catch {
  case ex: Exception =>
  val errorMsg = s"""
  |ADF: $dataFactoryName
  |PIPELINE: $pipelineName
  |LOCATION: Child job
  |TASK: Inserting to the runlog table to indicate the start of the job
  |LINK: $url_return
  |EXCEPTION: ${ex}
  """.stripMargin.trim
  STATUS = "F"
  operation = "JOB_FAILED"
  throw new Exception(errorMsg, ex)
  }
%md # Parameters from Parent
val selectQuery = s"""(SELECT jq.pkTblJobQueue, jq.jobGroup, jq.jobNum, jq.jobOrder, jq.jobStepNum, jq.fkJobType, jq.fkJobStep, jq.fkLoadType, jq.fkSourceApplication, jq.sourceTblName, jq.sourcePKCols, jq.sourceChangeKeyCols, jq.excludeColumns, jq.targetDBName, jq.targetExtTblSchemaName, jq.targetTblName, jq.targetStorageAccount, jq.targetContainer, jq.extTbDataSource, jq.forceTblRecreate, jq.targetFilePath, jq.targetFileName, jq.fkTargetFileFormat, jq.successNotificationEmailIDs, jq.failureNotificationEmailIDs, jq.IsActive,  jq.keyVaultName, jq.inscopeColumnList, jq.sourceURL, jq.sourceFilePath , jq.sourceFileName, jq.additionalColumnsInTarget, jg.tokenURL, jq.filterQuery, jg.tokenCredentialKeyList, jg.tokenCredentialKVNameList, jg.fkTokenRequestMethod, jg.fkTokenAuthType, jg.fkTokenAuthParamPassingMethod, jg.tokenRespContentType, jg.srcAuthCredentialKeyList, jg.srcAuthCredentialKVNameList, jg.fkSrcRequestMethod, jg.fkSrcAuthType, jg.fkSrcAuthParamPassingMethod, jg.fkSrcResponseType, jg.fkSrcResponseFormat, jg.srcRespContentType, jg.hasPagination, jg.paginationURLKeyword, jg.paginationURLLocation, jg.paginationType, jg.paginationAdditionalParams, jq.sourceChgKeyLatestValues, jq.sourceTimestampFormat, jq.targetStoreKeyName, jq.target2StorageAccount, jq.target2Container, jq.target2FilePath, jq.target2FileName, jq.transformedColumnsInTarget
FROM
  audit.tblJobQueue jq
JOIN
  audit.tblJobQueueExtn jg ON jg.fkJobQueue = jq.pkTblJobQueue 
WHERE
  jq.jobGroup = ${jobGroup}
  AND jq.jobOrder = ${jobOrder}
  AND jobNum = ${jobNum}
  AND jq.isActive='Y'
  )
  AS test"""

currentRow = spark.read.jdbc(url = JDBC_url, table = selectQuery, properties = connectionProperty).na.fill("")
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
var hasPagination = currentRow.select("hasPagination").take(1)(0)(0).toString()
val paginationURLKeyword = currentRow.select("paginationURLKeyword").take(1)(0)(0).toString()
val paginationURLLocation = currentRow.select("paginationURLLocation").take(1)(0)(0).toString()
val paginationType = currentRow.select("paginationType").take(1)(0)(0).toString()
val additionalParams = currentRow.select("paginationAdditionalParams").take(1)(0)(0).toString()
val filePath = currentRow.select("sourceFilePath").take(1)(0)(0).toString()
val fileName = currentRow.select("sourceFileName").take(1)(0)(0).toString()
val columnChangeSchema = currentRow.select("additionalColumnsInTarget").take(1)(0)(0).toString()
val sourceChgKeyLatestValues = currentRow.select("sourceChgKeyLatestValues").take(1)(0)(0).toString()
val incQueryPrama = currentRow.select("tokenRespContentType").take(1)(0)(0).toString()
val sourceTimestampFormat = currentRow.select("sourceTimestampFormat").take(1)(0)(0).toString()
val secondaryBlobStorageKey = currentRow.select("targetStoreKeyName").take(1)(0)(0).toString()
val target2StorageAccount = currentRow.select("target2StorageAccount").take(1)(0)(0).toString()
val target2Container = currentRow.select("target2Container").take(1)(0)(0).toString()
val target2FilePath = currentRow.select("target2FilePath").take(1)(0)(0).toString()
val target2FileName = currentRow.select("target2FileName").take(1)(0)(0).toString()
val deleteTable = currentRow.select("target2FileName").take(1)(0)(0).toString()
%md # Initialize Global Variables
def generateToken() : Map[String, String] = {
  val credentials: Map[String, String] = Map.empty
  try {
    if (apiAuthId.contains(";") && apiTokens.contains(";")) {
      val keys = apiAuthId.split(";")
      val vals = apiTokens.split(";")
      println("Both true")
      if (keys.length == vals.length) {
      for (i <- 0 until vals.length) {
        if (vals(i).contains("-KV"))
          credentials += (keys(i) -> dbutils.secrets.get(keyVaultName.toString(), vals(i)))
        else if (vals(i).contains("guid"))
          credentials += (keys(i) -> java.util.UUID.randomUUID().toString())
        else
          credentials += (keys(i) -> vals(i))
      }
    }
    } else {
        if (apiAuthId != "") {
          println("ONE Parameter")
          if (apiTokens.contains("-KV"))
            credentials += (apiAuthId-> dbutils.secrets.get(keyVaultName.toString(), apiTokens))
          else
          credentials += (apiAuthId -> apiTokens)
        }
      }

  for ((k,v) <- credentials){println(k+ " = " + v)}

  if (apiAuthType == "OAuth2.0") {
    var t = ""
    if (tokenClientId.contains("-KV") && tokenClientCredential.contains("-KV"))
      println("tokenURL - ", tokenURL)    
      t = getAPIAccessToken(tokenURL, dbutils.secrets.get(keyVaultName.toString(),tokenClientId),dbutils.secrets.get(keyVaultName.toString(),tokenClientCredential))
    else
      t = getAPIAccessToken(tokenURL, tokenClientId, tokenClientCredential)
    val bearer_token = "Bearer "+ t
    credentials += ("Authorization" -> bearer_token)
  }
  credentials += ("tokenGenerateTime" -> LocalDateTime.now.toEpochSecond(ZoneOffset.UTC).toString)
  } catch {
      case ex: Exception =>
      val currentTS = LocalDateTime.now().toString
      val errorMsg = s"⚠️ Exception occurred at $currentTS in generateToken() function: $ex"
      throw new Exception(errorMsg, ex)
    }
    credentials
}


val credentials: Map[String, String] = generateToken()
%md # Get Data
%md ##### JSON Response Body
try {
  if ("Body".equals(apiResponseType) && "JSON".equals(apiResponseFormat)) {
    processingDetails = s"JSON body of $TARGET_TABLE of $fkSourceApplication."

    if ("INC".equals(loadType)) {
      print("1st condition")

      // Ensure the increment query parameter is valid and the target table exists
      if (!incQueryPrama.contains("N/A") && spark.catalog.tableExists(s"$TARGET_DATABASE.$TARGET_TABLE")) {
        print("2nd condition")
        val timeQuery = s"""
          (SELECT fkJobQueue, MIN(startTime) AS maxTime 
          FROM audit.tblRunLog 
          WHERE fkJobQueue = $pkTblJobQueue 
          GROUP BY fkJobQueue) AS tab
        """
        print("timeQuery: ", timeQuery)

        val max_time = spark.read.jdbc(url=JDBC_url, table=timeQuery, properties=connectionProperty).select("maxTime").take(1)(0)(0)
        print("max_time: ", max_time)

        var maxtime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:sss'Z'").format(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(max_time.toString()))
        print("maxtime :", maxtime)

        sourceURL += s"&$incQueryPrama'$maxtime'"
      }
    } 

    println(s"Source URL -> $sourceURL\nTarget db.table -> $TARGET_DATABASE.$TARGET_TABLE\nCredentials -> ${credentials.toMap}\n")

    // Fetch the response from the source URL
    val resp = getResp(sourceURL, apiReqMethod, apiAuthType, apiAuthWay, additionalParams, credentials.toMap, apiReqContentType)

    if (!resp.isSuccess) {
      println("❌ Status: " + resp.statusLine)
      }

    if (resp.isSuccess) {
      println("✅ Status: " + resp.statusLine)
      val json = resp.body
      tableDF = flattenDataframe(spark.read.json(Seq(json).toDS).select(selectCols))

      if ("Y".equals(hasPagination)) {
        val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], flattenDataframe(tableDF).schema)
        tableDF = getAllPagesData(sourceURL, apiReqMethod, apiAuthType, apiAuthWay, additionalParams, credentials, apiReqContentType, selectCols:String, paginationType, paginationURLKeyword, paginationURLLocation, excludeColumns, emptyDF, generateToken)      
      }

    tableDF = removePrefix(tableDF, sourceChangeKeyCols)
    tableDF.columns.foreach(col => tableDF = tableDF.withColumnRenamed(col, col.replaceAll("\\s", "_")))

    if(columnChangeSchema!=null && !columnChangeSchema.trim.isEmpty) {
      tableDF = changeColumnSchema(tableDF, columnChangeSchema)
    }
    
    recInSrc = tableDF.count()
    println("Table count: " + recInSrc)
    saveADLS(tableDF, loadType, excludeColumns, TARGET_DATABASE, TARGET_TABLE, sourcePKCols, deleteTable)

    if("INC".equals(loadType)){
      println("🕒 Updating timestamp for INC Load")
      if(apiAuthId.contains("fromdate") && spark.catalog.tableExists(TARGET_DATABASE+"."+TARGET_TABLE)){ // header contains key name 'fromdate'
        val ids = apiAuthId.split(";")
        val vals = apiTokens.split(";")
        for(i <- 0 until ids.length){
          if("fromdate".equals(ids(i))) vals(i) = DateTimeFormatter.ofPattern(sourceTimestampFormat).format(LocalDateTime.now) 
        }
        val tokens = vals.mkString(";")
        val updateQuery = "update audit.tblJobQueueExtn set srcAuthCredentialKVNameList = "+tokens+" where fkJobQueue = "+pkJobQueue
        val noOfRowsUpdated = connection.prepareStatement(updateQuery).executeUpdate()
        println("Number Of Rows Updated: " + noOfRowsUpdated)
      }
    }

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
  |LOCATION: Child job
  |TASK: Processing JSON response
  |TARGET TABLE: $TARGET_DATABASE.$TARGET_TABLE
  |LINK: $url_return
  |EXCEPTION: ${ex}
  """.stripMargin.trim
  STATUS = "F"
  operation = "JOB_FAILED"
  recFail = recInSrc
  throw new Exception(errorMsg, ex)
  }
%md ##### Text Response Body
try {
  if("Body".equals(apiResponseType) && "CSV".equals(apiResponseFormat)){
    val resp = getResp(sourceURL, apiReqMethod, apiAuthType, apiAuthWay, additionalParams, credentials.toMap, apiReqContentType)
    println(resp)
    if(resp.isSuccess){
      val text = resp.body
      val f = TARGET_DATABASE+"_"+TARGET_TABLE+".csv"
      dbutils.fs.put("abfss://udl-container@zneudl1p48defstor01.dfs.core.windows.net/Raw/GOO_CATC/historydata/"+f, text, true)
      operation = "JOB_SUCCESSFUL"
      STATUS = "S"
    } else throw new Exception(resp.statusLine)
  }
} catch {
  case ex: Exception =>
  val errorMsg = s"""
  |ADF: $dataFactoryName
  |PIPELINE: $pipelineName
  |LOCATION: Child job
  |TASK: Processing TEXT response into a CSV file
  |TARGET TABLE: $TARGET_DATABASE.$TARGET_TABLE
  |LINK: $url_return
  |EXCEPTION: ${ex}
  """.stripMargin.trim
  STATUS = "F"
  operation = "JOB_FAILED"
  recFail = recInSrc
  throw new Exception(errorMsg, ex)
  }
%md ##### XLSX  Response Body
try{  
if("Attachment".equals(apiResponseType) ){ 
  
  var time:String = ""
  println(apiResponseType)
  println(filePath)
  val list = dbutils.fs.ls(filePath).toString()
  println(list.contains(fileName))
  var url = sourceURL   
  println(TARGET_TABLE)
  if(("APPEND".equals(loadType) || "INC".equals(loadType)) && list.contains(fileName)){
    if("XLSX".equals(apiResponseFormat)){
      processingDetails = "XLSX file from API body for "+ TARGET_TABLE+" of "+fkSourceApplication+"."
      println(apiResponseType)
       val v = spark.read.format("com.crealytics.spark.excel").option("header","false").option("treatEmptyValuesAsNulls","false").option("inferSchema","true").option("addColorColumns", "false").option("sheetName","Summary").load(filePath+fileName).filter($"_c0"==="Completed").take(1)(0) 
      println(v)
      if(v.size==3) time = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(v(1).toString()))
      else if (v.size==4) time = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss+HH:mm").parse(v(3).toString()))
      url = url + "/delta/"+time      
    }
  }
  println(url)
  if("QueryParam".equals(apiAuthWay)){
    for ((k,v) <- credentials.take(1)){url = url +"?"+k+"="+v}
    for ((k,v) <- credentials.tail){url = url +"&"+k+"="+v}  
  }
  println("Before buildCURL")
  val result = buildCURL(Seq("curl", "-H", "Cache-Control: no-cache", url, "-k"), apiReqMethod, apiAuthType, apiAuthWay, credentials, fileName, filePath, apiResponseType, null) //Build curl command
    if("true".equals(result)){ 
      println(result)
      operation = "JOB_SUCCESSFUL"
      STATUS = "S"
    }
    else throw new Exception("CURL command Fails to download Excel attachment!")
}
} catch {
  case ex: Exception =>
  val errorMsg = s"""
  |ADF: $dataFactoryName
  |PIPELINE: $pipelineName
  |LOCATION: Child job
  |TASK: Processing excel file download
  |TARGET TABLE: $TARGET_DATABASE.$TARGET_TABLE
  |LINK: $url_return
  |EXCEPTION: ${ex}
  """.stripMargin.trim
  STATUS = "F"
  operation = "JOB_FAILED"
  recFail = recInSrc
  throw new Exception(errorMsg, ex)
  }
%md # Exit Notebook
exitNotebook(operation, url_return, pkTblJobQueue, jobGroup, jobNum, jobOrder, jobStepNum, STATUS, processingDetails, errorMsg, recInSrc.toInt, recIng.toInt, recInSrc.toInt, recFail.toInt, JDBC_url, connectionProperty)
