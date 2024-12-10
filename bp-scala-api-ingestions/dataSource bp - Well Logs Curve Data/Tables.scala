%md # Run ModuleFunctions
%run ./ModuleFunctions
%md # Imports
// Java Time for Date and Time Handling
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

// Scala Collections
import scala.collection.mutable.ArrayBuffer

// Java Text and Formatting
import java.text._

// Java SQL Classes for Database Connections
import java.util.Properties
import java.sql.{Connection, PreparedStatement, DriverManager}

// Custom Utilities from bp.dwx Package
import bp.dwx.tableUtils._

// Apache Spark SQL Types and Row
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.Row

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
var STATUS = "R" 
var processingDetails = ""
var query = ""
var fail = ""
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

//JDBC Connection
var connectionProperty:Properties = null
var connection : Connection = null 
var preparedStatement : PreparedStatement  = null
%md # Insert in RunLog Table
try {
  auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
  connectionProperty = getJDBCConnectionProperty(auditDBAccessToken, "108000")
  connection = DriverManager.getConnection(JDBC_url,connectionProperty)
  operation = "JOB_STARTED"
  query = "EXEC [audit].[SP_LOG_RUN_DETAILS_NEW] @operation = N'"+ operation +"', @runID = N'"+url_return+"',@fkJobQueue = "+pkTblJobQueue+", @jobGroup = "+jobGroup+", @jobNum = "+jobNum+", @jobOrder = "+jobOrder+", @jobStepNum = "+jobStepNum+", @status = N'"+STATUS+"', @processingDetails = N'"+processingDetails+"', @errorMsg = N'"+errMsg+"', @recInSource = NULL, @recIngested = NULL, @recProcessed = NULL, @recFailed = NULL"
println(query)
//   sqlContext.sqlDBQuery( sqlDBquery(ENV_AuditTableServer, ENV_AuditTableDB, query, auditDBAccessToken)) 
  preparedStatement = connection.prepareStatement(query)
  preparedStatement.executeUpdate()
  } catch {
  case ex: Exception => errMsg = "ADF: " + dataFactoryName + " | PIPELINE: " + pipelineName + " | LOCATION: Child notebook in Databricks API Ingestion is failing for jobGroup " + jobGroup + " & jobOrder " + jobOrder + " when inserting to the runlog at the start | EXCEPTION: " + ex;
  STATUS = "F"
  }
%md # Parameters from Parent
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
var filterQuery = currentRow.select("filterQuery").take(1)(0)(0).toString()
val AccessTokenDuration = 25

if (loadType=="INC"){
  var y = " and a.created_date > '"+sourceChgKeyLatestValues+"'"
  filterQuery += y
}
%md # Initialize Global Variables
try {
  if (apiAuthId.contains(";") && apiTokens.contains(";")) 
  {
    val keys = apiAuthId.split(";")
    val vals = apiTokens.split(";")
    println("Both true")
    if (keys.length == vals.length) {
      for(i <- 0 until vals.length) {
        if(vals(i).contains("-KV"))
          credentials += (keys(i) -> dbutils.secrets.get(keyVaultName.toString(), vals(i)))
        else if(vals(i).contains("guid"))
          credentials += (keys(i) -> java.util.UUID.randomUUID().toString())
        else
          credentials += (keys(i) -> vals(i))
      }
    }
  } else {
      if (apiAuthId != "") {
        println("ONE Parameter")

        if (apiTokens.contains("-KV")) {
          credentials += (apiAuthId-> dbutils.secrets.get(keyVaultName.toString(), apiTokens))
        } else {
        credentials += (apiAuthId -> apiTokens)
        }
      }
    }

  for ((k,v) <- credentials){println(k+ " = " + v)}

  if (apiAuthType == "OAuth2.0"){
    var t = ""
    if (tokenClientId.contains("-KV") && tokenClientCredential.contains("-KV")) { 
      println("tokenURL - ", tokenURL)    
      t = getAPIAccessToken(tokenURL, dbutils.secrets.get(keyVaultName.toString(),tokenClientId),dbutils.secrets.get(keyVaultName.toString(),tokenClientCredential))
    } else {
      t = getAPIAccessToken(tokenURL, tokenClientId, tokenClientCredential)
      }
    val bearer_token = "Bearer "+ t
    credentials += ("Authorization" -> bearer_token)
  }
} catch {
  case ex: Exception =>
  errMsg = "ADF: " + dataFactoryName + " | PIPELINE: " + pipelineName + " | LOCATION: Child notebook in Databricks is failing for API Ingestion from " + fkSourceApplication + " for table " + TARGET_DATABASE + "." + TARGET_TABLE + " when processing API credentials | EXCEPTION: " + ex;
  STATUS = "F"
  operation = "JOB_FAILED"
} 
%md # Get Data
%md ##### JSON Response Body
var tableDF = spark.emptyDataFrame
var failuresDF = spark.emptyDataFrame
var df1 = spark.emptyDataFrame
val FAILURES_TABLE = "failures_" + TARGET_TABLE
var first_table = ""
var second_table = ""
var third_table = ""
var fourth_table = ""
var rerun_count = ""
var latestDateValue = ""
var query = ""
var failures_count:Long = 0
var count_before:Long = 0
var count_after:Long = 0


try {
  val tableLocation = get_table_location(spark, databaseName=TARGET_DATABASE, tableName=TARGET_TABLE)
  if ("Body".equals(apiResponseType) && "JSON".equals(apiResponseFormat)) {
    processingDetails = "JSON body of " + TARGET_TABLE + " of " + fkSourceApplication + "."
    var d_ids = spark.sql(filterQuery)
    val col1 = columnChangeSchema.split(",")(0) // well_log_set_curve_id
    val col2 = columnChangeSchema.split(",")(1) // curve_file_id
    val col3 = columnChangeSchema.split(",")(2) // created_date

    if (d_ids.count() != 0) {
      count_before = if (spark.catalog.tableExists("$TARGET_DATABASE.$TARGET_TABLE")) spark.sql(s"select * from ${TARGET_DATABASE}.${TARGET_TABLE}").count() else 0
      val schema = StructType(StructField(col1, StringType, false) :: StructField(col2, StringType, false) :: StructField(col3, StringType, false) :: Nil )
      val schemaFailures = StructType(StructField(col1, StringType, false) :: StructField(col2, StringType, false) :: Nil )
      failuresDF = spark.createDataFrame(sc.emptyRDD[Row], schemaFailures) // schemaFailures is: well_log_set_curve_id, curve_file_id, url
      var curveid = d_ids.select(col1).collect().map(_(0)).map(_.asInstanceOf[String]) // well_log_set_curve_id
      var fileid = d_ids.select(col2).collect().map(_(0)).map(_.asInstanceOf[String]) // curve_file_id
      var dateid = d_ids.select(col3).collect().map(_(0)).map(_.asInstanceOf[String]) // created_date
      var c = curveid(0) // first well_log_set_curve_id
      var f = fileid(0) // first curve_file_id
      var d = dateid(0) // first created_date
      var url = sourceURL + c + "/file/" + f

      println(s"First API call -> $url\nTarget db.table -> ${TARGET_DATABASE}.${TARGET_TABLE}\nCredentials  -> ${credentials.toMap}\n\n🔓 Auth method: $apiAuthWay\n🔑 Auth type: $apiAuthType\n📨 Request: $apiReqMethod\n   Content type: $apiReqContentType")
      var resp = getResp(url, apiReqMethod, apiAuthType, apiAuthWay, additionalParams, credentials.toMap, apiReqContentType)
      if(!resp.isSuccess){println("❌ Status: " + resp.statusLine)}

      if (resp.isSuccess) {
        println("✔️ Status: " + resp.statusLine + "\n")
        val json = resp.body
        df1 = flattenDataframe(spark.read.json(Seq(json).toDS).select(selectCols), selectCols).withColumn(col1, lit(c.toString)).withColumn(col2, lit(f.toString)).withColumn(col3, lit(d.toString)).select("*")
        tableDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], df1.schema)
        var counter = 1

        if (curveid.length == fileid.length) {
          var run = 1
          first_table = TARGET_TABLE.toString+"_"+run.toString
          var APIAccessTokenEndTime = getAPIAccessTokenEndTime(AccessTokenDuration)
          var t = ""
          if (tokenClientId.contains("-KV") && tokenClientCredential.contains("-KV")) {    
            t = getAPIAccessToken(tokenURL, dbutils.secrets.get(keyVaultName.toString(),tokenClientId), dbutils.secrets.get(keyVaultName.toString(),tokenClientCredential))
          } else {
            t = getAPIAccessToken(tokenURL, tokenClientId, tokenClientCredential)
            }

          getCurveData(curveid, fileid, dateid, col1, col2, col3, counter, tokenURL, keyVaultName, tokenClientId, tokenClientCredential, sourceURL, apiReqMethod, apiAuthType, apiAuthWay, additionalParams, apiReqContentType, selectCols, loadType, excludeColumns, TARGET_DATABASE, TARGET_TABLE, FAILURES_TABLE, df1, tableDF, failuresDF, run, t, APIAccessTokenEndTime, AccessTokenDuration)

          var fail_d_ids = spark.sql("select * from " + TARGET_DATABASE + "." + FAILURES_TABLE)
          if(fail_d_ids.count() > 0){
            run += 1
            println(s"\n\nRUN $run: ")
            second_table = TARGET_TABLE.toString+"_"+run.toString
            rerun_count = dbutils.notebook.run("./Tables_rerun", timeoutSeconds = 3600, arguments = Map("col1" -> col1.toString, "col2" -> col2.toString, "col3" -> col3.toString, "tokenURL" -> tokenURL.toString, "keyVaultName" -> keyVaultName.toString, "tokenClientId" -> tokenClientId.toString, "tokenClientCredential" -> tokenClientCredential.toString, "sourceURL" -> sourceURL.toString, "apiReqMethod" -> apiReqMethod.toString, "apiAuthType" -> apiAuthType.toString, "apiAuthWay" -> apiAuthWay.toString, "additionalParams" -> additionalParams.toString, "apiReqContentType" -> apiReqContentType.toString, "selectCols" -> selectCols.toString, "loadType" -> loadType.toString, "excludeColumns" -> excludeColumns.toString, "TARGET_DATABASE" -> TARGET_DATABASE.toString, "TARGET_TABLE" -> TARGET_TABLE.toString, "FAILURES_TABLE" -> FAILURES_TABLE.toString, "run" -> run.toString, "sourcePKCols" -> sourcePKCols.toString, "deleteTable" -> deleteTable.toString, "dataFactoryName" -> dataFactoryName, "pipelineName" -> pipelineName, "url_return" -> url_return, "AccessTokenDuration" -> AccessTokenDuration.toString))
            println("• " + TARGET_TABLE + " CUMULATIVE COUNT AFTER RUN " + run + ": " + rerun_count)
            }


          fail_d_ids = spark.sql("select * from " + TARGET_DATABASE + "." + FAILURES_TABLE)
          if (fail_d_ids.count() > 0) {
            run += 1
            println(s"\n\nRUN $run: ")
            third_table = TARGET_TABLE.toString+"_"+run.toString
            rerun_count = dbutils.notebook.run("./Tables_rerun", timeoutSeconds = 3600, arguments = Map("col1" -> col1.toString, "col2" -> col2.toString, "col3" -> col3.toString, "tokenURL" -> tokenURL.toString, "keyVaultName" -> keyVaultName.toString, "tokenClientId" -> tokenClientId.toString, "tokenClientCredential" -> tokenClientCredential.toString, "sourceURL" -> sourceURL.toString, "apiReqMethod" -> apiReqMethod.toString, "apiAuthType" -> apiAuthType.toString, "apiAuthWay" -> apiAuthWay.toString, "additionalParams" -> additionalParams.toString, "apiReqContentType" -> apiReqContentType.toString, "selectCols" -> selectCols.toString, "loadType" -> loadType.toString, "excludeColumns" -> excludeColumns.toString, "TARGET_DATABASE" -> TARGET_DATABASE.toString, "TARGET_TABLE" -> TARGET_TABLE.toString, "FAILURES_TABLE" -> FAILURES_TABLE.toString, "run" -> run.toString, "sourcePKCols" -> sourcePKCols.toString, "deleteTable" -> deleteTable.toString, "dataFactoryName" -> dataFactoryName, "pipelineName" -> pipelineName, "url_return" -> url_return, "AccessTokenDuration" -> AccessTokenDuration.toString))
            println("• " + TARGET_TABLE + " CUMULATIVE COUNT AFTER RUN " + run + ": " + rerun_count)
          }


          fail_d_ids = spark.sql("select * from " + TARGET_DATABASE + "." + FAILURES_TABLE)
          if (fail_d_ids.count() > 0) {
            run += 1
            println(s"\n\nRUN $run: ")
            fourth_table = TARGET_TABLE.toString+"_"+run.toString
            rerun_count = dbutils.notebook.run("./Tables_rerun", timeoutSeconds = 3600, arguments = Map("col1" -> col1.toString, "col2" -> col2.toString, "col3" -> col3.toString, "tokenURL" -> tokenURL.toString, "keyVaultName" -> keyVaultName.toString, "tokenClientId" -> tokenClientId.toString, "tokenClientCredential" -> tokenClientCredential.toString, "sourceURL" -> sourceURL.toString, "apiReqMethod" -> apiReqMethod.toString, "apiAuthType" -> apiAuthType.toString, "apiAuthWay" -> apiAuthWay.toString, "additionalParams" -> additionalParams.toString, "apiReqContentType" -> apiReqContentType.toString, "selectCols" -> selectCols.toString, "loadType" -> loadType.toString, "excludeColumns" -> excludeColumns.toString, "TARGET_DATABASE" -> TARGET_DATABASE.toString, "TARGET_TABLE" -> TARGET_TABLE.toString, "FAILURES_TABLE" -> FAILURES_TABLE.toString, "run" -> run.toString, "sourcePKCols" -> sourcePKCols.toString, "deleteTable" -> deleteTable.toString, "dataFactoryName" -> dataFactoryName, "pipelineName" -> pipelineName, "url_return" -> url_return, "AccessTokenDuration" -> AccessTokenDuration.toString))
            println("• " + TARGET_TABLE + " CUMULATIVE COUNT AFTER RUN " + run + ": " + rerun_count)
          }
        } // curveid.length == fileid.length       
      } // if success

      if("INC".equals(loadType)){
        val oldDataQuery = s"select * from ${TARGET_DATABASE}.${TARGET_TABLE}"
        val newDataQuery = s"select * from ${TARGET_DATABASE}.STG_${TARGET_TABLE}"
        val oldData = spark.sql(oldDataQuery)
        val newData = spark.sql(newDataQuery)
        val finalData = oldData.union(newData)

        tableUtils.saveAsTable(
                            df = finalData,  
                            fullTargetTableName = s"$TARGET_DATABASE.$TARGET_TABLE",
                            writeMode = "overwrite",  
                            debug = true,  
                            dryRun = false 
                            )

        if (spark.catalog.tableExists(s"$TARGET_DATABASE.STG_$TARGET_TABLE")) {
          tableUtils.runDDL(s"DROP TABLE ${TARGET_DATABASE}.STG_${TARGET_TABLE}")
        }
      }
    }

    // regenerate SQL P12 access token & update sourceChgKeyLatestValues
    val selectQuery = s"select max($col3) from ${TARGET_DATABASE}.${TARGET_TABLE}"
    latestDateValue = spark.sql(selectQuery).take(1)(0)(0).toString()
    println(s"• Latest date value at source:     ${latestDateValue}")
    query = s"update audit.tblJobQueue set sourceChgKeyLatestValues='$latestDateValue' where pkTblJobQueue=$pkJobQueue"
    println(s"• Update query for P12 jobQueue:   ${query}")
    auditDBSPId = null
    auditDBSPPwd = null
    auditDBAccessToken = null
    connectionProperty = null
    connection = null
    auditDBSPId = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKey)
    auditDBSPPwd = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKeyPwd)
    auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
    connectionProperty = getJDBCConnectionProperty(auditDBAccessToken, "108000")
    connection = DriverManager.getConnection(JDBC_url,connectionProperty)
    preparedStatement = connection.prepareStatement(query)
    preparedStatement.executeUpdate()

    // Failures that were not resolved even after 4 re-runs
    if (spark.catalog.tableExists(s"${TARGET_DATABASE}.${FAILURES_TABLE}")) {
      failures_count = spark.sql(s"SELECT * FROM ${TARGET_DATABASE}.${FAILURES_TABLE}").count()
      println(s"\n• Unresolved failures ➜ $failures_count")
      tableUtils.runDDL(s"DROP TABLE $TARGET_DATABASE.$FAILURES_TABLE")
    }

    if (d_ids.count() == 0) { println(s"No new data at source after timestamp: $sourceChgKeyLatestValues") }
    count_after = if (spark.catalog.tableExists(s"${TARGET_DATABASE}.${TARGET_TABLE}")) spark.sql(s"SELECT * FROM ${TARGET_DATABASE}.${TARGET_TABLE}").count() else 0
    println(s"\n• ${TARGET_DATABASE.toLowerCase()}.${TARGET_TABLE.toLowerCase()} count\n\t➜ before = $count_before\n\t➜ after  = $count_after")
    recInSrc = count_after
    operation = "JOB_SUCCESSFUL"
    STATUS = "S"
    println("\n🏁🏁🏁🏁🏁🏁🏁\n")
    
  }
} catch {
  case ex: Exception => 
    errMsg = "ADF: " + dataFactoryName + " | PIPELINE: " + pipelineName + " | LOCATION: Child notebook in Databricks API Ingestion is failing for " + TARGET_DATABASE + "." + TARGET_TABLE + " | LINK: " + url_return + " | EXCEPTION: " + ex;
    STATUS = "F"
    operation = "JOB_FAILED"
    recFail = recInSrc
    println()
    }
%md # Exit Notebook
exitNotebook(operation, url_return, pkTblJobQueue, jobGroup, jobNum, jobOrder, jobStepNum, STATUS, processingDetails, errMsg, recInSrc.toInt, recIng.toInt, recInSrc.toInt, recFail.toInt, JDBC_url)
