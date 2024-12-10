%run ./ModuleFunctions
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer
import java.text._

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
val start_time = java.sql.Timestamp.valueOf(LocalDateTime.now)
val workspace_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
val job_id = dbutils.notebook.getContext.tags("jobId").toString()
val url_return = "https://northeurope.azuredatabricks.net/?o="+workspace_id+"#job/"+job_id+"/run/1"
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true") //Resolve Full load issue of notebook detach while running
var credentials = scala.collection.mutable.Map[String, String]()
var recInSrc:Long = 0
var recFail:Long = 0
var recIng: Long = 0L
var recProcessed: Long = 0
var operation = ""
var errorMsg = ""
var STATUS = "R" 
var processingDetails = ""
var query = ""
var fail = ""
var mergeCondition = ""
var count = 0
var tableDF: org.apache.spark.sql.DataFrame = null
var finaldf: org.apache.spark.sql.DataFrame = null
var tempdf: org.apache.spark.sql.DataFrame = null

var auditDBSPId = ""
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
var connectionProperty: Properties = null
var connection: Connection = null 
var preparedStatement: PreparedStatement = null
try {    
  auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
  connectionProperty = getJDBCConnectionProperty(auditDBAccessToken, "36000")
  connection = DriverManager.getConnection(JDBC_url,connectionProperty)
  operation = "JOB_STARTED"
  query = "EXEC [audit].[SP_LOG_RUN_DETAILS_NEW] @operation = N'"+ operation +"', @runID = N'"+url_return+"',@fkJobQueue = "+pkTblJobQueue+", @jobGroup = "+jobGroup+", @jobNum = "+jobNum+", @jobOrder = "+jobOrder+", @jobStepNum = "+jobStepNum+", @status = N'"+STATUS+"', @processingDetails = N'"+processingDetails+"', @errorMsg = N'"+errorMsg+"', @recInSource = NULL, @recIngested = NULL, @recProcessed = NULL, @recFailed = NULL"
  println(query)
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
auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
connectionProperty = getJDBCConnectionProperty(auditDBAccessToken, "36000")
connection = DriverManager.getConnection(JDBC_url, connectionProperty)

val selectQuery="(select jq.pkTblJobQueue, jq.jobGroup, jq.jobNum, jq.jobOrder, jq.jobStepNum, jq.fkJobType, jq.fkJobStep, jq.fkLoadType, jq.fkSourceApplication, jq.sourceTblName, jq.sourcePKCols, jq.sourceChangeKeyCols, jq.excludeColumns, jq.targetDBName, jq.targetExtTblSchemaName, jq.targetTblName, jq.targetStorageAccount, jq.targetContainer, jq.extTbDataSource, jq.forceTblRecreate, jq.targetFilePath, jq.targetFileName, jq.fkTargetFileFormat, jq.successNotificationEmailIDs, jq.failureNotificationEmailIDs, jq.IsActive,  jq.keyVaultName, jq.inscopeColumnList, jq.sourceURL, jq.sourceFilePath , jq.sourceFileName, jq.additionalColumnsInTarget, jg.tokenURL, jg.tokenCredentialKeyList, jg.tokenCredentialKVNameList, jg.fkTokenRequestMethod, jg.fkTokenAuthType, jg.fkTokenAuthParamPassingMethod, jg.tokenRespContentType, jg.srcAuthCredentialKeyList, jg.srcAuthCredentialKVNameList, jg.fkSrcRequestMethod, jg.fkSrcAuthType, jg.fkSrcAuthParamPassingMethod, jg.fkSrcResponseType, jg.fkSrcResponseFormat, jg.srcRespContentType, jg.hasPagination, jg.paginationURLKeyword, jg.paginationURLLocation, jg.paginationType, jg.paginationAdditionalParams, jq.sourceChgKeyLatestValues,jq.transformedColumnsInTarget,jq.sourceTimestampFormat FROM  audit.tblJobQueue jq, audit.tblJobQueueExtn jg WHERE jg.fkJobQueue=jq.pkTblJobQueue AND jq.jobGroup="+jobGroup+" AND jq.jobOrder = "+jobOrder+" AND jobNum="+jobNum+" AND jq.isActive='Y') as tab" 

currentRow = spark.read.jdbc(url=JDBC_url, table=selectQuery, properties=connectionProperty).na.fill("")
display(currentRow)

val successNotificationEmailIDs = currentRow.select("successNotificationEmailIDs").take(1)(0)(0).toString()
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
val sourceURL = currentRow.select("sourceURL").take(1)(0)(0).toString()
val apiAuthId = currentRow.select("srcAuthCredentialKeyList").take(1)(0)(0).toString()
val apiTokens = currentRow.select("srcAuthCredentialKVNameList").take(1)(0)(0).toString()
val apiReqMethod = currentRow.select("fkSrcRequestMethod").take(1)(0)(0).toString()
val apiAuthType = currentRow.select("fkSrcAuthType").take(1)(0)(0).toString()
val apiAuthWay = currentRow.select("fkSrcAuthParamPassingMethod").take(1)(0)(0).toString()
val apiResponseType = currentRow.select("fkSrcResponseType").take(1)(0)(0).toString()
val apiResponseFormat = currentRow.select("fkSrcResponseFormat").take(1)(0)(0).toString()
val apiReqContentType = currentRow.select("srcRespContentType").take(1)(0)(0).toString()
val hasPagination = currentRow.select("hasPagination").take(1)(0)(0).toString()
val pageRuleName = currentRow.select("paginationURLKeyword").take(1)(0)(0).toString()
val pageRuleValue = currentRow.select("paginationURLLocation").take(1)(0)(0).toString()
val paginationType = currentRow.select("paginationType").take(1)(0)(0).toString()
val additionalParams = currentRow.select("paginationAdditionalParams").take(1)(0)(0).toString()
// val commandType = currentRow.select("commandType").take(1)(0)(0).toString()
val filePath = currentRow.select("sourceFilePath").take(1)(0)(0).toString()
val fileName = currentRow.select("sourceFileName").take(1)(0)(0).toString()
val columnChangeSchema = currentRow.select("transformedColumnsInTarget").take(1)(0)(0).toString()
val sourceChgKeyLatestValues = currentRow.select("sourceChgKeyLatestValues").take(1)(0)(0).toString()
val sourceTimestampFormat = currentRow.select("sourceTimestampFormat").take(1)(0)(0).toString()
try {
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
  } else {
      println("ONE Parameter")
    if(apiTokens.contains("-KV"))
      credentials += (apiAuthId-> dbutils.secrets.get(keyVaultName.toString(), apiTokens))
    else
      credentials += (apiAuthId -> apiTokens)
  }

  for ((k,v) <- credentials){println(k+" = "+ v)}

  if("OAuth2.0".equals(apiAuthType)) {
    println(tokenURL)
    var t = ""
    if(tokenClientId.contains("-KV") && tokenClientCredential.contains("-KV"))
      t = getAPIAccessToken(tokenURL, dbutils.secrets.get(keyVaultName.toString(),tokenClientId), dbutils.secrets.get(keyVaultName.toString(),tokenClientCredential))
    else
      t = getAPIAccessToken(tokenURL, tokenClientId, tokenClientCredential)
    val bearer_token = "Bearer "+ t
    credentials += ("Authorization" -> bearer_token)
  }
} catch {
  case ex: Exception =>
  val errorMsg = s"""
  |ADF: $dataFactoryName
  |PIPELINE: $pipelineName
  |LOCATION: Child job
  |TASK: Processing API credentials
  |TARGET TABLE: $TARGET_DATABASE.$TARGET_TABLE
  |LINK: $url_return
  |EXCEPTION: ${ex}
  """.stripMargin.trim
  STATUS = "F"
  operation = "JOB_FAILED"
  throw new Exception(errorMsg, ex)
  }
try {
  if("Body".equals(apiResponseType) && "JSON".equals(apiResponseFormat)){
    processingDetails = "JSON body of "+ TARGET_TABLE+" of "+fkSourceApplication+"."
    val resp = getResp(sourceURL, apiReqMethod, apiAuthType, apiAuthWay, additionalParams, credentials.toMap, apiReqContentType)
    println(resp.statusLine)
    if(resp.isSuccess) {
      val json = resp.body
      tableDF =flattenDataframe(spark.read.json(Seq(json).toDS).select(selectCols))
      if("Y".equals(hasPagination)){
        val dfs = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], flattenDataframe(tableDF).schema)  
        tableDF = getAllPagesData(sourceURL, apiReqMethod, apiAuthType, apiAuthWay, additionalParams, credentials, apiReqContentType, selectCols:String, paginationType, pageRuleName, pageRuleValue, excludeColumns, dfs)      
      }
      recInSrc = tableDF.count()

    for(col <- tableDF.columns){ tableDF = tableDF.withColumnRenamed(col,col.replaceAll("\\s", "_"))  }
      tableDF.printSchema()

      //Change Type of Column in Dataframe [Eg. ALl are StringType]
      if(columnChangeSchema!=null && !columnChangeSchema.trim.isEmpty)
        tableDF = changeColumnSchema(tableDF, columnChangeSchema,sourceTimestampFormat)

      //Save to ADLS
      saveADLS(tableDF, loadType, excludeColumns, TARGET_DATABASE, TARGET_TABLE, sourcePKCols)


      //SQL DW External table
  //   auditDBAccessToken = getDBAccessToken(dbutils.secrets.get(keyVaultName, "spname"), dbutils.secrets.get(keyVaultName, "sppassword"))
  //   createExtTbl(auditDBAccessToken, extTbDataSource, TARGET_DATABASE, TARGET_TABLE, targetExtTblSchemaName, forceTblRecreate)
  //     recInSrc = tableDF.rdd.countApprox(6000)  // Need to resolve
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
try {
  if("Body".equals(apiResponseType) && "CSV".equals(apiResponseFormat)) {
    processingDetails = s"CSV file from TEXT body of API for table ${TARGET_DATABASE}.${TARGET_TABLE} from source ${fkSourceApplication}."
    val text = getAllPagesTEXTData(sourceURL, apiReqMethod, apiAuthType, apiAuthWay, additionalParams, credentials, apiReqContentType, selectCols:String, paginationType, pageRuleName, pageRuleValue, excludeColumns)   
    
    if(!text.isEmpty()) {
      val f = "abfss://"+targetContainer+"@"+targetStorageAccount+targetFilePath+targetFileName+".csv"
      dbutils.fs.put(f, text, true)
      operation = "JOB_SUCCESSFUL"
      STATUS = "S"
    } else throw new Exception("Repsonse is empty!")
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
try{  
  if("Attachment".equals(apiResponseType) ) {
    
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
import java.time.LocalDateTime
import org.apache.spark.sql.functions.{to_date, to_timestamp}
import org.apache.spark.sql.types.{StructType, StructField, StringType,TimestampType, DoubleType, IntegerType, DateType}

try {
    if("Body".equals(apiResponseType) && "XML".equals(apiResponseFormat)) {
      val currentTime =  LocalDateTime.now()
      var cSeconds: Long = System.currentTimeMillis/1000 + 62135596800L
      var currentSeconds = cSeconds + "0000000"
      var data = ""
      var name =  ""
      var cols = inscopeColumnList.split(",")
      var fields:Array[StructField] = Array[StructField]();

      for(column<-cols){fields = fields :+ StructField(column.trim(), StringType, true)}

      var schema = new StructType(fields)
      var df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
      println("Table: " + TARGET_TABLE)
      
      if(TARGET_TABLE == "tag_alarms") {
        val parameters = "All D&amp;R Asphalt Alarms (Audible 0.1),All D&amp;R Alky Alarms (Audible 0.1),All D&amp;R 12PS Alarms (Audible 0.1),All D&amp;R UIU Alarms (Audible 0.1),All D&amp;R VRU 300 (Audible 0.1),All D&amp;R Utilities Alarms (Audible 0.1),All D&amp;R Lakefront (Audible 0.1),All D&amp;R DDU Alarms (Audible 0.1),All D&amp;R SRC (Audible 0.1),All D&amp;R 11PS Alarms (Audible 0.1),All D&amp;R FCU_VRU Alarms (Audible 0.1),All D&amp;R OMD Alarms (Audible 0.1),All D&amp;R GOHT Alarms (Audible 0.1),All D&amp;R ACC Alarms (Audible 0.1),All D&amp;R Coker Alarms (Audible 0.1)"
        val pids = parameters.split(",")

        pids.foreach(e => {
          data = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
<soap:Body>
<GetDRQueryResultDataTableXML xmlns="http://www.pas.com/webservices/">
      <queryName>"""+e+"""</queryName>
 </GetDRQueryResultDataTableXML>
</soap:Body>
</soap:Envelope>""" 
        
         val result = buildCURL(Seq("curl","--http1.1","-k", sourceURL), apiReqMethod, apiAuthType, apiAuthWay, credentials, fileName, filePath, apiResponseType, data) 
         if(!result.isEmpty){
          tempdf = parseXMLtoDF(result, sourcePKCols, cols, schema)
           df = df.union(tempdf)
         } })
        tableDF = df
      } else if(TARGET_TABLE == "shelving_history") {
        var dw_guids = spark.sql("select ID from pss_whi.dataowner_guids")
        var pids = dw_guids.select("ID").collect().map(_(0))
         pids.foreach(e => {
         data = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
<soap:Body>
<GetShelvingHistory xmlns="http://www.pas.com/webservices/">
      <ParentID>"""+e+"""</ParentID> 
      <Recurse>1</Recurse> 
      <StartTimeUTC>"""+sourceChgKeyLatestValues+"""</StartTimeUTC> 
      <EndTimeUTC>"""+currentSeconds+"""</EndTimeUTC>      
    </GetShelvingHistory>
</soap:Body>
</soap:Envelope>""" 
           
         val result = buildCURL(Seq("curl","--http1.1","-k", sourceURL), apiReqMethod, apiAuthType, apiAuthWay, credentials, fileName, filePath, apiResponseType, data) 
         if(!result.isEmpty){
          tempdf = parseXMLtoDF(result, sourcePKCols, cols, schema)
           df = df.union(tempdf)
         } })
        tableDF = df
      } else if(TARGET_TABLE == "kpigridnameresults") {
        var kpinames = "11PS Performance Metrics,12PS Performance Metrics,4UF Performance Metrics,Alky PCU Performance Metrics,Asphalt Performance Metrics,BOU ARU Performance Metrics,CFU Performance Metrics,Coker2 Performance Metrics,DDU HU Performance Metrics,FCU 500 Performance Metrics,FCU 600 Performance Metrics,GOHT NHT Performance Metrics,Lakefront Performance Metrics,OMD Performance Metrics,SRC Performance Metrics,UIU Performance Metrics,Utilities Performance Metrics,VRU 100 200 Performance Metrics,VRU 300 Performance Metrics"
        var names = kpinames.split(",")
        
        names.foreach(e =>{
          name = e
         data = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
<soap:Body>
  <GetKPIGridNameResultsForNow xmlns="http://www.pas.com/webservices/">
      <KPIGridName>"""+e+"""</KPIGridName>
    </GetKPIGridNameResultsForNow>
</soap:Body>
</soap:Envelope>"""            
         val result = buildCURL(Seq("curl","--http1.1","-k", sourceURL), apiReqMethod, apiAuthType, apiAuthWay, credentials, fileName, filePath, apiResponseType, data) 
         if(!result.isEmpty){
          tempdf = parseXMLtoDF(result, sourcePKCols, cols, schema)
           tempdf = tempdf.withColumn("Name",lit(name))
           df = df.union(tempdf)
         } })
        tableDF = df
      } else if(TARGET_TABLE == "standing_alarms_summary") {
        var p_guids = spark.sql("select distinct ID from PSS_WHI.PARAMETER_GUIDS where Name like '%Standing%Alarms%'")
        var pids = p_guids.select("ID").collect().map(_(0))
        
        pids.foreach(e => {
         data = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
<soap:Body>
 <RunAnalysis xmlns="http://www.pas.com/webservices/">
      <AnalysisID>4f5e0404-4503-4840-a0e5-3094c4fd8ef8</AnalysisID>
      <ParameterSetID>"""+e+"""</ParameterSetID>
      <EndTimeUTC>"""+currentTime+"""</EndTimeUTC>
      <StartTimeUTC>2021-01-01T00:00:00.000</StartTimeUTC>
      <IncDetails>1</IncDetails>
    </RunAnalysis>
</soap:Body>
</soap:Envelope>"""            
         val result = buildCURL(Seq("curl","--http1.1","-k", sourceURL), apiReqMethod, apiAuthType, apiAuthWay, credentials, fileName, filePath, apiResponseType, data) 
         if(!result.isEmpty){
          tempdf = parseXMLtoDF(result, sourcePKCols, cols, schema)
           df = df.union(tempdf)
         } })
        tableDF = df
      }
      
      
      else {
        TARGET_TABLE match {
          
          case "dataowner_guids" => data ="""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <DataOwnerListForUser xmlns="http://www.pas.com/webservices/">
      <UserID>9d8d9925-6c28-4411-a01f-302b29b9f708</UserID>
    </DataOwnerListForUser>
  </soap:Body>
</soap:Envelope>"""
          case "analysis_guids" => data ="""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetAnalysisTreeForDataOwner xmlns="http://www.pas.com/webservices/">
      <DataOwnerID>00000000-0000-0000-0000-000000000000</DataOwnerID>
    </GetAnalysisTreeForDataOwner>
  </soap:Body>
</soap:Envelope>"""
          case "boundary" => data ="""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
   <GetNormalizedObjectXML xmlns="http://www.pas.com/webservices/">
   <dataOwner></dataOwner>
   <objectType>Boundary</objectType>
   <filterText></filterText>
   <properties></properties>
   </GetNormalizedObjectXML>
  </soap:Body>
</soap:Envelope>""" 
           case "control_loops" => data ="""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
   <GetNormalizedObjectXML xmlns="http://www.pas.com/webservices/">
      <dataOwner></dataOwner>
      <objectType>ControlLoop</objectType>
      <filterText></filterText>
      <properties></properties>
    </GetNormalizedObjectXML>
  </soap:Body>
</soap:Envelope>""" 
           case "tags" => data ="""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
   <GetDRQueryResultDataTableXML xmlns="http://www.pas.com/webservices/">
      <queryName>All D&amp;R RefineryTags</queryName>      
    </GetDRQueryResultDataTableXML>
  </soap:Body>
</soap:Envelope>""" 
          case "operator_changes" => data ="""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
   <GetNormalizedHistoryXML xmlns="http://www.pas.com/webservices/">
<dataOwner></dataOwner>
<objectType>EventJournal</objectType>
<filterText>Journal = 'Change'</filterText>
<properties></properties>
<startUtc>"""+sourceChgKeyLatestValues+"""</startUtc>
<endUtc>"""+currentTime+"""</endUtc>
<historyFilter></historyFilter>
<horizontal>1</horizontal>
</GetNormalizedHistoryXML>
  </soap:Body>
</soap:Envelope>""" 
           case "process_alarms" => data ="""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
   <GetNormalizedHistoryXML xmlns="http://www.pas.com/webservices/">
<dataOwner></dataOwner>
<objectType>EventJournal</objectType>
<filterText>Journal = 'Alarm'</filterText>
<properties></properties>
<startUtc>"""+sourceChgKeyLatestValues+"""</startUtc>
<endUtc>"""+currentTime+"""</endUtc>
<historyFilter></historyFilter>
<horizontal>1</horizontal>
</GetNormalizedHistoryXML>
  </soap:Body>
</soap:Envelope>""" 
          case "system_events" => data ="""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
   <GetNormalizedHistoryXML xmlns="http://www.pas.com/webservices/">
<dataOwner></dataOwner>
<objectType>EventJournal</objectType>
<filterText>Journal = 'System'</filterText>
<properties></properties>
<startUtc>"""+sourceChgKeyLatestValues+"""</startUtc>
<endUtc>"""+currentTime+"""</endUtc>
<historyFilter></historyFilter>
<horizontal>1</horizontal>
</GetNormalizedHistoryXML>
  </soap:Body>
</soap:Envelope>""" 
          case "process_excursions" => data ="""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
   <GetNormalizedHistoryXML xmlns="http://www.pas.com/webservices/">
<dataOwner></dataOwner>
<objectType>EventJournal</objectType>
<filterText>Journal = 'Excursion'</filterText>
<properties></properties>
<startUtc>"""+sourceChgKeyLatestValues+"""</startUtc>
<endUtc>"""+currentTime+"""</endUtc>
<historyFilter></historyFilter>
<horizontal>1</horizontal>
</GetNormalizedHistoryXML>
  </soap:Body>
</soap:Envelope>""" 
          case "kpi_history" => data ="""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
   <GetNormalizedHistoryXML xmlns="http://www.pas.com/webservices/">
      <dataOwner></dataOwner>
      <objectType>Kpi</objectType>
      <filterText></filterText>
      <properties></properties>
      <startUtc>"""+sourceChgKeyLatestValues+"""</startUtc>
      <endUtc>"""+currentTime+"""</endUtc>
      <historyFilter></historyFilter>
      <horizontal>1</horizontal>
    </GetNormalizedHistoryXML>
  </soap:Body>
</soap:Envelope>""" 
          case "kpireport" => data ="""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
   <GetKPIReportSectionResultsName xmlns="http://www.pas.com/webservices/">
      <rsName>Whiting Monthly KPI Report</rsName>
      <ReportTimeUTC>2021-08-10T00:00:00.000</ReportTimeUTC>
    </GetKPIReportSectionResultsName>
  </soap:Body>
</soap:Envelope>""" 
          case "kpigridresults_caption" =>  data ="""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
   <GetKPIGridNameResultsForNow xmlns="http://www.pas.com/webservices/">
      <KPIGridName>11PS Performance Metrics</KPIGridName>
    </GetKPIGridNameResultsForNow>
  </soap:Body>
</soap:Envelope>"""
          case _ =>
      }
      val result = buildCURL(Seq("curl","--http1.1","-k", sourceURL), apiReqMethod, apiAuthType, apiAuthWay, credentials, fileName, filePath, apiResponseType, data) 
      if(!result.isEmpty) {
            tableDF = parseXMLtoDF(result, sourcePKCols, cols, schema)
      }
      }
  
      
      if(columnChangeSchema!=null && !columnChangeSchema.trim.isEmpty) {
        tableDF = changeColumnSchema(tableDF, columnChangeSchema, sourceTimestampFormat)
        }
      
      // Upsert
      if(sourceChangeKeyCols != null && sourceChangeKeyCols.trim.nonEmpty && loadType == "APPEND") {
        println("Primary Keys for upsert")

        // Construct the merge condition query by joining primary key columns
        val primaryKeys = sourceChangeKeyCols.split(",").map(_.trim)
        val mergeCondition = primaryKeys.map(pk => s"eu.$pk = es.$pk").mkString(" AND ")          
        println(s"Merge query: $mergeCondition")

        // Create a temporary view for incremental data
        var incrementalView = "incrementalview"
        tableDF.createOrReplaceTempView(incrementalView)

        // Define and execute the SQL merge statement
        val sql = s"""
          MERGE INTO $TARGET_DATABASE.$TARGET_TABLE AS eu
          USING $incrementalView AS es
          ON $mergeCondition
          WHEN MATCHED THEN 
            UPDATE SET *
          WHEN NOT MATCHED THEN 
            INSERT *
        """
        spark.sql(sql)
        println(s"Successfully completed upsert for table $TARGET_DATABASE.$TARGET_TABLE")
      } else {
        saveADLS(tableDF, loadType, excludeColumns, TARGET_DATABASE, TARGET_TABLE, sourcePKCols)
      }
      
      if(loadType == "APPEND") {
        // Update sourceChgKeyLatestValues column in jobQueue table with the enddatetime used for this current run
        val baseQuery = "UPDATE audit.tblJobQueue SET sourceChgKeyLatestValues = ? WHERE jobGroup = ? AND jobOrder = ? AND jobNum = ?"
        val preparedStatement = connection.prepareStatement(baseQuery)

        TARGET_TABLE match {
          case "shelving_history" =>
            preparedStatement.setString(1, currentSeconds.toString)
            
          case "kpireport" =>
            val nmonth = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'00:00:000").format(LocalDateTime.now().plusMonths(1))
            preparedStatement.setString(1, nmonth)

          case _ =>
            preparedStatement.setString(1, currentTime.toString)
        }

        // Set the parameters for jobGroup, jobOrder, and jobNum
        preparedStatement.setInt(2, jobGroup)
        preparedStatement.setInt(3, jobOrder)
        preparedStatement.setInt(4, jobNum)

        // if(TARGET_TABLE == "shelving_history") {
        // query = "update audit.tblJobQueue set sourceChgKeyLatestValues='"+currentSeconds+"' where jobGroup="+ jobGroup +" and jobOrder="+jobOrder+" and jobNum="+jobNum
        // } else if(TARGET_TABLE == "kpireport") {
        //   var nmonth = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'00:00:000").format(LocalDateTime.now().plusMonths(1))
        //   query = "update audit.tblJobQueue set sourceChgKeyLatestValues='"+nmonth+"' where jobGroup="+ jobGroup +" and jobOrder="+jobOrder+" and jobNum="+jobNum
        // } else {
        // query = "update audit.tblJobQueue set sourceChgKeyLatestValues='"+currentTime+"' where jobGroup="+ jobGroup +" and jobOrder="+jobOrder+" and jobNum="+jobNum
        // }
        // preparedStatement = connection.prepareStatement(query)

        println(s"Update query is: ${query}")
        preparedStatement.executeUpdate()
        println(s">>> sourceChgKeyLatestValues updated in jobQueue table")
      }
    
      // Optimize and Vacuum delta table with upsert operation
      println(">>> Optimize and vacuum started")
      spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled","false")
      spark.conf.set("spark.databricks.delta.optimize.zorder.checkStatsCollection.enabled","false")
      spark.sql("OPTIMIZE " + TARGET_DATABASE + "." + TARGET_TABLE) 
      spark.sql("VACUUM " + TARGET_DATABASE + "." + TARGET_TABLE + " RETAIN 0 HOURS")
      println(">>> Optimize and vacuum completed")
      
      recInSrc = tableDF.count()
      recIng = spark.table(s"""$TARGET_DATABASE.$TARGET_TABLE""").count()
      recProcessed = recIng
      recFail = recInSrc - recIng
      operation = "JOB_SUCCESSFUL"
      processingDetails = TARGET_TABLE + " table ingested successfully"
      STATUS = "S"
      errorMsg = "N/A"
    }  else throw new Exception("CURL command XML response body is empty!");
  } catch {
    case ex: Exception =>
    val errorMsg = s"""
    |ADF: $dataFactoryName
    |PIPELINE: $pipelineName
    |LOCATION: Child job
    |TASK: Processing XML response
    |TARGET TABLE: $TARGET_DATABASE.$TARGET_TABLE
    |LINK: $url_return
    |EXCEPTION: ${ex}
    """.stripMargin.trim
    STATUS = "F"
    operation = "JOB_FAILED"
    recFail = recInSrc
    throw new Exception(errorMsg, ex)
    }
// Re generate Token
auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
connectionProperty = getJDBCConnectionProperty(auditDBAccessToken, "36000")
exitNotebook(operation, url_return, pkTblJobQueue, jobGroup, jobNum, jobOrder, jobStepNum, STATUS, processingDetails, errorMsg, recInSrc.toInt, recIng.toInt, recInSrc.toInt, recFail.toInt, JDBC_url, connectionProperty)
