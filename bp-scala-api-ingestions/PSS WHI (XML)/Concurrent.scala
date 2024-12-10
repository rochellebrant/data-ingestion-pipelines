import java.time.LocalDateTime
// import java.time.format.DateTimeFormatter

// dbutils.widgets.text("jobGroup","555")
// dbutils.widgets.text("jobOrder","1")
// dbutils.widgets.text("runID","run_id"+java.sql.Timestamp.valueOf(LocalDateTime.now))
// dbutils.widgets.text("pipelineName","pipeline_name")
// dbutils.widgets.text("dataFactoryName","dataFactory_name")
%run ./ModuleFunctions
%run ./parallel-notebooks
val start_time = java.sql.Timestamp.valueOf(LocalDateTime.now)

val jobGroup = dbutils.widgets.get("jobGroup")
val jobOrder = dbutils.widgets.get("jobOrder")
val runID = dbutils.widgets.get("runID")
val pipelineName = dbutils.widgets.get("pipelineName")
val dataFactoryName = dbutils.widgets.get("dataFactoryName")

var operation =""
var errorMsg = "NULL"
var message = "NULL"
var processingDetails = ""
var STATUS = "R"

var auditDBSPId = ""
var auditDBSPPwd = ""
var auditDBAccessToken = ""
var failureNotificationEmailIDs = ""
var successNotificationEmailIDs = ""
var fkSourceApplication = ""
var query = ""

var connectionProperty: Properties = _
var connection: Connection = _
var preparedStatement: PreparedStatement = _
def logRunInfo_jG(operation: String, STATUS: String) {
  try {
    println(operation, STATUS)   
    val auditDBSPId = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKey)
    val auditDBSPPwd = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKeyPwd)
    val auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
    val connectionProperty = getJDBCConnectionProperty(auditDBAccessToken, "36000")
    val connection = DriverManager.getConnection(JDBC_url,connectionProperty)
    val query = "EXEC [audit].[SP_LOG_RUN_DETAILS_NEW] @operation = N'"+ operation +"', @runID = N'"+runID+"',@fkJobQueue = NULL, @jobGroup = "+jobGroup+", @jobNum = NULL, @jobOrder = "+jobOrder+", @jobStepNum = NULL, @status = N'"+STATUS+"', @processingDetails = N'"+processingDetails+"', @errorMsg = N'"+errorMsg+"', @recInSource = NULL, @recIngested = NULL, @recProcessed = NULL, @recFailed = NULL"
    println(query)
    preparedStatement = connection.prepareStatement(query)
    preparedStatement.executeUpdate()
  } catch {
    case ex: Exception =>
    val currentTS = LocalDateTime.now().toString
    val errorMsg = s"⚠️ Exception occurred at $currentTS in logRunInfo_jG() function: $ex"
    throw new Exception(errorMsg, ex)
    }
}

try {
  operation = "JOB_GROUP_STARTED"
  logRunInfo_jG(operation, STATUS)
} catch {
  case ex: Exception => 
  errorMsg = s"""
  |ADF: $dataFactoryName
  |PIPELINE: $pipelineName
  |LOCATION: Parent job
  |TASK: Inserting to the runlog table at the start
  |EXCEPTION: ${ex}
  """.stripMargin.trim
  STATUS = "F"
  operation = "JOB_FAILED"
  throw new Exception(errorMsg, ex)
}
var collection:org.apache.spark.sql.DataFrame = null
try {
  auditDBSPId = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKey)
  auditDBSPPwd = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKeyPwd)
  auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
  connectionProperty = getJDBCConnectionProperty(auditDBAccessToken, "36000")
  val query="(SELECT pkTblJobQueue,jobGroup,jobNum,jobOrder,jobStepNum, targetDBName, targetTblName, successNotificationEmailIDs, failureNotificationEmailIDs, fkSourceApplication, keyVaultName FROM audit.tblJobQueue where jobGroup="+jobGroup+" and jobOrder="+jobOrder+" and isActive='Y') as tab"
  
  collection = spark.read.jdbc(url=JDBC_url, table=query, properties=connectionProperty).na.fill("")
  failureNotificationEmailIDs = collection.select("failureNotificationEmailIDs").take(1)(0)(0).toString()
  successNotificationEmailIDs = collection.select("successNotificationEmailIDs").take(1)(0)(0).toString()
  fkSourceApplication = collection.select("fkSourceApplication").take(1)(0)(0).toString()
  display(collection)
} catch {
  case ex: Exception => 
  errorMsg = s"""
  |ADF: $dataFactoryName
  |PIPELINE: $pipelineName
  |LOCATION: Parent job
  |TASK: Fetching audit table metadata
  |EXCEPTION: ${ex}
  """.stripMargin.trim
  STATUS = "F"
  operation = "JOB_FAILED"
  throw new Exception(errorMsg, ex)
}
import scala.concurrent._

var resultant:Seq[String] = _

try {
  var Notebooks= Seq[NotebookData]();
  for (row <- collection.rdd.collect) {    
    var nbData= NotebookData("Tables", 3600000, Map("pkTblJobQueue"->row.getAs("pkTblJobQueue").toString(),"jobGroup" -> row.getAs("jobGroup").toString(), "jobOrder" -> row.getAs("jobOrder").toString(), "jobNum" -> row.getAs("jobNum").toString(), "jobStepNum"->row.getAs("jobStepNum").toString(), "runID"->runID, "pipelineName"->pipelineName,"dataFactoryName"->dataFactoryName, "keyVaultName"->row.getAs("keyVaultName").toString(), "failureNotificationEmailIDs"->row.getAs("failureNotificationEmailIDs").toString()))
    Notebooks = Notebooks :+ nbData;  
  }

val res = parallelNotebooks(Notebooks)
resultant= Await.result(res, 380000 seconds) // this is a blocking call

} catch {
  case ex: Exception => 
  errorMsg = s"""
  |ADF: $dataFactoryName
  |PIPELINE: $pipelineName
  |LOCATION: Parent job
  |TASK: Attempting to spawn multiple notebooks in parallel
  |EXCEPTION: ${ex}
  """.stripMargin.trim
  STATUS = "F"
  operation = "JOB_FAILED"
  throw new Exception(errorMsg, ex)
}
import scala.util.control.Breaks._
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import scala.collection.mutable.ListBuffer

var successCount = 0
var failureCount = 0
var succeededTables = new ListBuffer[String]()
var failedTables = new ListBuffer[String]()
var dbName = ""
var errorMsgStr = ""
val adfPipelineURL = s"https://adf.azure.com/monitoring/pipelineruns/${runID}?factory=/subscriptions/${ENV_SubscriptionID}/resourceGroups/${ENV_ResourceGroup}/providers/Microsoft.DataFactory/factories/${dataFactoryName}"

auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
connectionProperty = getJDBCConnectionProperty(auditDBAccessToken, "36000")

val query = s"(SELECT pkTblJobQueue, jobNum, jobStepNum,targetDBName, targetTblName, successNotificationEmailIDs, failureNotificationEmailIDs, fkSourceApplication, keyVaultName FROM audit.tblJobQueue where jobGroup=${jobGroup} and jobOrder=${jobOrder} and isActive='Y') as tab"
collection = spark.read.jdbc(url = JDBC_url, table = query, properties = connectionProperty)    
val uniqueJobNums = collection.count()

try {
  val jsonMapper = new ObjectMapper with ScalaObjectMapper
  jsonMapper.registerModule(DefaultScalaModule)
  STATUS = "S"
  for (value <- resultant) 
  {
    val childStatus = jsonMapper.readValue[Map[String, String]](value).get("STATUS").getOrElse("")
    
    if (childStatus == "S")
    {
      successCount = successCount + 1
      
      var priKey = jsonMapper.readValue[Map[String, String]](value).get("fkJobQueue").getOrElse("")
      var tblName = collection.where(collection("pkTblJobQueue") === priKey).select("targetTblName").collectAsList().get(0).get(0)
      var DbName = collection.where(collection("pkTblJobQueue") === priKey).select("targetDBName").collectAsList().get(0).get(0)
      succeededTables+= DbName.toString() + "." + tblName.toString()
    }
    else if (childStatus == "F")
    {
      failureCount = failureCount + 1
      var priKey = jsonMapper.readValue[Map[String, String]](value).get("fkJobQueue").getOrElse("")
      var tblName = collection.where(collection("pkTblJobQueue") === priKey).select("targetTblName").collectAsList().get(0).get(0)
      var DbName = collection.where(collection("pkTblJobQueue") === priKey).select("targetDBName").collectAsList().get(0).get(0)
      errorMsgStr = jsonMapper.readValue[Map[String, String]](value).get("errorMsg").getOrElse("")
      failedTables+= DbName.toString() + "." + tblName.toString()
     }
  }  
  val succeededTablesStr = succeededTables.mkString(", ")
  var failedTableStr = failedTables.mkString(", ")
  var failedTableCount = failedTables.size
  val totalTableCount = resultant.size
    
  if (failureCount == 0) {
        STATUS = "S"
        operation = "JOB_GROUP_SUCCESSFUL"
  } else if (failureCount > 0) {
        STATUS = "F"
        operation = "JOB_GROUP_FAILED"
  }  
  
  logRunInfo_jG(operation, STATUS)
  println("STATUS: ", STATUS)
  
} catch {
  case ex: Exception => 
  errorMsg = s"""
  |ADF: $dataFactoryName
  |PIPELINE: $pipelineName
  |LOCATION: Parent job
  |TASK: Inserting to the runlog table following job completion
  |EXCEPTION: ${ex}
  """.stripMargin.trim
  STATUS = "F"
  operation = "JOB_FAILED"
  throw new Exception(errorMsg, ex)
  }
  finally { 
    STATUS match {
      case "S" =>
      dbutils.notebook.exit("SUCCESS")
      case "F" =>
      throw new Exception("An error occurred!")
      case _ =>
      }
}
