// val jobGroup = "538"
// val jobOrder = "2"
// val runID = "manual_run" + java.sql.Timestamp.valueOf(LocalDateTime.now)
// val pipelineName = "manual_run"
// val dataFactoryName = "manual_run"
%run ./ModuleFunctions
%run ./parallel-notebooks
// Java Time for Date and Time Handling
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

// Java SQL Classes for Database Connections
import java.util.Properties
import java.sql._

// Scala Concurrency and Utility
import scala.concurrent._
import scala.util.control.Breaks._

// Jackson for JSON Mapping (Scala Integration)
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper

// Scala Collections
import scala.collection.mutable.ListBuffer
val start_time = java.sql.Timestamp.valueOf(LocalDateTime.now)
val jobGroup = dbutils.widgets.get("jobGroup")
val jobOrder = dbutils.widgets.get("jobOrder")
val runID = dbutils.widgets.get("runID")
val pipelineName = dbutils.widgets.get("pipelineName")
val dataFactoryName = dbutils.widgets.get("dataFactoryName")
var operation = ""
var errMsg = "NULL"
var message = ""
var processingDetails = ""
var STATUS = "R"
var auditDBSPId = ""
var auditDBSPPwd = ""
var auditDBAccessToken = ""
var failureNotificationEmailIDs = ""
var successNotificationEmailIDs = ""
var fkSourceApplication = ""
var query = ""
var connectionProperty:Properties = null
var connection : Connection = null 
var preparedStatement : PreparedStatement  = null
def logRunInfo_jG(operation: String, STATUS: String) {
  try {
    println(operation, STATUS)   
    val auditDBSPId = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKey)
    val auditDBSPPwd = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKeyPwd)
    val auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
    val connectionProperty = getJDBCConnectionProperty(auditDBAccessToken, "36000")
    val connection = DriverManager.getConnection(JDBC_url,connectionProperty)
    val query = "EXEC [audit].[SP_LOG_RUN_DETAILS_NEW] @operation = N'"+ operation +"', @runID = N'"+runID+"',@fkJobQueue = NULL, @jobGroup = "+jobGroup+", @jobNum = NULL, @jobOrder = "+jobOrder+", @jobStepNum = NULL, @status = N'"+STATUS+"', @processingDetails = N'"+processingDetails+"', @errorMsg = N'"+errMsg+"', @recInSource = NULL, @recIngested = NULL, @recProcessed = NULL, @recFailed = NULL"
    println(query)
    preparedStatement = connection.prepareStatement(query)
    preparedStatement.executeUpdate()
  } catch {
    case e: Exception => println(s"⚠️ Exception occurred while inserting to the run log table in logRunInfo_jG() function: ${e}");
    val currentTS = LocalDateTime.now().toString
    throw new Exception(s"⚠️ Exception occurred at ${currentTS} in logRunInfo_jG() function: ${e}")
  }
}

try {
  operation = "JOB_GROUP_STARTED"
  logRunInfo_jG(operation, STATUS)
  } 
catch {
      case ex: Exception => errMsg = "ADF: " + dataFactoryName + " | PIPELINE: " + pipelineName + " | LOCATION: Parent job in Databricks is failing to insert to the runlog table at the start | EXCEPTION: " + ex;
      STATUS = "F"
  }
var collection:org.apache.spark.sql.DataFrame = null
try {
  auditDBSPId = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKey)
  auditDBSPPwd = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKeyPwd)
  auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
  connectionProperty = getJDBCConnectionProperty(auditDBAccessToken, "108000")
  var query="(SELECT pkTblJobQueue, jobNum, jobStepNum, targetDBName, targetTblName, successNotificationEmailIDs, failureNotificationEmailIDs, fkSourceApplication, keyVaultName FROM audit.tblJobQueue where jobGroup="+jobGroup+" and jobOrder="+jobOrder+" and isActive='Y') as tab"  
  collection = spark.read.jdbc(url=JDBC_url, table=query, properties=connectionProperty)

  failureNotificationEmailIDs = collection.select("failureNotificationEmailIDs").take(1)(0)(0).toString()
  successNotificationEmailIDs = collection.select("successNotificationEmailIDs").take(1)(0)(0).toString()
  fkSourceApplication = collection.select("fkSourceApplication").take(1)(0)(0).toString()
  display(collection)
  } catch {
    case ex: Exception => errMsg = "ADF: " + dataFactoryName + " | PIPELINE: " + pipelineName + " | LOCATION: Parent job in Databricks is failing when fetching audit table metadata | EXCEPTION: " + ex;
    STATUS = "F"
}
var resultant:Seq[String] = null
try{
  var Notebooks= Seq[NotebookData]();
  for (row <- collection.rdd.collect)
  {    
    var nbData= NotebookData("Tables", 3600000, Map("pkTblJobQueue"->row.getAs("pkTblJobQueue").toString(),"jobGroup"->jobGroup, "jobOrder"->jobOrder, "jobNum"->row.getAs("jobNum").toString(), "jobStepNum"->row.getAs("jobStepNum").toString(), "runID"->runID, "pipelineName"->pipelineName, "dataFactoryName"->dataFactoryName, "keyVaultName"->row.getAs("keyVaultName").toString(), "failureNotificationEmailIDs"->row.getAs("failureNotificationEmailIDs").toString()))
    Notebooks = Notebooks :+ nbData;
  }

  val res = parallelNotebooks(Notebooks)
  resultant= Await.result(res, 380000 seconds) // this is a blocking call.
  } catch {
    case ex: Exception =>
    errMsg =  "ADF: " + dataFactoryName + " | PIPELINE: " + pipelineName + " | LOCATION: Parent job in Databricks is failing when spawning multiple notebooks in parallel | EXCEPTION: " + ex;
    STATUS = "F"
    }
var successCount = 0
var failureCount = 0
var succeededTables = new ListBuffer[String]()
var failedTables = new ListBuffer[String]()
var dbName = ""
var errorMsgStr = ""
val adfPipelineURL = "https://adf.azure.com/monitoring/pipelineruns/" + runID + "?factory=/subscriptions/" + ENV_SubscriptionID + "/resourceGroups/" + ENV_ResourceGroup + "/providers/Microsoft.DataFactory/factories/" + dataFactoryName

try{
  
  auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
  connectionProperty = getJDBCConnectionProperty(auditDBAccessToken, "36000")
  val query="(SELECT pkTblJobQueue, jobNum, jobStepNum,targetDBName, targetTblName, successNotificationEmailIDs, failureNotificationEmailIDs, fkSourceApplication, keyVaultName FROM audit.tblJobQueue where jobGroup="+jobGroup+" and jobOrder="+jobOrder+" and isActive='Y') as tab"
  collection = spark.read.jdbc(url=JDBC_url, table=query, properties=connectionProperty)
  val uniqueJobNums = collection.count()
  
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
      succeededTables += DbName.toString() + "." + tblName.toString()
    }
    else if (childStatus == "F")
    {
      failureCount = failureCount +1
      
      var priKey = jsonMapper.readValue[Map[String, String]](value).get("fkJobQueue").getOrElse("")
      var tblName = collection.where(collection("pkTblJobQueue") === priKey).select("targetTblName").collectAsList().get(0).get(0)
      var DbName = collection.where(collection("pkTblJobQueue") === priKey).select("targetDBName").collectAsList().get(0).get(0)
      errorMsgStr = jsonMapper.readValue[Map[String, String]](value).get("errMsg").getOrElse("")
      
      failedTables += DbName.toString() + "." + tblName.toString()
     }
  }  
  val succeededTablesStr = succeededTables.mkString(", ")
  var failedTableStr = failedTables.mkString(", ")
  var failedTableCount = failedTables.size
  val totalTableCount = resultant.size
    
  if (failureCount == 0)
  {
        STATUS = "S"
        operation = "JOB_GROUP_SUCCESSFUL"
        logRunInfo_jG(operation, STATUS)
  }
  else if (failureCount > 0)
   {
        STATUS = "F"
        operation = "JOB_GROUP_FAILED"
        logRunInfo_jG(operation, STATUS)
  }
  println("STATUS : ", STATUS)
  
}
catch
{
  case ex: Exception => 
        errMsg = "Exception Occured for Pipeline "+ pipelineName + "while updating runlog table for source "+fkSourceApplication+ " \n "+ex
        STATUS = "F"
}
finally
{ 
  STATUS match{
    case "S" =>
      dbutils.notebook.exit("SUCCESS")
    case "F" =>
      throw new Exception("Error occurred!")
    case _ =>
  }
}
