%run ./ModuleFunctions

%run ./parallel-notebooks

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.sql._
val start_time = java.sql.Timestamp.valueOf(LocalDateTime.now); //LocalDateTime.now //DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.ms").format(LocalDateTime.now)

val jobGroup = dbutils.widgets.get("jobGroup")
val jobOrder = dbutils.widgets.get("jobOrder")
val runID = dbutils.widgets.get("runID")
val pipelineName = dbutils.widgets.get("pipelineName")
val dataFactoryName = dbutils.widgets.get("dataFactoryName")

var operation =""
var errMsg = "NULL"
var message = ""
var subject = "Parent Job Databricks pipleline with JobGroup=" + jobGroup +" and JobOrder="+ jobOrder +" "
var processingDetails = ""
var STATUS = "R"

var auditDBSPId =""
var auditDBSPPwd = ""
var auditDBAccessToken = ""
var failureNotificationEmailIDs = ""
var successNotificationEmailIDs = ""
var fkSourceApplication = ""
var query = ""
var connectionProperty:Properties = null
var connection : Connection = null 
var preparedStatement : PreparedStatement  = null

try
{   
  auditDBSPId = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKey)
  auditDBSPPwd = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKeyPwd)
  auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
  connectionProperty = getJDBCConnectionProperty(auditDBAccessToken, "36000")
  connection = DriverManager.getConnection(JDBC_url,connectionProperty)
  operation = "JOB_GROUP_STARTED"
  query = "EXEC [audit].[SP_LOG_RUN_DETAILS_NEW] @operation = N'"+ operation +"', @runID = N'"+runID+"',@fkJobQueue = NULL, @jobGroup = "+jobGroup+", @jobNum = NULL, @jobOrder = "+jobOrder+", @jobStepNum = NULL, @status = N'"+STATUS+"', @processingDetails = N'"+processingDetails+"', @errorMsg = N'"+errMsg+"', @recInSource = NULL, @recIngested = NULL, @recProcessed = NULL, @recFailed = NULL"
println(query)  
//   sqlContext.sqlDBQuery( sqlDBquery(ENV_AuditTableServer, ENV_AuditTableDB, query, auditDBAccessToken)) 
//   spark.read.jdbc(url=jdbcUrl, table=tblOrQuery, properties=connectionProperties)
//   preparedStatement = connection.prepareStatement(query)
//   preparedStatement.executeUpdate()
}catch{
  case ex: Exception => errMsg = "Azure Data Factory " + dataFactoryName + " pipeline " + pipelineName + ".\n\nLocation: Parent Job is failing at databricks while inserting records in runlog table at starting of pipeline. \n\n Refer Exception: \n" + ex;
        STATUS = "F"
        subject += " is Failing."
        sendEmail(errMsg, subject, failureNotificationEmailIDs);
}

var collection:org.apache.spark.sql.DataFrame = null
try{
  auditDBSPId = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKey)
  auditDBSPPwd = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKeyPwd)
  auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
  connectionProperty = getJDBCConnectionProperty(auditDBAccessToken, "36000")
  val query="(SELECT pkTblJobQueue, jobNum, jobStepNum,targetDBName, targetTblName, successNotificationEmailIDs, failureNotificationEmailIDs, fkSourceApplication, keyVaultName FROM audit.tblJobQueue where jobGroup="+jobGroup+" and jobOrder="+jobOrder+" and isActive='Y') as tab"
//   println(connectionProperty)
//   collection = sqlContext.read.sqlDB(sqlDBTable( "zneudl1p48defdevtstsqlsrv001.database.windows.net", "zneudl1p48defdevtstsqldb02", query, auditDBAccessToken))
  collection = spark.read.jdbc(url=JDBC_url, table=query, properties=connectionProperty)

  failureNotificationEmailIDs = collection.select("failureNotificationEmailIDs").take(1)(0)(0).toString()
  successNotificationEmailIDs = collection.select("successNotificationEmailIDs").take(1)(0)(0).toString()
  fkSourceApplication = collection.select("fkSourceApplication").take(1)(0)(0).toString()
  display(collection)
}catch{
  case ex: Exception => errMsg = "Azure Data Factory "+dataFactoryName + " pipeline "+ pipelineName+".\n\nLocation: Parent Job is failing at databricks pipeline while fetching audit table info at parent level. \n\n Refer Exception: \n" +ex;
        STATUS = "F"
        subject += " is Failing"
        sendEmail(errMsg, subject, failureNotificationEmailIDs);
}

import scala.concurrent._

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
}catch{
  case ex: Exception => errMsg =  "Azure Data FActory "+dataFactoryName + " pipeline "+ pipelineName+".\n\nLocation: Parent Job is failing at databricks pipeline while spawn multiple notebools in parallel. \n\n Refer Exception: \n" +ex;
        STATUS = "F"
        subject += " is Failing"
        sendEmail(errMsg, subject, failureNotificationEmailIDs);
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
val uniqueJobNums = collection.count()
val adfPipelineURL = "https://adf.azure.com/monitoring/pipelineruns/" + runID + "?factory=/subscriptions/" + ENV_SubscriptionID + "/resourceGroups/" + ENV_ResourceGroup + "/providers/Microsoft.DataFactory/factories/" + dataFactoryName

try{
  val jsonMapper = new ObjectMapper with ScalaObjectMapper
  jsonMapper.registerModule(DefaultScalaModule)
//   auditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
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
      failureCount = failureCount +1
      
      var priKey = jsonMapper.readValue[Map[String, String]](value).get("fkJobQueue").getOrElse("")
      var tblName = collection.where(collection("pkTblJobQueue") === priKey).select("targetTblName").collectAsList().get(0).get(0)
      var DbName = collection.where(collection("pkTblJobQueue") === priKey).select("targetDBName").collectAsList().get(0).get(0)
      errorMsgStr = jsonMapper.readValue[Map[String, String]](value).get("errMsg").getOrElse("")
      
      failedTables+= DbName.toString() + "." + tblName.toString()
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
        subject = fkSourceApplication+" API Ingestion with jobGroup "+jobGroup+" and jobOrder "+jobOrder+" is Successful."
        processingDetails = fkSourceApplication + "API ingestion is Successful"
        message = s"""Source Application : $fkSourceApplication <br>
               |ADF Pipeline : $pipelineName <br>
               |jobGroup : $jobGroup <br>
               |jobOrder : $jobOrder <br>
               |Table Name(s) : $succeededTablesStr <br>
               |Date : $start_time <br>
               |ADFPipelineURL : $adfPipelineURL """.stripMargin
  }
  else if (failureCount > 0)
   {
        STATUS = "F"
        operation = "JOB_GROUP_FAILED"
        subject = fkSourceApplication+" API Ingestion with jobGroup "+jobGroup+" and jobOrder "+jobOrder+" has failed."
        processingDetails = fkSourceApplication + "API ingestion has failed"
        message = s"""Source Application : $fkSourceApplication <br>
                      |ADF Pipeline : $pipelineName <br>
                      |jobGroup : $jobGroup <br>
                      |jobOrder : $jobOrder <br>
                      |Total Table Count : $totalTableCount <br>
                      |Succeeded Table(s) : $succeededTablesStr <br><br>
                      |Failed Table Count : $failedTableCount <br>
                      |Failed Table(s) : $failedTableStr <br>
                      |errMsg -> $errorMsgStr <br>
                      |Date : $start_time <br>
                      |ADFPipelineURL : $adfPipelineURL """.stripMargin
  }
  
  query = null
  query = "EXEC [audit].[SP_LOG_RUN_DETAILS_NEW] @operation = N'"+ operation +"', @runID = N'"+runID+"',@fkJobQueue = NULL, @jobGroup = "+jobGroup+", @jobNum = NULL, @jobOrder = "+jobOrder+", @jobStepNum = NULL, @status = N'"+STATUS+"', @processingDetails = N'"+processingDetails+"', @errorMsg = N'"+errMsg+"', @recInSource = NULL, @recIngested = NULL, @recProcessed = NULL, @recFailed = NULL"
  
  auditDBSPId = null
  auditDBSPPwd = null
  auditDBAccessToken = null
  connectionProperty = null
  connection = null
  println("Cleared DB connectivity variables")
  
  auditDBSPId = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKey)
  auditDBSPPwd = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKeyPwd)
  auditDBAccessToken = getDBAccessToken_(auditDBSPId, auditDBSPPwd)
  connectionProperty = getJDBCConnectionProperty(auditDBAccessToken, "108000")
  connection = DriverManager.getConnection(JDBC_url,connectionProperty)
  preparedStatement = connection.prepareStatement(query)
  println("Reinititated DB connectivity variables")
  preparedStatement.executeUpdate()
  
  println("STATUS : ", STATUS)
  
}
catch
{
  case ex: Exception => 
        errMsg = "Exception Occured for Pipeline "+ pipelineName + "while updating runlog table for source "+fkSourceApplication+ " \n "+ex
        STATUS = "F"
        subject = fkSourceApplication+" API Ingestion with jobGroup "+jobGroup+" and jobOrder "+jobOrder+" has failed with an exception"
        message = s"""Source Application : $fkSourceApplication <br>
                      |ADF Pipeline : $pipelineName <br>
                      |jobGroup : $jobGroup <br>
                      |jobOrder : jobOrder <br>
                      |Date : $start_time <br>
                      |errMsg -> $errMsg <br>
                      |ADFPipelineURL : $adfPipelineURL """.stripMargin
//         println("Error Msg: "+errMsg)
}
finally
{ 
  STATUS match{
    case "S" =>
      println("Success Email "+successNotificationEmailIDs)
      sendEmail(message, subject, successNotificationEmailIDs);
      dbutils.notebook.exit("SUCCESS")
    case "F" =>
      println("Failure Email "+failureNotificationEmailIDs)
      sendEmail(message, subject, failureNotificationEmailIDs);
      throw new Exception("Error occurred!")
//       dbutils.notebook.exit("FAILURE")
    case _ =>
  }
}
