import scala.concurrent.duration._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
Thread.sleep(60000)
%run ./ModuleFunctions
val col1 = dbutils.widgets.get("col1").toString
val col2 = dbutils.widgets.get("col2").toString
val tokenURL = dbutils.widgets.get("tokenURL").toString
val keyVaultName = dbutils.widgets.get("keyVaultName").toString
val tokenClientId = dbutils.widgets.get("tokenClientId").toString
val tokenClientCredential = dbutils.widgets.get("tokenClientCredential").toString
val sourceURL = dbutils.widgets.get("sourceURL").toString
val apiReqMethod = dbutils.widgets.get("apiReqMethod").toString
val apiAuthType = dbutils.widgets.get("apiAuthType").toString
val apiAuthWay = dbutils.widgets.get("apiAuthWay").toString
val additionalParams = dbutils.widgets.get("additionalParams").toString
val apiReqContentType = dbutils.widgets.get("apiReqContentType").toString
val selectCols = dbutils.widgets.get("selectCols").toString
val loadType = dbutils.widgets.get("loadType").toString
val excludeColumns = dbutils.widgets.get("excludeColumns").toString
val TARGET_DATABASE = dbutils.widgets.get("TARGET_DATABASE").toString
val TARGET_TABLE = dbutils.widgets.get("TARGET_TABLE").toString
val FAILURES_TABLE = dbutils.widgets.get("FAILURES_TABLE").toString
val run = dbutils.widgets.get("run").toInt
val sourcePKCols = dbutils.widgets.get("sourcePKCols").toString
val deleteTable = dbutils.widgets.get("deleteTable").toString
val dataFactoryName = dbutils.widgets.get("dataFactoryName").toString
val pipelineName = dbutils.widgets.get("pipelineName").toString
val url_return = dbutils.widgets.get("url_return").toString
val AccessTokenDuration = dbutils.widgets.get("AccessTokenDuration").toInt

var credentials = scala.collection.mutable.Map[String, String]()
var recInSrc:Long = 0
var operation = ""
var errMsg = ""
var STATUS = "R" 
var subject = ""
val failureNotificationEmailIDs = ""
var tableDF = spark.emptyDataFrame
var failuresDF = spark.emptyDataFrame
var df1 = spark.emptyDataFrame
val FAILURES_TABLE = "failures"

try{
  val schema = StructType(StructField(col1, StringType, false) :: StructField(col2, StringType, false) :: Nil)
  failuresDF = spark.createDataFrame(sc.emptyRDD[Row], schema) // schema is: well_log_set_curve_id, curve_file_id, url
  var d_ids = spark.sql("select * from " + TARGET_DATABASE + "." + FAILURES_TABLE)
  var curveid = d_ids.select(col1).collect().map(_(0)).map(_.asInstanceOf[String]) // well_log_set_curve_id
  var fileid = d_ids.select(col2).collect().map(_(0)).map(_.asInstanceOf[String]) // curve_file_id
  var c = curveid(0) // first well_log_set_curve_id
  var f = fileid(0) // first curve_file_id
  var url = sourceURL + c + "/file/" + f
  var APIAccessTokenEndTime = getAPIAccessTokenEndTime(AccessTokenDuration)
  var t = getAPIAccessToken(tokenURL, dbutils.secrets.get(keyVaultName.toString(),tokenClientId),dbutils.secrets.get(keyVaultName.toString(),tokenClientCredential))
  var bearer_token = "Bearer " + t
  credentials += ("Authorization" -> bearer_token)

  
  println("First API call -> " + url + "\nTarget db.table -> " + TARGET_DATABASE + "." + TARGET_TABLE + "\nCredentials  -> " + credentials.toMap + "\n\n🔓 Auth method: " + apiAuthWay + "\n🔑 Auth type: " + apiAuthType + "\n📨 Request: " + apiReqMethod + "\n   Content type: " + apiReqContentType)
  var resp = getResp(url, apiReqMethod, apiAuthType, apiAuthWay, additionalParams, credentials.toMap, apiReqContentType)
  if(!resp.isSuccess){println("❌ Status: " + resp.statusLine)}

  if(resp.isSuccess){
    println("✔️ Status: " + resp.statusLine + "\n")
    val json = resp.body
    df1 = flattenDataframe(spark.read.json(Seq(json).toDS).select(selectCols), selectCols).withColumn(col1, lit(c.toString)).withColumn(col2, lit(f.toString)).select("*")
    tableDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], df1.schema)
    var counter = 1
    
    if(curveid.length == fileid.length){
      getCurveData(curveid, fileid, col1, col2, counter, tokenURL, keyVaultName, tokenClientId, tokenClientCredential, sourceURL, apiReqMethod, apiAuthType, apiAuthWay, additionalParams, apiReqContentType, selectCols, loadType, excludeColumns, TARGET_DATABASE, TARGET_TABLE, FAILURES_TABLE, df1, tableDF, failuresDF, run, t, APIAccessTokenEndTime, AccessTokenDuration)

      }
    } // if success

  recInSrc = spark.sql("select * from " + TARGET_DATABASE + "." + TARGET_TABLE).count()
  operation = "JOB_SUCCESSFUL"
  STATUS = "S"
  println("\n🏁🏁🏁🏁🏁🏁🏁\n")
} catch {
  case ex: Exception =>
  errMsg = "ADF: " + dataFactoryName + " | PIPELINE: " + pipelineName + " | LOCATION: Child notebook in Databricks is failing for API Ingestion from " + fkSourceApplication + " for table " + TARGET_DATABASE + "." + TARGET_TABLE + " when re-running the API ingestion for the failed calls | EXCEPTION: " + ex;
  STATUS = "F"
  operation = "JOB_FAILED"
}
dbutils.notebook.exit(recInSrc.toString)
