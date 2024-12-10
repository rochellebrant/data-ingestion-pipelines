val ENV_TenantId = "ea80952e-a476-42d4-aaf4-5457852b0f7e"
val ENV_SubscriptionID ="f55d4ef9-4d7f-4763-8661-9b82de6c08c9"
val ENV_ResourceGroup = "ZNE-UDL1-P-40-ING-RSG"
var ENV_KeyVaultScope="ZNEUDL1P40INGing-kvl00"

var ENV_AuditTableServer = "zneudl1p12sqlsrv002.database.windows.net"
var ENV_AuditTableDB = "zneudl1p12sqldb001"
var ENV_AuditDBSPKey="spid-BI-zne-udl1-p-12-udl-rsg" 
var ENV_AuditDBSPKeyPwd="sppwd-BI-zne-udl1-p-12-udl-rsg"

var ENV_SQLDWServer = "zneudl1p33dessql01.database.windows.net"
var ENV_SQLDW = "zneudl1p33desdwsqldb01"

var ENV_storageACKeySecretName = "AccKey-zneudl1p33lakesstor"
var ENV_storagACName = "zneudl1p33lakesstor.core.windows.net"
var JDBC_url = "jdbc:sqlserver://zneudl1p12sqlsrv002.database.windows.net:1433;database=zneudl1p12sqldb001"

import com.microsoft.aad.adal4j.ClientCredential
import com.microsoft.aad.adal4j.AuthenticationContext
import java.util.concurrent.Executors
// import com.microsoft.azure.sqldb.spark.config.Config
// import com.microsoft.azure.sqldb.spark.connect._
// import com.microsoft.azure.sqldb.spark.query._
import java.util.Properties
import org.apache.spark.sql.DataFrame
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager

def getDBAccessToken(ServicePrincipalId : String, ServicePrincipalPwd : String) : String = {

  val TenantId = "ea80952e-a476-42d4-aaf4-5457852b0f7e"//"ea80952e-a476-42d4-aaf4-5457852b0f7e"
  val authority = "https://login.windows.net/" + TenantId
  val resourceAppIdURI = "https://database.windows.net/"
  
  val service = Executors.newFixedThreadPool(1)
  val context = new AuthenticationContext(authority, true, service);
  val ClientCred = new ClientCredential(ServicePrincipalId, ServicePrincipalPwd)
  val authResult = context.acquireToken(resourceAppIdURI, ClientCred, null)
  val accessToken = authResult.get().getAccessToken
  
  return accessToken
}

def getJDBCConnectionProperty(accessToken:String, fetchSize:String):Properties=  {
            
       // Get Data From Oracle Source System
      val connectionProperties:Properties = new Properties()
      val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      connectionProperties.setProperty("Driver", driverClass)
      connectionProperties.put("fetchsize", fetchSize)
      connectionProperties.put("accessToken", accessToken)      
      connectionProperties.put("hostNameInCertificate", "*.database.windows.net")
      connectionProperties.put("encrypt", "true")
      connectionProperties
 }

case class property( Message: String, Subject:String, EmailTo: String )

def sendEmail(msg: String, sub: String, email: String){
import java.io._
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import com.google.gson.Gson
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder;
  
    // create our object as a json string
    val spock=new property(msg, sub, email)
    //val spock = new properties(prop)
    val spockAsJson = new Gson().toJson(spock)
//val spockAsJsonString=spockAsJson.replace("\"","")

    // add name value pairs to a post object [logic App]
  val post = new HttpPost("https://prod-62.northeurope.logic.azure.com:443/workflows/bbd53bef627d44029b2c996b45aa2dd1/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=u-GC01O4yghBe79dL2XjADcY1GDdtdCjZSGYK80G9O8")
  
  post.setHeader("Content-type", "application/json")

  // add the JSON as a StringEntity
  post.setEntity(new StringEntity(spockAsJson))

  // send the post request
  val response = (HttpClientBuilder.create().build()).execute(post)
}

// @throws(classOf[SQLException])
def createExtTbl(jdbcUrl:String, connectionProperty:Properties, datasource : String, db_name : String, tbl_name : String, extSchema: String, forceTblRecreate:String) = {
// try{
  val connection = DriverManager.getConnection(jdbcUrl,connectionProperty)
  var preparedStatement:PreparedStatement = null
  
  //Check external table exist or not
  val existQuery = "(SELECT count(*) as cnts FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '"+extSchema+"' AND TABLE_NAME = '"+tbl_name+"') as tab"
//   val exist = sqlContext.read.sqlDB(sqlDBTable(ENV_SQLDWServer, ENV_SQLDW, existQuery, sqlAccessToken)).rdd.map(_(0)).collect.toList.head
   val exist = spark.read.jdbc(url=jdbcUrl, table=existQuery, properties=connectionProperty).rdd.map(_(0)).collect.toList.head
  // Get location of Database
  if(exist == 0 || "Y".equals(forceTblRecreate)){    
    val DBdf=spark.sql("DESCRIBE DATABASE " +db_name)
    val loc = DBdf.filter($"database_description_item" ==="Location").select("database_description_value")
    val location = loc.rdd.take(1)(0).getString(0)   
    println(location)
    //Get DDL of Database table
    val SQL="SHOW CREATE TABLE " +db_name+ "."+ tbl_name
    val df=spark.sql(SQL)
    val string=df.rdd.take(1)
    // Replace specific Data type in DDL
    val newStr = string(0).getString(0).replace("`","").replace("\n","").replace(" STRING,"," NVARCHAR(MAX),").replace(" STRING)"," NVARCHAR(MAX))").replace(" DOUBLE"," FLOAT").replace("TIMESTAMP","DATETIME").replace("BOOLEAN","BIT").replace("References","Reference_s").replace("Current","Current_0").replace("CREATE TABLE", "CREATE EXTERNAL TABLE").replace(db_name, extSchema)

    val create_statement = newStr.replace("USING parquet", " WITH( LOCATION='"+location+ "/" + tbl_name.toLowerCase() + "', DATA_SOURCE="+datasource.replace("'","")+", FILE_FORMAT=Parquet);" ).replace("abfss://udl-container@zneudl1p48defstor01.dfs.core.windows.net","")
    val drop_statement="IF EXISTS ( SELECT * FROM sys.external_tables WHERE object_id = OBJECT_ID('"+ extSchema+ "."+ tbl_name + "') ) DROP EXTERNAL TABLE " + extSchema+ "."+ tbl_name +";"
    val final_statement = drop_statement + create_statement
    println(final_statement)
//     sqlContext.sqlDBQuery(sqlDBquery(ENV_SQLDWServer, ENV_SQLDW, final_statement, sqlAccessToken))
    preparedStatement = connection.prepareStatement(final_statement)
  preparedStatement.executeUpdate()
  }
// }catch{
//   case e: Exception => throw new Exception(e);  
// }
}

  def getAPIAccessToken(url : String, ServicePrincipalId : String, ServicePrincipalPwd : String) : String = {

  val TenantId = "ea80952e-a476-42d4-aaf4-5457852b0f7e"
  val authority = "https://login.windows.net/" + TenantId
  val resourceAppIdURI = url//"https://database.windows.net/"
  
  val service = Executors.newFixedThreadPool(1)
  val context = new AuthenticationContext(authority, true, service);
  val ClientCred = new ClientCredential(ServicePrincipalId, ServicePrincipalPwd)
  val authResult = context.acquireToken(resourceAppIdURI, ClientCred, null)
  val accessToken = authResult.get().getAccessToken
  
  return accessToken
}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import java.sql.Timestamp

def exitNotebook(operation:String, jobID:String, pkTblJobQueue:Int, jobGroup:Int, jobNum:Int, jobOrder:Int, jobStepNum:Int, STATUS:String, processingDetails:String, errMsg:String, recInSource:Int, recIngested:Int, recProcessed:Int, recFailed:Int, jdbcUrl:String, connectionProperty:Properties) = {
  val connection = DriverManager.getConnection(jdbcUrl,connectionProperty)
  // update runLog table
  val query = "EXEC [audit].[SP_LOG_RUN_DETAILS_NEW] @operation = N'"+ operation +"', @runID = N'"+jobID+"',@fkJobQueue = "+pkTblJobQueue+", @jobGroup = "+jobGroup+", @jobNum = "+jobNum+", @jobOrder = "+jobOrder+", @jobStepNum = "+jobStepNum+", @status = N'"+STATUS+"', @processingDetails = N'"+processingDetails+"', @errorMsg = N'"+errMsg+"', @recInSource = "+recInSource+", @recIngested = "+recIngested+", @recProcessed = "+recProcessed+", @recFailed = "+recFailed+""
//     sqlContext.sqlDBQuery( sqlDBquery(ENV_AuditTableServer, ENV_AuditTableDB, query, auditDBAccessToken))
  val preparedStatement = connection.prepareStatement(query)
  preparedStatement.executeUpdate()
  //Send JSON string of Info back to parent notebook
  val jsonMapper = new ObjectMapper with ScalaObjectMapper
  jsonMapper.registerModule(DefaultScalaModule)
  val str =  Map("runID" -> jobID, "fkJobQueue" -> pkTblJobQueue, "jobGroup" -> jobGroup, "jobOrder" -> jobOrder, "jobNum" -> jobNum, "jobStepNum" -> jobStepNum, "errMsg" -> errMsg, "STATUS" -> STATUS)
  dbutils.notebook.exit(jsonMapper.writeValueAsString(str))
}

import scalaj.http.Http
import javax.net.ssl._
import java.security.cert.X509Certificate
import scala.io.Source

def bypass() = {
  
  // Bypasses both client and server validation.
object TrustAll extends X509TrustManager {
  val getAcceptedIssuers = null
  def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String) = {}
  def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String) = {}  
}
// Verifies all host names by simply returning true.
object VerifiesAllHostNames extends HostnameVerifier {
  def verify(s: String, sslSession: SSLSession) = true
}
  
val sslContext = SSLContext.getInstance("TLSv1.2")
sslContext.init(null, Array(TrustAll), new java.security.SecureRandom())
HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory)
HttpsURLConnection.setDefaultHostnameVerifier(VerifiesAllHostNames)  
}

import org.apache.spark.sql.types.{StructType,ArrayType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode_outer
import scala.collection.mutable._

def flattenDataframe(df: DataFrame): DataFrame = {

    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    val length = fields.length
    
    for(i <- 0 to fields.length-1){
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name
      fieldtype match {
        case arrayType: ArrayType =>
          val fieldNamesExcludingArray = fieldNames.filter(_!=fieldName)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
          val explodedDf = df.selectExpr(fieldNamesAndExplode:_*)
          return flattenDataframe(explodedDf)
        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName +"."+childname)
          val newfieldNames = fieldNames.filter(_!= fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))))
         val explodedf = df.select(renamedcols:_*)
          return flattenDataframe(explodedf)
        case _ =>
      }
    }
  val cols = fieldNames.map(c => col(c.toString()).as(c.toString().substring(c.toString().indexOf("_")+1))) // removes prefix & underscore when flattening
  return df.select(cols:_*)
//   df
  }

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column

def dropSubColumn(col: Column, colType: DataType, fullColName: String, dropColName: String): Option[Column] = {
    if (fullColName.equals(dropColName)) {
      None
    } else if (dropColName.startsWith(s"$fullColName.")) {
      colType match {
        case colType: StructType =>
          Some(struct(
            colType.fields
                .flatMap(f =>
                  dropSubColumn(col.getField(f.name), f.dataType, s"$fullColName.${f.name}", dropColName) match {
                    case Some(x) => Some(x.alias(f.name))
                    case None => None
                  })
                : _*))
        case colType: ArrayType =>
          colType.elementType match {
            case innerType: StructType =>
              // we are potentially dropping a column from within a struct, that is itself inside an array
              // Spark has some very strange behavior in this case, which they insist is not a bug
              // see https://issues.apache.org/jira/browse/SPARK-31779 and associated comments
              // and also the thread here: https://stackoverflow.com/a/39943812/375670
              // this is a workaround for that behavior

              // first, get all struct fields
              val innerFields = innerType.fields
              // next, create a new type for all the struct fields EXCEPT the column that is to be dropped
              // we will need this later
              val preserveNamesStruct = ArrayType(StructType(
                innerFields.filterNot(f => s"$fullColName.${f.name}".equals(dropColName))
              ))
              // next, apply dropSubColumn recursively to build up the new values after dropping the column
              val filteredInnerFields = innerFields.flatMap(f =>
                dropSubColumn(col.getField(f.name), f.dataType, s"$fullColName.${f.name}", dropColName) match {
                    case Some(x) => Some(x.alias(f.name))
                    case None => None
                }
              )
              // finally, use arrays_zip to unwrap the arrays that were introduced by building up the new. filtered
              // struct in this way (see comments in SPARK-31779), and then cast to the StructType we created earlier
              // to get the original names back
              Some(arrays_zip(filteredInnerFields:_*).cast(preserveNamesStruct))
          }

        case _ => Some(col)
      }
    } else {
      Some(col)
    }
  }

  def dropColumn(df: DataFrame, colName: String): DataFrame = {
    df.schema.fields.flatMap(f => {
      if (colName.startsWith(s"${f.name}.")) {
        dropSubColumn(col(f.name), f.dataType, f.name, colName) match {
          case Some(x) => Some((f.name, x))
          case None => None
        }
      } else {
        None
      }
    }).foldLeft(df.drop(colName)) {
      case (df, (colName, column)) => df.withColumn(colName, column)
    }
  }

// removes 1 or more columns from json_string schema (to be used before flattenDataframe()); takes comma-separated arguments from excludeColumns field in audit table
  def removingColumns(df: DataFrame, excludeColumns: String): DataFrame = {
    var df_ex = df
    val exclCol = excludeColumns.split(",")

    for(i <- 0 until exclCol.length) {
      df_ex = dropColumn(df_ex, exclCol(i))
    }
    df_ex
  }

import io.delta.tables._
import org.apache.spark.sql.DataFrame

var cond = ""
def incrementalLoad(newTableDF:DataFrame, sourcePKCols:String, TARGET_DATABASE:String, SOURCE_TB:String) ={
    val target = DeltaTable.forName("STG_"+TARGET_DATABASE+"."+SOURCE_TB)
    if(sourcePKCols != null && sourcePKCols.contains(";")){
//       sourcePKCols.split(";").foreach(cond = cond + "target."+ _+" = source." +_)
      val PKcols = sourcePKCols.split(";")
      for(i <- 0 until PKcols.length){
        cond = cond + " target."+ PKcols(i)+" = source." +PKcols(i)
        if(i != PKcols.length-1) cond += " AND"
      }
    }
    else{
      cond =  "target."+ sourcePKCols+" = source." +sourcePKCols
    }
  newTableDF.printSchema()
    println(cond)
    target.alias("target").merge(newTableDF.alias("source"), cond).whenMatched.updateAll.whenNotMatched.insertAll.execute()
}

import scalaj.http._
import org.apache.spark.sql.functions._

def getResp(url:String, reqMethod:String , auth_type:String , auth_way:String, additionalParams:String, credentials:scala.collection.immutable.Map[String, String], contentType:String): HttpResponse[String]= {
  // Bypasses both client and server validation.
  
  var t = scala.collection.Seq[(String, String)]()
//   var resp:HttpResponse[String]
  var req:HttpRequest = Http(url).timeout(connTimeoutMs = 1000, readTimeoutMs = 600000)
  
  if("QueryParam".equals(auth_way) && ("Bearer Token".equals(auth_type) || "API Key".equals(auth_type)|| "OAuth2.0".equals(auth_type)|| "Basic".equals(auth_type) || "No Auth".equals(auth_type))){
    println("🔓 Auth method: " + auth_way + "\n🔑Auth type: " + auth_type)
    for ((k,v) <- credentials){req = req.param(k, v)}   
  }
  else if("header".equals(auth_way) && ("Bearer Token".equals(auth_type) || "API Key".equals(auth_type)|| "OAuth2.0".equals(auth_type)|| "Basic".equals(auth_type) || "No Auth".equals(auth_type))){
    println("🔓 Auth method: " + auth_way + "\n🔑 Auth type: " + auth_type)
    req = req.headers(credentials)   
  }
  else if("PostForm".equals(auth_way)){
    t = credentials.toSeq
    println("POST Form = " + t.getClass)    
  }
   
  if(additionalParams != null && !additionalParams.isEmpty() && additionalParams.contains(";")){
     println("Multiple parameters -> " + additionalParams)
       for(s <- additionalParams.split(";")){
        req = req.param(s.split("=")(0), s.split("=")(1))
      }
    }
  else if(additionalParams != null && !additionalParams.isEmpty() && !additionalParams.contains(";") && !"null".equals(additionalParams)){
     println("One parameter -> " + additionalParams)
     req = req.param(additionalParams.split("=")(0), additionalParams.split("=")(1))
   }
  
  req = req.header("Content-Type", contentType).header("Charset", "UTF-8")
  bypass()
  println("📨 Request: " + reqMethod + "\n   Content type:" + contentType)
  
  reqMethod match{
    case "POST" =>
      return req.postForm(t).asString
    case "GET" =>
      return req.asString
    case _ =>
    return null
  }
}

import scalaj.http._
import org.apache.spark.sql.functions._
import scala.collection.mutable.Map
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
var df_fetch: org.apache.spark.sql.DataFrame = null
var df_fetch_fl: org.apache.spark.sql.DataFrame = null
var df: org.apache.spark.sql.DataFrame = null
var nurl = ""
var counter = 0
var params = ""


def getAllPagesData(url:String, reqMethod:String, auth_type:String, auth_way:String, additionalParams:String, credentials:Map[String, String], contentType:String, selectCols:String, paginationType:String, paginationURLKeyword:String, paginationURLLocation:String, excludeCols:String, tableDF: org.apache.spark.sql.DataFrame):DataFrame ={
    
  var response = getResp(url, reqMethod, auth_type, auth_way, additionalParams, credentials.toMap, contentType)
  if(response.isSuccess){
    println(response.statusLine)
    var json_string = response.body
    
  if(!("[]".equals(json_string))) {
    df_fetch = spark.read.json(Seq(json_string).toDS).select(selectCols)
    df_fetch_fl = flattenDataframe(df_fetch)
      
    println("\nFETCHED COUNT " + df_fetch_fl.count())
    println("\nTABLE COUNT \n\tbefore union: " + tableDF.count())
    df = tableDF.union(df_fetch_fl)
    println("\tafter union: " + df.count() + "\n")
  }
    else {  return df  }
    
  //Next Page (pagination) 
  paginationType match{
    case "HEADER" =>
      val id = response.header(paginationURLKeyword).get
      if(!paginationURLLocation.equals(id)){
        credentials += (paginationURLKeyword->id)
        getAllPagesData(url, reqMethod, auth_type, auth_way, additionalParams, credentials, contentType, selectCols, paginationType, paginationURLKeyword, paginationURLLocation, excludeCols, df)
      }
    case "QueryParam" =>
     if(df_fetch.count() == paginationURLLocation.toInt){
       counter+=1
       val arr = additionalParams.split(";")
       for(x <- 0 to arr.length-1){
         if(x==arr.length-1) arr(x) = arr(x).replace(additionalParams.substring(additionalParams.lastIndexOf("=")),"="+counter*paginationURLLocation.toInt)
       }
       params = arr.mkString(";")
       println(params)
       getAllPagesData(url, reqMethod, auth_type, auth_way, params, credentials, contentType, selectCols, paginationType, paginationURLKeyword, paginationURLLocation, excludeCols, df)
        }
//     case "AbsoluteURL" =>
//       if(json_string.contains(paginationURLLocation)){
//         nurl = flattenDataframe(spark.read.json(Seq(json_string).toDS).select(excludeCols)).where(paginationURLKeyword+"='"+paginationURLLocation+"'").select("href").head.get(0).toString()
//         getAllPagesData(nurl, reqMethod, auth_type, auth_way, additionalParams, credentials, contentType, selectCols, paginationType, paginationURLKeyword, paginationURLLocation, excludeCols, df)
//       }
  
    case "AbsoluteURL" =>
//     println("paginationURLLocation : ", paginationURLLocation)
    if(json_string.contains(paginationURLLocation)){
      nurl = spark.read.json(Seq(json_string).toDS).toDF("col_1", "col_2", "col_3").select("col_2").head.get(0).toString()
      println("nurl ---> ", nurl)
        getAllPagesData(nurl, reqMethod, auth_type, auth_way, additionalParams, credentials, contentType, selectCols, paginationType, paginationURLKeyword, paginationURLLocation, excludeCols, df)
      }
    case "RelativeURL" => 
    case _ =>
    } 
   df
  }
  else{
    println("response : ", response.statusLine)
    tableDF
  } 
}

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{to_date, to_timestamp}
import org.apache.spark.sql.functions._

def changeColumnSchema(tableDF: org.apache.spark.sql.DataFrame, transformedColumnsInTarget:String):DataFrame = {
  
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
   println("INSIDE changeColumnSchema()")
    var updatedDF = tableDF
    val newCol = transformedColumnsInTarget.split(";")

    for(i <- 0 until newCol.length) {
      val cols = newCol(i).split(":")(0)
      val dtype: String = newCol(i).split(":")(1)
      if (newCol(i).split(":").length == 3) {
        val n_name = newCol(i).split(":")(2)
        updatedDF = updatedDF.withColumn(cols, updatedDF(cols).cast(dtype)).as(n_name)
      }
      if (newCol(i).split(":").length == 2) {
          updatedDF = updatedDF.withColumn(cols, updatedDF(cols).cast(dtype))
        }
      }
    updatedDF
  }

// replaced "transformedColumnsInTarget" with all "columnChangeSchema"

import org.apache.spark.sql._
def saveADLS(tableDF:DataFrame, loadType:String, excludeColumns:String, TARGET_DATABASE:String, TARGET_TABLE:String, sourcePKCols:String, deleteTable:String)={
  var new_data: org.apache.spark.sql.DataFrame = null
  println("  💾 Load type: " + loadType)
  loadType match{    
    case "SNP" =>
      if(spark.catalog.tableExists(TARGET_DATABASE+"."+TARGET_TABLE))
        spark.sql("TRUNCATE TABLE "+ TARGET_DATABASE+"."+TARGET_TABLE)
      if(excludeColumns!=null && !excludeColumns.toString().trim.isEmpty)
        tableDF.drop(excludeColumns).write.mode(SaveMode.Overwrite).saveAsTable(TARGET_DATABASE+"."+TARGET_TABLE)
      else
        tableDF.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable(TARGET_DATABASE+"."+TARGET_TABLE)
    case "APPEND" =>
      println("Append Exclude ->"+excludeColumns)
      if(!spark.catalog.tableExists(TARGET_DATABASE+"."+TARGET_TABLE)){
        if(excludeColumns!=null && !excludeColumns.toString().trim.isEmpty) 
          tableDF.drop(excludeColumns).write.format("parquet").saveAsTable(TARGET_DATABASE+"."+TARGET_TABLE)
        else 
          tableDF.write.format("parquet").saveAsTable(TARGET_DATABASE+"."+TARGET_TABLE)
      }
      else{
        if(excludeColumns!=null && !excludeColumns.toString().trim.isEmpty)
          tableDF.drop(excludeColumns).write.format("parquet").insertInto(TARGET_DATABASE+"."+TARGET_TABLE)
        else 
          tableDF.write.format("parquet").insertInto(TARGET_DATABASE+"."+TARGET_TABLE)
      }
    case "INC" =>
      println("INCR "+TARGET_DATABASE)
      if(!spark.catalog.tableExists(TARGET_DATABASE+"."+TARGET_TABLE)){
        println("First "+TARGET_TABLE)
        if(excludeColumns!=null && !excludeColumns.toString().trim.isEmpty)
          tableDF.drop(excludeColumns).write.format("delta").saveAsTable("STG_"+TARGET_DATABASE+"."+TARGET_TABLE)
        else
          tableDF.write.format("delta").saveAsTable("STG_"+TARGET_DATABASE+"."+TARGET_TABLE)
        println("After Write")
      }
      else{
          if(excludeColumns!=null && !excludeColumns.toString().trim.isEmpty)
             incrementalLoad(tableDF.drop(excludeColumns), sourcePKCols, TARGET_DATABASE, TARGET_TABLE)
          else
             incrementalLoad(tableDF, sourcePKCols, TARGET_DATABASE, TARGET_TABLE)          
        }
    new_data = spark.sql("select * from STG_"+TARGET_DATABASE+"."+TARGET_TABLE)
    println(" Before remove delete "+ new_data.count())    
    if(!"".equals(deleteTable)){
      println(deleteTable)
      val delete_data = spark.sql("select * from "+deleteTable)
      new_data = new_data.join(delete_data, Seq(sourcePKCols),"left_anti")//.show
      println(" After remove delete "+new_data.count())
    }    
    new_data.write.mode(SaveMode.Overwrite).saveAsTable(TARGET_DATABASE+"."+TARGET_TABLE) 
        
    case _ =>
  }
}

import sys.process._
def buildCURL(c:Seq[String], reqMethod:String, authType:String, authWay:String, credentials:Map[String, String], fileName:String, filePath:String, apiResponseType:String, p_data:String):String={
  var credential:Map[String, String] = Map.empty[String, String]
  var cmd:Seq[String] = c
  if(!"GET".equals(reqMethod))
    cmd = cmd++Seq("-X", reqMethod)

  // Authorization either Basic OR NTLM only pass as -u
  if("Authorization".equals(authWay)){
    val (user,pwd) = credentials.head
    val cred = user+":"+pwd
    authType match{
      case "Basic" =>          
           cmd = cmd++Seq("-u", cred)      
      case "NTLM" =>    
         cmd = cmd++Seq("--ntlm", "-u", cred)      
      case _ =>
        
    }
  }
  if("Authorization".equals(authWay) && credentials.size > 1){
    credential = credentials.tail
  }
  else if("Header".equals(authWay)){
    credential = credentials    
  }
  for((k,v)<-credential){
          val head = k+": "+v
          cmd = cmd++Seq("-H", head)  
        }
  if(("POST".equals(reqMethod)) && p_data!=null){
    cmd = cmd ++ Seq("-d",p_data)
  }
  
  println(filePath)
  // If API response has attachment, download options of CURL command
  if("Attachment".equals(apiResponseType))
    cmd = cmd++Seq( "-o", "/dbfs/"+fileName)
// return cmd
  cmd.foreach(println(_))
  // Run Curl command
  val data = cmd.!!
  if("Attachment".equals(apiResponseType) && filePath != null && fileName != null){   
    val strg_key = dbutils.secrets.get(ENV_KeyVaultScope, ENV_storageACKeySecretName)
    spark.conf.set("fs.azure.account.key."+ENV_storagACName,strg_key)
    val isCp = dbutils.fs.cp("dbfs:/"+fileName, filePath+fileName)
    println(isCp+" IN : "+apiResponseType)
    if(isCp) dbutils.fs.rm("dbfs:/"+fileName)
    return String.valueOf(isCp)
  }
 return data
}

import scalaj.http._
import org.apache.spark.sql.functions._
import scala.collection.mutable.Map
import org.apache.spark.sql._
// var tableDF: org.apache.spark.sql.DataFrame = null
var nurl = ""
var text = ""
def getAllPagesTEXTData(url:String, reqMethod:String, auth_type:String, auth_way:String, additionalParams:String, credentials:Map[String, String], contentType:String, selectCols:String, paginationType:String, paginationURLKeyword:String, paginationURLLocation:String, excludeCols:String):String ={  
var response = getResp(url, reqMethod, auth_type, auth_way, additionalParams, credentials.toMap, contentType)
  if(response.isSuccess){
     println(response.statusLine)
    if(response.body.isEmpty()) return text
    if(text.isEmpty())    text += response.body
    //else  text +=  response.body.substring(245)
  //Next Page (pagination) 
  paginationType match{
    case "HEADER" =>
      val id = response.header(paginationURLKeyword).get
      if(!paginationURLLocation.equals(id)){
        credentials += (paginationURLKeyword->id)
        getAllPagesTEXTData(url, reqMethod, auth_type, auth_way, additionalParams, credentials, contentType, selectCols, paginationType, paginationURLKeyword, paginationURLLocation, excludeCols)
      }
    case "QueryParam" =>
    case "AbsoluteURL" =>      
    case "RelativeURL" => 
    case _ =>
    } 
   return text
  }

  else{
    println(response.statusLine)
    return ""
  } 
}  

def parseXMLtoDF(xmlData:String, sourcePKCols:String, attrs:Array[String], schema:org.apache.spark.sql.types.StructType): org.apache.spark.sql.DataFrame = {
  var rows:Seq[Row] = Seq[Row]();
  var param: Seq[String] = Seq[String]();
  var xmlNodes = sourcePKCols.split(";")
  var data = ""
  if(xmlData.contains("&lt;") || xmlData.contains("&gt;")) 
    data = xmlData.replace("&lt;", "<").replace("&gt;", ">")
   else data = xmlData
      val xml = scala.xml.XML.loadString(data)   
  println(xmlNodes(0)+"xml NOdes length: "+xmlNodes.size)
      var responseCode = (xml \ xmlNodes(0))
  //Iterate and reach to the Inner most node of Interest
      for(node <- xmlNodes.slice(1, xmlNodes.length)){
        responseCode =(responseCode \ node)
      }
  println("Resonsecode Length: "+responseCode.size)
  //From Inner most node extract all attributes
  responseCode.foreach(row => { 
    for(attr <- attrs){
      val value = "@"+attr.trim()  // @Action
      param = param :+ (row \ value).text
    }
    rows = rows :+ Row.fromSeq(param)
    param = Seq[String]()
//     println(param.size)
  })
  println("Total: "+rows.size)
  val prow:RDD[Row] = sc.parallelize(rows)
  return spark.createDataFrame(prow, schema)
}

