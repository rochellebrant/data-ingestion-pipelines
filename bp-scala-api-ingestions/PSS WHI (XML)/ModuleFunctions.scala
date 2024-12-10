%md # Connections
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
%md # SQL SERVER Methods
import java.util.{Collections, Properties}
import java.util.concurrent.Executors
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.time.LocalDateTime

import com.microsoft.aad.msal4j._
import org.apache.spark.sql.DataFrame
import bp.dwx.tableUtils._
%md ##### getDBAccessToken()
def getDBAccessToken(ServicePrincipalId : String, ServicePrincipalPwd : String) : String = {
  try {
    val TenantId = "ea80952e-a476-42d4-aaf4-5457852b0f7e"
    val authority = "https://login.windows.net/" + TenantId
    val resourceAppIdURI = "https://database.windows.net/.default"
    val ClientCred = ConfidentialClientApplication.builder(ServicePrincipalId, ClientCredentialFactory.createFromSecret(ServicePrincipalPwd)).authority(authority).build()
    val clientCredentialParam = ClientCredentialParameters.builder(Collections.singleton(resourceAppIdURI)).build()
    val accessToken = ClientCred.acquireToken(clientCredentialParam).get().accessToken()
    return accessToken
  } catch {
    case ex: Exception =>
    val currentTS = LocalDateTime.now().toString
    val errorMsg = s"⚠️ Exception occurred at $currentTS in getDBAccessToken() function: $ex"
    throw new Exception(errorMsg, ex)
  }
}
%md ##### getJDBCConnectionProperty()
def getJDBCConnectionProperty(accessToken:String, fetchSize:String) : Properties=  {
     try {
          val connectionProperties:Properties = new Properties()
          val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
          connectionProperties.setProperty("Driver", driverClass)
          connectionProperties.put("fetchsize", fetchSize)
          connectionProperties.put("accessToken", accessToken)      
          connectionProperties.put("hostNameInCertificate", "*.database.windows.net")
          connectionProperties.put("encrypt", "true")
          connectionProperties
     } catch {
          case ex: Exception =>
          val currentTS = LocalDateTime.now().toString
          val errorMsg = s"⚠️ Exception occurred at $currentTS in getJDBCConnectionProperty() function: $ex"
          throw new Exception(errorMsg, ex)
     }
 }
%md # General Functions
case class property( Message: String, Subject:String, EmailTo: String )
%md ##### createExtTbl()
// @throws(classOf[SQLException])
def createExtTbl(jdbcUrl:String, connectionProperty:Properties, datasource : String, db_name : String, tbl_name : String, extSchema: String, forceTblRecreate:String) = {
try {
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
} catch {
  case ex: Exception =>
  val currentTS = LocalDateTime.now().toString
  val errorMsg = s"⚠️ Exception occurred at $currentTS in createExtTbl() function: $ex"
  throw new Exception(errorMsg, ex)
}
}
%md # API Functions
%md ##### getAPIAccessToken()
def getAPIAccessToken(url : String, ServicePrincipalId : String, ServicePrincipalPwd : String) : String = {
  try {
    val TenantId = "ea80952e-a476-42d4-aaf4-5457852b0f7e"
    val authority = "https://login.windows.net/" + TenantId
    val resourceAppIdURI = url + "/.default"
    val ClientCred = ConfidentialClientApplication.builder(ServicePrincipalId, ClientCredentialFactory.createFromSecret(ServicePrincipalPwd)).authority(authority).build()
    val clientCredentialParam = ClientCredentialParameters.builder(Collections.singleton(resourceAppIdURI)).build()
    val accessToken = ClientCred.acquireToken(clientCredentialParam).get().accessToken()
    return accessToken
  } catch {
    case ex: Exception =>
      val currentTS = LocalDateTime.now().toString
      val errorMsg = s"⚠️ Exception occurred at $currentTS in getAPIAccessToken() function: $ex"
      throw new Exception(errorMsg, ex)
  }
}
%md ##### exitNotebook()
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
%md ##### bypass()
import scalaj.http.Http
import javax.net.ssl._
import java.security.cert.X509Certificate

def bypass() = {
  try {
      // Trust manager that bypasses both client and server validation.
      object TrustAll extends X509TrustManager {
        val getAcceptedIssuers = null
        def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String) = {}
        def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String) = {}
      }

      // Hostname verifier that bypasses verification by always returning true.
      object VerifiesAllHostNames extends HostnameVerifier {
        def verify(s: String, sslSession: SSLSession) = true
      }
      
      val sslContext = SSLContext.getInstance("TLSv1.2")
      sslContext.init(null, Array(TrustAll), new java.security.SecureRandom())
      HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory)
      HttpsURLConnection.setDefaultHostnameVerifier(VerifiesAllHostNames)
  } catch {
    case ex: Exception =>
      val currentTS = LocalDateTime.now().toString
      val errorMsg = s"⚠️ Exception occurred at $currentTS in bypass() function: $ex"
      throw new Exception(errorMsg, ex)
  }
}
%md ##### flattenDataframe()
import org.apache.spark.sql.types.{StructType, ArrayType}
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
  val cols = fieldNames.map(c => col(c.toString()).as(c.toString().substring(c.toString().lastIndexOf("_")+1)))
    df.select(cols:_*)
//   df
  }
%md ##### incrementalLoad()
import io.delta.tables._

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
%md ##### saveADLS()
import org.apache.spark.sql._

def saveADLS(tableDF_ : DataFrame, loadType : String, excludeColumns : String, TARGET_DATABASE : String, TARGET_TABLE : String, sourcePKCols : String) = {
  try {
    var tableDF = tableDF_
    
    loadType match{
      case "SNP" =>
      if (excludeColumns != null && excludeColumns.trim.nonEmpty) {
          tableDF = tableDF.drop(excludeColumns)
        }
        tableUtils.saveAsTable(
                              df = tableDF,  
                              fullTargetTableName = s"$TARGET_DATABASE.$TARGET_TABLE",
                              writeMode = "overwrite",  
                              debug = true,  
                              dryRun = false 
                              )
                              
      case "APPEND" =>
      if (excludeColumns != null && excludeColumns.trim.nonEmpty) {
          tableDF = tableDF.drop(excludeColumns)
        }
      tableDF.write.format("delta").mode("append").insertInto(TARGET_DATABASE+"."+TARGET_TABLE)

      case "INC" =>
      incrementalLoad(tableDF, sourcePKCols, TARGET_DATABASE, TARGET_TABLE) //needs DeltaTable => This is only insert and update NOT delete records 

      case _ =>
    }
  } catch {
    case ex: Exception =>
      val currentTS = LocalDateTime.now().toString
      val errorMsg = s"⚠️ Exception occurred at $currentTS in saveADLS() function: $ex"
      throw new Exception(errorMsg, ex)
  }
}
%md ##### getResp()
import scalaj.http._
import org.apache.spark.sql.functions._

def getResp(url:String, reqMethod:String , auth_type:String , auth_way:String, additionalParams:String, credentials:scala.collection.immutable.Map[String, String], contentType:String): HttpResponse[String]= {
  // Bypasses both client and server validation. 
  
  var t = scala.collection.Seq[(String, String)]()
//   var resp:HttpResponse[String]
  var req:HttpRequest = Http(url).timeout(connTimeoutMs = 10000, readTimeoutMs = 600000)
  
  if("QueryParam".equals(auth_way) && ("Bearer Token".equals(auth_type) || "API Key".equals(auth_type)|| "OAuth2.0".equals(auth_type)|| "Basic".equals(auth_type) || "No Auth".equals(auth_type))){
    println("Type is: "+auth_way)
    for ((k,v) <- credentials){req = req.param(k, v)}   
  }
  else if("header".equals(auth_way) && ("Bearer Token".equals(auth_type) || "API Key".equals(auth_type)|| "OAuth2.0".equals(auth_type)|| "Basic".equals(auth_type) || "No Auth".equals(auth_type))){
    println(auth_way+" Type is: "+auth_type)
    req = req.headers(credentials)   
  }
  else if("PostForm".equals(auth_way)){
    t = credentials.toSeq
    println("POST Form = " + t.getClass)    
  }
   if(additionalParams != null && !additionalParams.isEmpty() && additionalParams.contains(";")){
     println("Multiple ; "+additionalParams)
       for(s <- additionalParams.split(";")){
        req = req.param(s.split("=")(0), s.split("=")(1))
      }
    }else if(additionalParams != null && !additionalParams.isEmpty() && !additionalParams.contains(";") && !"null".equals(additionalParams)){
     println("One "+additionalParams)
     req = req.param(additionalParams.split("=")(0), additionalParams.split("=")(1))
   }
  req = req.header("Content-Type", contentType).header("Charset", "UTF-8")
  bypass()  
  println(reqMethod+contentType)
  reqMethod match{
    case "POST" =>
      return req.postForm(t).asString
    case "GET" =>
      return req.asString
    case _ =>
    return null
  }
}
%md ##### getAllPagesData()
import scala.collection.mutable.Map

var df: org.apache.spark.sql.DataFrame = _
var nurl = ""
var counter = 0

def getAllPagesData(url:String, reqMethod:String, auth_type:String, auth_way:String, additionalParams:String, credentials:Map[String, String], contentType:String, selectCols:String, paginationType:String, pageRuleName:String, pageRuleValue:String, excludeCols:String, tableDF: org.apache.spark.sql.DataFrame):DataFrame ={  
var response = getResp(url, reqMethod, auth_type, auth_way, additionalParams, credentials.toMap, contentType)
  if(response.isSuccess){
     println(response.statusLine)
    var json_string = response.body
  //Create Dataframe
  val dfd = spark.read.json(Seq(json_string).toDS).select(selectCols)//.select(explode(new Column(dt)).as(key)).select(cols:_*)
  df = tableDF.union(flattenDataframe(dfd))
  //Next Page (pagination) 
  paginationType match{
    case "HEADER" =>
      val id = response.header(pageRuleName).get
      if(!pageRuleValue.equals(id)){
        credentials += (pageRuleName->id)
        getAllPagesData(url, reqMethod, auth_type, auth_way, additionalParams, credentials, contentType, selectCols, paginationType, pageRuleName, pageRuleValue, excludeCols, df)
      }
    case "QueryParam" =>
      counter+=1
      val offset = flattenDataframe(spark.read.json(Seq(json_string).toDS).select(excludeCols)).head.get(0).toString()
      if(offset.toInt == counter*pageRuleValue.toInt){ 
        val v = "="+offset
        val params = additionalParams.replace(additionalParams.substring(45),v)
        getAllPagesData(url, reqMethod, auth_type, auth_way, params, credentials, contentType, selectCols, paginationType, pageRuleName, pageRuleValue, excludeCols, df)
      }
    case "AbsoluteURL" =>
      if(json_string.contains(pageRuleValue)){
        nurl = flattenDataframe(spark.read.json(Seq(json_string).toDS).select(excludeCols)).where(pageRuleName+"='"+pageRuleValue+"'").select("href").head.get(0).toString()
        getAllPagesData(nurl, reqMethod, auth_type, auth_way, additionalParams, credentials, contentType, selectCols, paginationType, pageRuleName, pageRuleValue, excludeCols, df)
      }
    case "RelativeURL" => 
    case _ =>
    } 
   df
  }

  else{
    println(response.statusLine)
    tableDF
  } 
}  
%md ##### buildCURL()
import sys.process._
def buildCURL(c:Seq[String], reqMethod:String, authType:String, authWay:String, credentials:Map[String, String], fileName:String, filePath:String, apiResponseType:String, p_data:String):String={
  var credential:Map[String, String] = Map.empty[String, String]
  var cmd:Seq[String] = c
  if(!"GET".equals(reqMethod))
    cmd = cmd++Seq("-X", reqMethod)

  // Authorization either Basic OR NTLM only pass as -u
  if("Authorization".equals(authWay)){
    val (user,pwd) = credentials.head
//     val cred = user+":"+pwd
    val cred = "bp1\\" + user + ":" + pwd
    
    println(cred)
    authType match{
      case "Basic" =>          
           cmd = cmd++Seq("-u", cred)      
      case "NTLM" =>    
//          cmd = cmd++Seq("--ntlm", "-u", cred)  
      cmd = cmd++Seq("-u", cred)  
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
    println(isCp + " IN : " + apiResponseType)
    if(isCp) dbutils.fs.rm("dbfs:/" + fileName)
    return String.valueOf(isCp)
  }
 return data
}
%md ##### getAllPagesTEXTData()
var nurl = ""
var text = ""

def getAllPagesTEXTData(url:String, reqMethod:String, auth_type:String, auth_way:String, additionalParams:String, credentials:Map[String, String], contentType:String, selectCols:String, paginationType:String, pageRuleName:String, pageRuleValue:String, excludeCols:String):String ={  
var response = getResp(url, reqMethod, auth_type, auth_way, additionalParams, credentials.toMap, contentType)
  if(response.isSuccess){
     println(response.statusLine)
    if(response.body.isEmpty()) return text
    if(text.isEmpty())    text += response.body
    //else  text +=  response.body.substring(245)
  //Next Page (pagination) 
  paginationType match{
    case "HEADER" =>
      val id = response.header(pageRuleName).get
      if(!pageRuleValue.equals(id)){
        credentials += (pageRuleName->id)
        getAllPagesTEXTData(url, reqMethod, auth_type, auth_way, additionalParams, credentials, contentType, selectCols, paginationType, pageRuleName, pageRuleValue, excludeCols)
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
%md ##### changeColumnSchema()
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{to_date, to_timestamp}

def changeColumnSchema(tableDF: org.apache.spark.sql.DataFrame, columnChangeSchema:String,sourceTimestampFormat:String):DataFrame = {
  
  try {
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
      var updatedDF = tableDF
      val newCol = columnChangeSchema.split(";")

      for(i <- 0 until newCol.length) {
        val cols = newCol(i).split(":")(0)
        val dtype: String = newCol(i).split(":")(1)
        if (newCol(i).split(":").length == 3) {
          val n_name = newCol(i).split(":")(2)
          updatedDF = updatedDF.withColumn(cols, updatedDF(cols).cast(dtype)).as(n_name)
        }
        if (newCol(i).split(":").length == 2) {
          if(dtype=="to_timestamp"){
            updatedDF = updatedDF.withColumn(cols,to_timestamp(col(cols),sourceTimestampFormat))
          }
          else {
            updatedDF = updatedDF.withColumn(cols, updatedDF(cols).cast(dtype))
          }
          
        }
      }
      updatedDF
  } catch {
    case ex: Exception =>
      val currentTS = LocalDateTime.now().toString
      val errorMsg = s"⚠️ Exception occurred at $currentTS in changeColumnSchema() function: $ex"
      throw new Exception(errorMsg, ex)
  }
}
%md ##### parseXMLtoDF()
def parseXMLtoDF(xmlData:String, sourcePKCols:String, attrs:Array[String], schema:org.apache.spark.sql.types.StructType): org.apache.spark.sql.DataFrame = {
  var rows:Seq[Row] = Seq[Row]();
  var param: Seq[String] = Seq[String]();
  var p_id:Seq[String] = Seq[String]();
  var p_name:Seq[String] = Seq[String]();
  var xmlNodes = sourcePKCols.split(";")
  var data = ""

  if(xmlData.contains("&lt;") || xmlData.contains("&gt;")) 
    data = xmlData.replace("&lt;", "<").replace("&gt;", ">")
   else data = xmlData
      val xml = scala.xml.XML.loadString(data)   
  println(xmlNodes(0) + "xml Nodes length: " + xmlNodes.size)
      var responseCode = (xml \ xmlNodes(0))
  if(sourcePKCols.contains("GetDRQueryResultDataTableXMLResponse")){
        responseCode = (responseCode \\ xmlNodes(2))   
  } else {
  //Iterate and reach to the Inner most node of Interest
      for(node <- xmlNodes.slice(1, xmlNodes.length)) {
        responseCode = (responseCode \ node)
      }
  }
      
  println("Response code length: " + responseCode.size)
  //From Inner most node extract all attributes
  responseCode.foreach(row => { 
    for(attr <- attrs){
      if(sourcePKCols.contains("GetDRQueryResultDataTableXMLResponse")) {
//       Code for tags table
        param = param :+ (row \ attr).text
      }else{        
        val value = "@"+attr.trim()  // @Action
        param = param :+ (row \ value).text
    }
      }
    
    rows = rows :+ Row.fromSeq(param)
    param = Seq[String]()
//     println(param.size)
  })
  println("Total: " + rows.size)
  val prow:RDD[Row] = sc.parallelize(rows)
  return spark.createDataFrame(prow, schema)
}
