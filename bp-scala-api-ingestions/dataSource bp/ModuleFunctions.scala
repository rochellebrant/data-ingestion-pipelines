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

var ENV_sqlDWSPKey = "spid"
var ENV_sqlDWSPKeyPwd = "sppwd"
var ENV_DB_URL = "https://database.windows.net/"
%md # SQL SERVER Methods
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.time.{LocalDateTime, Duration, ZoneOffset}
import java.util.{Collections, Properties}

import com.microsoft.aad.msal4j._
import org.apache.spark.sql.DataFrame
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
def getJDBCConnectionProperty(accessToken: String, fetchSize: String): Properties = {
  try {
    val connectionProperties: Properties = new Properties()
    val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    connectionProperties.setProperty("Driver", driverClass)
    connectionProperties.put("fetchsize", fetchSize)
    connectionProperties.put("accessToken", accessToken)
    connectionProperties.put("hostNameInCertificate", "*.database.windows.net")
    connectionProperties.put("encrypt", "true")
    return connectionProperties;
  } catch {
    case ex: Exception =>
    val currentTS = LocalDateTime.now().toString
    val errorMsg = s"⚠️ Exception occurred at $currentTS in getJDBCConnectionProperty() function: $ex"
    throw new Exception(errorMsg, ex)
  }
}
%md # General Functions
%md ##### createExtTbl()
def createExtTbl(jdbcUrl : String, connectionProperty : Properties, datasource : String, db_name : String, tbl_name : String, extSchema : String, forceTblRecreate : String) = {
  try {
    val connection = DriverManager.getConnection(jdbcUrl, connectionProperty)
    var preparedStatement: PreparedStatement = null
    
    // Check external table exist or not
    val existQuery = "(SELECT count(*) as cnts FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '"+extSchema+"' AND TABLE_NAME = '"+tbl_name+"') as tab"
  //   val exist = sqlContext.read.sqlDB(sqlDBTable(ENV_SQLDWServer, ENV_SQLDW, existQuery, sqlAccessToken)).rdd.map(_(0)).collect.toList.head
    val exist = spark.read.jdbc(url=jdbcUrl, table=existQuery, properties=connectionProperty).rdd.map(_(0)).collect.toList.head

    // Get location of Database
    if (exist == 0 || "Y".equals(forceTblRecreate)) {
      val DBdf = spark.sql("DESCRIBE DATABASE " + db_name)
      val loc = DBdf.filter($"database_description_item" ==="Location").select("database_description_value")
      val location = loc.rdd.take(1)(0).getString(0)   
      println(location)
      //Get DDL of Database table
      val SQL = "SHOW CREATE TABLE " +db_name+ "."+ tbl_name
      val df = spark.sql(SQL)
      val string = df.rdd.take(1)
      // Replace specific Data type in DDL
      val newStr = string(0).getString(0).replace("`","").replace("\n","").replace(" STRING,"," NVARCHAR(MAX),").replace(" STRING)"," NVARCHAR(MAX))").replace(" DOUBLE"," FLOAT").replace("TIMESTAMP","DATETIME").replace("BOOLEAN","BIT").replace("References","Reference_s").replace("Current","Current_0").replace("CREATE TABLE", "CREATE EXTERNAL TABLE").replace(db_name, extSchema)

      val create_statement = newStr.replace("USING parquet", " WITH( LOCATION='"+location+ "/" + tbl_name.toLowerCase() + "', DATA_SOURCE="+datasource.replace("'","")+", FILE_FORMAT=Parquet);" ).replace("abfss://udl-container@zneudl1p48defstor01.dfs.core.windows.net","")
      val drop_statement="IF EXISTS ( SELECT * FROM sys.external_tables WHERE object_id = OBJECT_ID('"+ extSchema+ "."+ tbl_name + "') ) DROP EXTERNAL TABLE " + extSchema+ "."+ tbl_name +";"
      val final_statement = drop_statement + create_statement
      println(final_statement)
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

def exitNotebook(operation : String, jobID : String, pkTblJobQueue : Int, jobGroup : Int, jobNum : Int, jobOrder : Int, jobStepNum : Int, STATUS : String, processingDetails : String, errorMsg : String, recInSource : Int, recIngested : Int, recProcessed : Int, recFailed : Int, jdbcUrl : String, connectionProperty : Properties) = {
  
  val auditDBSPId_ = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKey)
  val auditDBSPPwd_ = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKeyPwd)
  val connectionProperty_ = getJDBCConnectionProperty(getDBAccessToken(auditDBSPId_, auditDBSPPwd_), "108000")

  val connection = DriverManager.getConnection(jdbcUrl, connectionProperty_)

  // Update runLog table
  val query =
    s"""
       |EXEC [audit].[SP_LOG_RUN_DETAILS_NEW]
       | @operation = N'$operation',
       | @runID = N'$jobID',
       | @fkJobQueue = $pkTblJobQueue,
       | @jobGroup = $jobGroup,
       | @jobNum = $jobNum,
       | @jobOrder = $jobOrder,
       | @jobStepNum = $jobStepNum,
       | @status = N'$STATUS',
       | @processingDetails = N'$processingDetails',
       | @errorMsg = N'$errorMsg',
       | @recInSource = $recInSource,
       | @recIngested = $recIngested,
       | @recProcessed = $recProcessed,
       | @recFailed = $recFailed
       |""".stripMargin.trim

  val preparedStatement = connection.prepareStatement(query)
  preparedStatement.executeUpdate()

  // Send JSON string of info back to parent notebook
  val jsonMapper = new ObjectMapper with ScalaObjectMapper
  jsonMapper.registerModule(DefaultScalaModule)

  val resultMap = Map(
    "runID" -> jobID,
    "fkJobQueue" -> pkTblJobQueue,
    "jobGroup" -> jobGroup,
    "jobOrder" -> jobOrder,
    "jobNum" -> jobNum,
    "jobStepNum" -> jobStepNum,
    "errorMsg" -> errorMsg,
    "STATUS" -> STATUS
  )

  dbutils.notebook.exit(jsonMapper.writeValueAsString(resultMap))
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
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.{col, explode_outer}
import org.apache.spark.sql.types.{ArrayType, StructType}

def flattenDataframe(df : DataFrame): DataFrame = {

  try {
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
  // val cols = fieldNames.map(c => col(c.toString()).as(c.toString().substring(c.toString().indexOf("_")+1)))
  // df.select(cols:_*)
  df
  } catch {
    case ex: Exception =>
      val currentTS = LocalDateTime.now().toString
      val errorMsg = s"⚠️ Exception occurred at $currentTS in flattenDataframe() function: $ex"
      throw new Exception(errorMsg, ex)
  }
}
%md ##### removePrefix()
def removePrefix(df : DataFrame, sourceChangeKeyCols : String): DataFrame = {
  try {
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    val cols = fieldNames.map(c => col(c.toString()).as(c.toString().substring(sourceChangeKeyCols.length+1, c.toString().length()))) // removes sourceChangeKeyCols prefix & following underscore only
    return df.select(cols : _*)
  } catch {
      case ex: Exception =>
        val currentTS = LocalDateTime.now().toString
        val errorMsg = s"⚠️ Exception occurred at $currentTS in removePrefix() function: $ex"
        throw new Exception(errorMsg, ex)
    }
  }
%md ##### removingColumns() , dropColumn() , dropSubColumn()
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.sql.functions.{col, struct, arrays_zip}

/**
 * Recursively drops a nested sub-column from a DataFrame's schema, handling complex structures like structs and arrays.
 *
 * This function is particularly useful when working with deeply nested columns, allowing you to drop a specific sub-column.
 * It supports both `StructType` and `ArrayType` and ensures that any nested columns are handled properly, even for complex 
 * structures inside arrays (a workaround for a known Spark behavior).
 *
 * @param col           The column object from the DataFrame.
 * @param colType       The DataType of the column (e.g., StructType, ArrayType, etc.).
 * @param fullColName   The full name of the current column being processed.
 * @param dropColName   The full name of the sub-column that needs to be dropped.
 * @return              An `Option[Column]` where `Some(Column)` indicates the column with the drop applied, or `None` if the column should be completely dropped.
 */

def dropSubColumn(col : Column, colType : DataType, fullColName : String, dropColName : String) : Option[Column] = {
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
/**
 * Drops a specified column from a DataFrame, including handling for nested columns within structs and arrays.
 * 
 * This function allows for dropping columns from within complex, nested data structures in a DataFrame schema. If the 
 * specified column exists inside a struct or array, the function will apply the drop recursively using the `dropSubColumn` method.
 * 
 * @param df        The DataFrame from which the column should be dropped.
 * @param colName   The name of the column to be dropped. Can include nested column names, separated by dots (e.g., "parent.child").
 * @return          A new DataFrame with the specified column dropped.
 */
 
def dropColumn(df : DataFrame, colName : String): DataFrame = {
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
/**
 * Removes one or more columns from a DataFrame based on a comma-separated list of column names.
 * 
 * This function iterates over a list of columns provided in the `excludeColumns` parameter and drops each column from the
 * DataFrame. It uses the `dropColumn` function to handle cases where columns are nested inside structs or arrays.
 * 
 * @param df              The DataFrame from which columns should be removed.
 * @param excludeColumns  A comma-separated string of column names to be excluded from the DataFrame.
 * @return                A new DataFrame with the specified columns removed.
 */
 
  def removingColumns(df : DataFrame, excludeColumns : String) : DataFrame = {
    var df_ex = df
    val exclCol = excludeColumns.split(",")

    for(i <- 0 until exclCol.length) {
      df_ex = dropColumn(df_ex, exclCol(i))
    }
    df_ex
  }
%md ##### incrementalLoad()
import io.delta.tables._
import org.apache.spark.sql.DataFrame

var cond = ""

def incrementalLoad(newTableDF : DataFrame, sourcePKCols : String, TARGET_DATABASE : String, SOURCE_TB : String) : Unit = {

  try {
    val target = DeltaTable.forName(s"STG_$TARGET_DATABASE.$SOURCE_TB")

    // Constructing the condition string based on primary key columns
    if (sourcePKCols != null && sourcePKCols.contains(";")) {
      // sourcePKCols.split(";").foreach(cond = cond + "target."+ _+" = source." +_)
      val PKcols = sourcePKCols.split(";")
      for(i <- 0 until PKcols.length) {
        cond = cond + " target."+ PKcols(i)+" = source." +PKcols(i)
        if(i != PKcols.length-1) cond += " AND"
      }
    } else {
      cond =  "target."+ sourcePKCols+" = source." +sourcePKCols
    }
    
    newTableDF.printSchema()
    println(cond)

    target.alias("target").merge(newTableDF.alias("source"), cond).whenMatched.updateAll.whenNotMatched.insertAll.execute()
  } catch {
    case ex: Exception =>
    val currentTS = LocalDateTime.now().toString
    val errorMsg = s"⚠️ Exception occurred at $currentTS in incrementalLoad() function: $ex"
    throw new Exception(errorMsg, ex)
  }
}
%md ##### getResp()
import scalaj.http._
import org.apache.spark.sql.functions._

def getResp(url : String, reqMethod : String , auth_type : String , auth_way : String, additionalParams : String, credentials : scala.collection.immutable.Map[String, String], contentType : String) : HttpResponse[String] = {
  try {
    var t = scala.collection.Seq[(String, String)]()
    var req: HttpRequest = Http(url).timeout(connTimeoutMs = 4000000, readTimeoutMs = 600000)
    
    if ("QueryParam".equals(auth_way) && ("Bearer Token".equals(auth_type) || "API Key".equals(auth_type)|| "OAuth2.0".equals(auth_type)|| "Basic".equals(auth_type) || "No Auth".equals(auth_type))) {
      for ((k,v) <- credentials){req = req.param(k, v)}   
    } else if ("header".equals(auth_way) && ("Bearer Token".equals(auth_type) || "API Key".equals(auth_type)|| "OAuth2.0".equals(auth_type)|| "Basic".equals(auth_type) || "No Auth".equals(auth_type))) {
      req = req.headers(credentials)   
    } else if ("PostForm".equals(auth_way)) {
      t = credentials.toSeq
      println("POST Form = " + t.getClass)    
    }
    
    if (additionalParams != null && !additionalParams.isEmpty() && additionalParams.contains(";")) {
      println("Multiple parameters -> " + additionalParams)
        for(s <- additionalParams.split(";")){
          req = req.param(s.split("=")(0), s.split("=")(1))
        }
    } else if (additionalParams != null && !additionalParams.isEmpty() && !additionalParams.contains(";") && !"null".equals(additionalParams)) {
      println("One parameter -> " + additionalParams)
      req = req.param(additionalParams.split("=")(0), additionalParams.split("=")(1))
      }
    
    req = req.header("Content-Type", contentType).header("Charset", "UTF-8")
    bypass()
    
    reqMethod match{
      case "POST" =>
        return req.postForm(t).asString
      case "GET" =>
        return req.asString
      case _ =>
      return null
    }
  } catch {
    case ex: Exception =>
      val currentTS = LocalDateTime.now().toString
      val errorMsg = s"⚠️ Exception occurred at $currentTS in getResp() function: $ex"
      throw new Exception(errorMsg, ex)
  }
}
%md ##### shouldRegenerateToken()
/**
 * Checks if the token needs to be regenerated based on the start time and the current time.
 * 
 * This function compares the time difference between the current time and the provided start time.
 * If the difference exceeds 50 minutes, the function returns true, indicating that the token needs to be regenerated.
 * 
 * @param tokenStartTime The start time of the token in epoch seconds (UTC).
 * @return Boolean indicating whether the token needs to be regenerated.
 */
 
def shouldRegenerateToken(tokenStartTime : Long): Boolean = {
  try {
    val currentTime = LocalDateTime.now.toEpochSecond(ZoneOffset.UTC)
    val elapsedTime = currentTime - tokenStartTime
    elapsedTime > (50 * 60) // 50 minutes
    } catch {
    case ex: Exception =>
    val currentTS = LocalDateTime.now().toString
    val errorMsg = s"⚠️ Exception occurred at $currentTS in shouldRegenerateToken() function: $ex"
    throw new Exception(errorMsg, ex)
  }
}
%md ##### getAllPagesData()
import scalaj.http._
import org.apache.spark.sql.functions._
import scala.collection.mutable.Map
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.json4s._
import org.json4s.jackson.JsonMethods._

var df_fetch: org.apache.spark.sql.DataFrame = _
var df_fetch_fl: org.apache.spark.sql.DataFrame = _
var df: org.apache.spark.sql.DataFrame = _ 
var nurl = ""
var counter = 0
var params = ""

def getAllPagesData(url : String, reqMethod : String, auth_type : String, auth_way : String, additionalParams : String, credentials : Map[String, String], contentType : String, selectCols : String, paginationType : String, paginationURLKeyword : String, paginationURLLocation : String, excludeCols : String, tableDF : org.apache.spark.sql.DataFrame, generateToken: () => Map[String,String] = () => Map()) : DataFrame = {

  try {
    val tokenGenerateTime = credentials.getOrElse("tokenGenerateTime", "1").toLong

    if (shouldRegenerateToken(tokenGenerateTime)) {
      val newCredentials = generateToken()
      credentials += ("Authorization" -> newCredentials.getOrElse("Authorization", ""))
      credentials += ("tokenGenerateTime" -> LocalDateTime.now.toEpochSecond(ZoneOffset.UTC).toString)
    }

    var response = getResp(url, reqMethod, auth_type, auth_way, additionalParams, credentials.toMap, contentType)

    if (response.isSuccess) {
      var json_string = response.body

      if (json_string != "[]") {
        df_fetch = spark.read.json(Seq(json_string).toDS)

        if (!(df_fetch.isEmpty && df_fetch.head(1).isEmpty)) {
          df_fetch_fl = flattenDataframe(df_fetch.select(selectCols))
          println(s"\nFETCHED      ${df_fetch_fl.count()}")
          println(s"EXISTING     ${tableDF.count()}")
          df = tableDF.union(df_fetch_fl)
          println(s"CUMULATIVE   ${df.count()}\n")
        }
    } else {
        return df
      }
        
    paginationType match {
      case "HEADER" =>
        val id = response.header(paginationURLKeyword).get
        if (!paginationURLLocation.equals(id)) {
          credentials += (paginationURLKeyword -> id)
          getAllPagesData(url, reqMethod, auth_type, auth_way, additionalParams, credentials, contentType, selectCols, paginationType, paginationURLKeyword, paginationURLLocation, excludeCols, df)
        }
      case "QueryParam" =>
        if (df_fetch.count() == paginationURLLocation.toInt) {
          counter += 1
          val arr = additionalParams.split(";")
          for (x <- 0 to arr.length-1)  {
            if (x==arr.length-1) arr(x) = arr(x).replace(additionalParams.substring(additionalParams.lastIndexOf("=")),"="+counter*paginationURLLocation.toInt)
          }
          params = arr.mkString(";")
          println(params)
          getAllPagesData(url, reqMethod, auth_type, auth_way, params, credentials, contentType, selectCols, paginationType, paginationURLKeyword, paginationURLLocation, excludeCols, df)
        }
      case "AbsoluteURL" =>
        if (json_string.contains(paginationURLLocation)) {
          nurl = df_fetch.toDF("col_1", "col_2", "col_3").select("col_2").head.get(0).toString()
          getAllPagesData(nurl, reqMethod, auth_type, auth_way, additionalParams, credentials, contentType, selectCols, paginationType, paginationURLKeyword, paginationURLLocation, excludeCols, df, generateToken)
        }
      case "RelativeURL" => 
      case _ =>
      } 
    df
    } else {
      println(s"⚠️ No response - ${response.statusLine}")
      tableDF
    }
  } catch {
    case ex: Exception =>
      val currentTS = LocalDateTime.now().toString
      val errorMsg = s"⚠️ Exception occurred at $currentTS in getAllPagesData() function: $ex"
      throw new Exception(errorMsg, ex)
  }
}  
%md ##### buildCURL()
import sys.process._

def buildCURL(c : Seq[String], reqMethod : String, authType : String, authWay : String, credentials : Map[String, String], fileName : String, filePath : String, apiResponseType : String, p_data : String) : String = {
  
  var credential: Map[String, String] = Map.empty[String, String]
  var cmd: Seq[String] = c

  if (!"GET".equals(reqMethod))
    cmd = cmd++Seq("-X", reqMethod)

  // Authorization either Basic OR NTLM only pass as -u
  if ("Authorization".equals(authWay)) {
    val (user, pwd) = credentials.head
    val cred = user + ":" + pwd
    authType match {
      case "Basic" =>          
        cmd = cmd++Seq("-u", cred)      
      case "NTLM" =>    
        cmd = cmd++Seq("--ntlm", "-u", cred)      
      case _ =>
    }
  }

  if("Authorization".equals(authWay) && credentials.size > 1) {
    credential = credentials.tail
  } else if("Header".equals(authWay)) {
    credential = credentials    
  }

  for ((k,v)<-credential) {
    val head = k + ": " + v
    cmd = cmd++Seq("-H", head)
    }

  if (("POST".equals(reqMethod)) && p_data != null) {
    cmd = cmd ++ Seq("-d", p_data)
  }

  println(filePath)
  // If API response has attachment, download options of CURL command
  if("Attachment".equals(apiResponseType))
    cmd = cmd++Seq( "-o", "/dbfs/"+fileName)
    
  cmd.foreach(println(_))
  // Run Curl command
  val data = cmd.!!
  if ("Attachment".equals(apiResponseType) && filePath != null && fileName != null) {   
    val strg_key = dbutils.secrets.get(ENV_KeyVaultScope, ENV_storageACKeySecretName)
    spark.conf.set("fs.azure.account.key." + ENV_storagACName, strg_key)
    val isCp = dbutils.fs.cp("dbfs:/" + fileName, filePath + fileName)
    println(isCp + " IN : " + apiResponseType)
    if(isCp) dbutils.fs.rm("dbfs:/" + fileName)
    return String.valueOf(isCp)
  }
 return data
}
%md ##### getAllPagesTEXTData()
import scalaj.http._
import org.apache.spark.sql.functions._
import scala.collection.mutable.Map
import org.apache.spark.sql._

var nurl = ""
var text = ""

def getAllPagesTEXTData(url : String, reqMethod : String, auth_type : String, auth_way : String, additionalParams : String, credentials : Map[String, String], contentType : String, selectCols : String, paginationType : String, paginationURLKeyword : String, paginationURLLocation : String, excludeCols : String) : String = {

var response = getResp(url, reqMethod, auth_type, auth_way, additionalParams, credentials.toMap, contentType)

  if (response.isSuccess) {
     println(response.statusLine)

    if(response.body.isEmpty()) return text

    if(text.isEmpty())    text += response.body
    
    //else  text +=  response.body.substring(245)
  //Next Page (pagination) 
  paginationType match {
    case "HEADER" =>
    val id = response.header(paginationURLKeyword).get
    if(!paginationURLLocation.equals(id)){
      credentials += (paginationURLKeyword->id)
      getAllPagesTEXTData(url, reqMethod, auth_type, auth_way, additionalParams, credentials, contentType, selectCols, paginationType, paginationURLKeyword, paginationURLLocation,excludeCols)
    }
    case "QueryParam" =>
    case "AbsoluteURL" =>
    case "RelativeURL" =>
    case _ =>
    }
   return text
  } else {
    println(response.statusLine)
    return ""
  } 
}
%md ##### changeColumnSchema()
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

def changeColumnSchema(tableDF : org.apache.spark.sql.DataFrame, transformedColumnsInTarget : String) : DataFrame = {
  try {
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
    return updatedDF
  } catch {
    case ex: Exception =>
      val currentTS = LocalDateTime.now().toString
      val errorMsg = s"⚠️ Exception occurred at $currentTS in changeColumnSchema() function: $ex"
      throw new Exception(errorMsg, ex)
  }
}
%md ##### parseXMLtoDF()
def parseXMLtoDF(xmlData : String, sourcePKCols : String, attrs : Array[String], schema : org.apache.spark.sql.types.StructType) : org.apache.spark.sql.DataFrame = {

  var rows: Seq[Row] = Seq[Row]();
  var param: Seq[String] = Seq[String]();
  var xmlNodes = sourcePKCols.split(";")
  var data = ""
  if(xmlData.contains("&lt;") || xmlData.contains("&gt;"))
    data = xmlData.replace("&lt;", "<").replace("&gt;", ">")
   else data = xmlData
      val xml = scala.xml.XML.loadString(data)
  println(xmlNodes(0) + "xml Nodes length: " + xmlNodes.size)
  var responseCode = (xml \ xmlNodes(0))
  //Iterate and reach to the Inner most node of Interest
  for (node <- xmlNodes.slice(1, xmlNodes.length)) {
    responseCode =(responseCode \ node)
  }
  println("Resonsecode Length: " + responseCode.size)
  //From Inner most node extract all attributes
  responseCode.foreach(row => {
    for (attr <- attrs) {
      val value = "@"+attr.trim()  // @Action
      param = param :+ (row \ value).text
    }
    rows = rows :+ Row.fromSeq(param)
    param = Seq[String]()
  })
  println("Total: " + rows.size)
  val prow:RDD[Row] = sc.parallelize(rows)
  return spark.createDataFrame(prow, schema)
}
%md ##### saveADLS()
import org.apache.spark.sql._
import bp.dwx.tableUtils._

def saveADLS(tableDF_ : DataFrame, loadType : String, excludeColumns : String, TARGET_DATABASE : String, TARGET_TABLE : String, sourcePKCols : String, deleteTable : String) = {
  try {
    println(s">>> Uploading to ADLS...")
    var new_data: org.apache.spark.sql.DataFrame = null
    var tableDF = tableDF_

    println(
    f"""|\tLoad type:     ➜ $loadType
        |\tExcl. columns  ➜ $excludeColumns
        |\tTable          ➜ ${TARGET_DATABASE.toLowerCase()}.${TARGET_TABLE.toLowerCase()}
        |\tNo. of rows    ➜ ${tableDF.count()}""".stripMargin
    )

    loadType match {
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
      tableUtils.saveAsTable( 
                          df = tableDF,  
                          fullTargetTableName = s"$TARGET_DATABASE.$TARGET_TABLE",
                          writeMode = "append",  
                          debug = true,  
                          dryRun = false 
                          )

      case _ =>
    }
    
    println(s">>> Upload to ADLS successful")

  } catch {
    case ex: Exception =>
      val currentTS = LocalDateTime.now().toString
      val errorMsg = s"⚠️ Exception occurred at $currentTS in saveADLS() function: $ex"
      throw new Exception(errorMsg, ex)
  }
}
%md ##### addColumns()
  def addColumns(sourceFileDF : DataFrame, additionalColumnsInTarget : String) : DataFrame = {

    println(s">>> Adding ${additionalColumnsInTarget.split(",").length} columns...")

    var cleanDF = sourceFileDF
    val addNewColumn: (String) => String = (data: String) => {data}
    val ColUDF = udf(addNewColumn)

    try {
      val columnsArray = additionalColumnsInTarget.split(",")

      // Add columns based on their names
      columnsArray.foreach {
        case "load_ts" =>
          cleanDF = cleanDF.withColumn("load_ts", current_timestamp())
        case _ =>
      }
    } catch {
        case ex: Exception =>
          val currentTS = LocalDateTime.now().toString
          val errorMsg = s"⚠️ Exception occurred at $currentTS in addColumns() function: $ex"
          throw new Exception(errorMsg, ex)
      }
    return cleanDF
  }
