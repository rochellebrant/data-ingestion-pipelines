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
%md # Imports
// Java Utilities and Concurrency
import java.util.{Properties, Collections}
import java.util.concurrent.Executors

// Java SQL Classes for Database Connections
import java.sql.{Connection, PreparedStatement, DriverManager}

// Microsoft Azure AD Authentication (MSAL)
import com.microsoft.aad.msal4j._

// Java Time for Date and Time Handling
import java.time.{Duration, LocalDateTime}

// Apache Spark SQL Classes and Functions
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, ArrayType}
import org.apache.spark.sql.functions.{col, explode_outer}

// Custom Utilities from bp.dwx Package
import bp.dwx.tableUtils._

// Scala Collections
import scala.collection.mutable._

import bp.dwx.tableUtils._
%md # SQL SERVER Methods
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

def exitNotebook(operation:String, jobID:String, pkTblJobQueue:Int, jobGroup:Int, jobNum:Int, jobOrder:Int, jobStepNum:Int, STATUS:String, processingDetails:String, errMsg:String, recInSource:Int, recIngested:Int, recProcessed:Int, recFailed:Int, jdbcUrl:String) = {
  var auditDBSPId = ""
  var auditDBSPPwd = ""
  auditDBSPId = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKey)
  auditDBSPPwd = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = ENV_AuditDBSPKeyPwd)
  println("auditDBSPId = " + auditDBSPId)
  println("auditDBSPPwd = " + auditDBSPPwd)
  var newauditDBAccessToken = getDBAccessToken(auditDBSPId, auditDBSPPwd)
  var connectionProperty = getJDBCConnectionProperty(newauditDBAccessToken, "36000")
  var connection = DriverManager.getConnection(jdbcUrl,connectionProperty)
  println("newauditDBAccessToken = " + newauditDBAccessToken)
  println("connectionProperty = " + connectionProperty)
  println("connection = " + connection)
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
%md ##### flattenDataframe()
def flattenDataframe(df: DataFrame, selectCols: String): DataFrame = {

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
          return flattenDataframe(explodedDf, selectCols)
        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName +"."+childname)
          val newfieldNames = fieldNames.filter(_!= fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))))
         val explodedf = df.select(renamedcols:_*)
          return flattenDataframe(explodedf, selectCols)
        case _ =>
      }
    }
  val cols = fieldNames.map(c => col(c.toString()).as(c.toString().substring(c.toString().indexOf("_")+1))) // removes prefix & underscore when flattening
  return df.select(cols:_*)
//   df
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
 
  def removingColumns(df: DataFrame, excludeColumns: String): DataFrame = {
    var df_ex = df
    val exclCol = excludeColumns.split(",")

    for(i <- 0 until exclCol.length) {
      df_ex = dropColumn(df_ex, exclCol(i))
    }
    df_ex
  }
%md ##### getResp()
import scalaj.http._
import org.apache.spark.sql.functions._

def getResp(url:String, reqMethod:String , auth_type:String , auth_way:String, additionalParams:String, credentials:scala.collection.immutable.Map[String, String], contentType:String) : HttpResponse[String]= {
  try {
    var t = scala.collection.Seq[(String, String)]()
    var req:HttpRequest = Http(url).timeout(connTimeoutMs = 1000000, readTimeoutMs = 600000)
    
    if("QueryParam".equals(auth_way) && ("Bearer Token".equals(auth_type) || "API Key".equals(auth_type)|| "OAuth2.0".equals(auth_type)|| "Basic".equals(auth_type) || "No Auth".equals(auth_type))){
  //     println("🔓 Auth method: " + auth_way + "\n🔑Auth type: " + auth_type)
      for ((k,v) <- credentials){req = req.param(k, v)}   
    }
    else if("header".equals(auth_way) && ("Bearer Token".equals(auth_type) || "API Key".equals(auth_type)|| "OAuth2.0".equals(auth_type)|| "Basic".equals(auth_type) || "No Auth".equals(auth_type))){
  //     println("🔓 Auth method: " + auth_way + "\n🔑 Auth type: " + auth_type)
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
    // println("📨 Request: " + reqMethod + "\n   Content type:" + contentType)
    
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
%md ##### changeColumnSchema()
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

def changeColumnSchema(tableDF: org.apache.spark.sql.DataFrame, transformedColumnsInTarget:String) : DataFrame = {
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
%md ##### saveADLS()
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
      if (spark.catalog.tableExists(s"$TARGET_DATABASE.$TARGET_TABLE")) {
        tableUtils.runDDL(s"DROP TABLE $TARGET_DATABASE.$TARGET_TABLE")
      }
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
      if (!spark.catalog.tableExists(s"$TARGET_DATABASE.$TARGET_TABLE")) {
        val errorMsg = s"⚠️ Cannot append data because table $TARGET_DATABASE.$TARGET_TABLE does not exist."
        throw new Exception(errorMsg)
      } else {
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
      }
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
  def addColumns(sourceFileDF: DataFrame, additionalColumnsInTarget: String): DataFrame = {

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
%md ### Functions Specific to this API
%md ##### getAPIAccessTokenEndTime()
def getAPIAccessTokenEndTime(AccessTokenDuration : Int) : (LocalDateTime) = {
  try {
    println("⌛ Re-setting access token timer...")
    val APIAccessTokenStartTime = LocalDateTime.now()
    val APIAccessTokenTimeDuration = Duration.ofMinutes(AccessTokenDuration)
    val APIAccessTokenEndTime = APIAccessTokenStartTime.plus(APIAccessTokenTimeDuration)
    return APIAccessTokenEndTime
  } catch {
    case ex: Exception =>
      val currentTS = LocalDateTime.now().toString
      val errorMsg = s"⚠️ Exception occurred at $currentTS in getAPIAccessTokenEndTime() function: $ex"
      throw new Exception(errorMsg, ex)
  }
}
%md ##### captureFailedAPI()
def captureFailedAPI(col1_data:String, col2_data:String, col1_name:String, col2_name:String, failuresDF:DataFrame) : DataFrame = {
  try {
    var df = failuresDF
    println("⚠️ SCHEMA MISMATCH ⚠️")
    var data = Seq((col1_data, col2_data))          // col1_data = c = well_log_set_curve_id         col2_data = f = curve_file_id
    var dfFromData = data.toDF().withColumnRenamed("_1",col1_name).withColumnRenamed("_2",col2_name)
    df = df.union(dfFromData)
    return df
  } catch {
    case ex: Exception =>
      val currentTS = LocalDateTime.now().toString
      val errorMsg = s"⚠️ Exception occurred at $currentTS in captureFailedAPI() function: $ex"
      throw new Exception(errorMsg, ex)
  }
}
%md ##### saveToTable()
/**
 * Saves a given DataFrame to a specified target table in either a snapshot (SNP) or incremental (INC) load type.
 * 
 * Depending on the `loadType` parameter, this function either appends or overwrites the table in the target database.
 * For "SNP" load type, it overwrites the target table on the first run and first counter, and appends thereafter.
 * For "INC" load type, it saves the DataFrame to a staging table with a similar overwrite-then-append logic.
 * 
 * It supports the exclusion of certain columns during the save operation and handles schema merging when appending data.
 * The function also handles any exceptions, logging relevant information and rethrowing the exception with a timestamp.
 * 
 * @param tableDF_        The DataFrame to be saved.
 * @param loadType        Specifies the load type: "SNP" for snapshot or "INC" for incremental.
 * @param excludeColumns  Comma-separated string of columns to be excluded (optional).
 * @param TARGET_DATABASE The name of the target database where the table resides.
 * @param TARGET_TABLE    The name of the target table to which the DataFrame should be saved.
 * @param counter         Specifies the iteration or batch number (used for conditional operations).
 * @param run             Specifies the run number, typically for controlling first-run behaviors.
 * 
 * @throws Exception If an error occurs during the table save process, it logs the error and throws an exception with the timestamp.
 */


def saveToTable(tableDF_ : DataFrame, loadType : String, excludeColumns : String, TARGET_DATABASE : String, TARGET_TABLE : String, counter : Int, run : Int) = {
  try {
    var tableDF = tableDF_
    var x:Long = 0
    for(col <- tableDF.columns){  tableDF = tableDF.withColumnRenamed(col,col.replaceAll("\\s", "_"))  }

    loadType match{
        case "SNP" =>
          // if table exists on run 1 & counter 1, delete
          if(spark.catalog.tableExists(TARGET_DATABASE+"."+TARGET_TABLE) && ((counter == 1) && (run == 1))){
            tableUtils.runDDL(s"DROP TABLE $TARGET_DATABASE.$TARGET_TABLE")
            }

          // if table exists -> append entry
          if(spark.catalog.tableExists(TARGET_DATABASE+"."+TARGET_TABLE)) {
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
          }

          // if table doesn't exist -> overwrite
          if(!(spark.catalog.tableExists(TARGET_DATABASE+"."+TARGET_TABLE))) {
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
          }
          var query = s"SELECT * FROM ${TARGET_DATABASE}.${TARGET_TABLE}"
          var x: Long = spark.sql(query).count()


        case "INC" =>
          // if table exists on run 1 & counter 1, delete
          if(spark.catalog.tableExists(TARGET_DATABASE+".STG_"+TARGET_TABLE) && ((counter == 1) && (run == 1))) {
            tableUtils.runDDL("DROP TABLE "+TARGET_DATABASE+".STG_"+TARGET_TABLE)
            }
          // if table exists -> append entry
          if(spark.catalog.tableExists(TARGET_DATABASE+".STG_"+TARGET_TABLE)) {
            tableUtils.saveAsTable( 
                              df = tableDF,  
                              fullTargetTableName = s"$TARGET_DATABASE.STG_$TARGET_TABLE",
                              writeMode = "append",  
                              debug = true,  
                              dryRun = false 
                              )
          }
          // if table doesn't exist -> overwrite
          if(!(spark.catalog.tableExists(TARGET_DATABASE+".STG_"+TARGET_TABLE))) {
            if (excludeColumns != null && excludeColumns.trim.nonEmpty) {
              tableDF = tableDF.drop(excludeColumns)
            }
            tableUtils.saveAsTable(
                                  df = tableDF,  
                                  fullTargetTableName = s"$TARGET_DATABASE.STG_$TARGET_TABLE",
                                  writeMode = "overwrite",  
                                  debug = true,  
                                  dryRun = false 
                                  )
          }
          var query = s"SELECT * FROM ${TARGET_DATABASE}.STG_${TARGET_TABLE}"
          var x: Long = spark.sql(query).count()

    
    }
    print("CUMULATIVE COUNT: " + x + "\n")
  } catch {
    case ex: Exception =>
      val currentTS = LocalDateTime.now().toString
      val errorMsg = s"⚠️ Exception occurred at $currentTS in saveToTable() function: $ex"
      throw new Exception(errorMsg, ex)
  }
}
%md ##### getCurveData()
/**
 * Fetches and processes curve data from an API, unions it with an existing DataFrame, and saves the data to a target table.
 * 
 * The function processes curve, file, and date identifiers in parallel, constructs API requests, and manages OAuth token renewal. NOTE: In the Tables notebook, the loadType parameter is modified at the start if it is INC load (it adds on sourceChgKeyLatestValues value).
 * Data fetched from the API is flattened, unioned with the target DataFrame, and saved to a specified table. If schema mismatches are detected, 
 * the function captures failed API requests in a separate DataFrame and saves the failure data for further review.
 * 
 * Additionally, if the API token is expired or about to expire (within a 25-minute window), the token is regenerated using stored credentials from a key vault.
 * This function also supports error handling during the token renewal process, attempting to retry token generation once in case of failure.
 * 
 * @param curveid_              Array of curve identifiers to fetch from the API.
 * @param fileid_               Array of file identifiers corresponding to each curve.
 * @param dateid_               Array of date identifiers corresponding to each curve and file.
 * @param col1, col2, col3      Column names for metadata in the fetched data (curve, file, date).
 * @param counter_              The current index or batch number for processing the data.
 * @param tokenURL              URL for retrieving API access tokens.
 * @param keyVaultName          Name of the key vault containing API credentials.
 * @param tokenClientId         Client ID for the API token authentication.
 * @param tokenClientCredential Client secret or credential for the API token authentication.
 * @param sourceURL             Base URL for the API endpoint to fetch data.
 * @param apiReqMethod          HTTP method to use for the API request (e.g., GET, POST).
 * @param apiAuthType           Type of API authentication (e.g., "Bearer", "Basic").
 * @param apiAuthWay            Method of authentication used in the API request (e.g., "Header").
 * @param additionalParams      Additional parameters to include in the API request.
 * @param apiReqContentType     Content type for the API request (e.g., "application/json").
 * @param selectCols            Columns to select after flattening the API response data.
 * @param loadType              Type of load operation: "SNP" for snapshot, "INC" for incremental.
 * @param excludeColumns        Comma-separated string of columns to be excluded during save.
 * @param targetDBName          Name of the target database where the table resides.
 * @param targetTblName         Name of the target table to save the fetched data.
 * @param FAILURES_TABLE_       Name of the table where failed API fetches are logged.
 * @param df1                   Schema of the empty DataFrame used for table resets.
 * @param tableDF_              DataFrame where API data is appended.
 * @param failuresDF_           DataFrame for capturing and storing failed API fetch attempts.
 * @param run_                  The current run number for the data fetching process.
 * @param t                     Current OAuth token for API requests.
 * @param APIAccessTokenEndTime_ Time when the current API access token expires.
 * @param AccessTokenDuration    Duration of the access token validity period, used for renewal.
 * 
 * @throws Exception If an error occurs during the data fetching or saving process, the function logs the error and rethrows it with a timestamp.
 */

import scala.concurrent.duration._

def getCurveData(curveid_ : Array[String], fileid_ : Array[String], dateid_ : Array[String], col1 : String, col2 : String, col3 : String, counter_ : Int, tokenURL : String, keyVaultName : String, tokenClientId : String, tokenClientCredential : String, sourceURL : String, apiReqMethod : String, apiAuthType : String, apiAuthWay : String, additionalParams : String, apiReqContentType : String, selectCols : String, loadType : String, excludeColumns : String, targetDBName : String, targetTblName : String, FAILURES_TABLE_ : String, df1 : DataFrame, tableDF_ : DataFrame, failuresDF_ : DataFrame, run_ : Int, t : String, APIAccessTokenEndTime_ : LocalDateTime, AccessTokenDuration : Int) = {
  try {  
    var curveid = curveid_
    var fileid = fileid_
    var dateid = dateid_
    var counter = counter_
    var tableDF = tableDF_
    var failuresDF = failuresDF_
    var FAILURES_TABLE = FAILURES_TABLE_
    var run = run_
    var t2 = t
    var APIAccessTokenEndTime = APIAccessTokenEndTime_
    
    println("\n════════════════════════════════")
    (curveid, fileid, dateid).zipped.foreach((c,f,d) => {
      println(s"Curve No.# $counter of ${curveid.length} (run $run)")

      // regenerate token if 25 min window has passed
      if(!(LocalDateTime.now().isBefore(APIAccessTokenEndTime))){
        println("___________________________________")
        APIAccessTokenEndTime = getAPIAccessTokenEndTime(AccessTokenDuration)
        try {
          t2 = getAPIAccessToken(tokenURL, dbutils.secrets.get(keyVaultName.toString(),tokenClientId),dbutils.secrets.get(keyVaultName.toString(),tokenClientCredential))
        }
        catch {
          case ex: Exception =>
          println("First getAPIAccessToken() attempt failed - retrying once | EXCEPTION: " + ex)
          Thread.sleep(60000)
          t2 = getAPIAccessToken(tokenURL, dbutils.secrets.get(keyVaultName.toString(),tokenClientId),dbutils.secrets.get(keyVaultName.toString(),tokenClientCredential))
          println("Second getAPIAccessToken() attempt succeeded ")
        }
      }
      
      var bearer_token2 = "Bearer " + t2
      var credentials2 = scala.collection.mutable.Map[String, String]()
      credentials2 += ("Authorization" -> bearer_token2)
      var url = sourceURL + c + "/file/" + f
      var resp = getResp(url, apiReqMethod, apiAuthType, apiAuthWay, additionalParams, credentials2.toMap, apiReqContentType)
      var json_string = resp.body
      var df_fetch_fl = flattenDataframe(spark.read.json(Seq(json_string).toDS), selectCols).withColumn(col1, lit(c.toString)).withColumn(col2, lit(f.toString)).withColumn(col3, lit(d.toString)).select("*")
      
      if(df_fetch_fl.schema == tableDF.schema){
        println("FETCHED COUNT: " + df_fetch_fl.count())
        tableDF = tableDF.union(df_fetch_fl)
        if(tableDF.count() > 0){
          saveToTable(tableDF, loadType, excludeColumns, targetDBName, targetTblName, counter, run)
          tableDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], df1.schema)
        }
      }

      if(df_fetch_fl.schema != tableDF.schema){ failuresDF = captureFailedAPI(c.toString, f.toString, col1, col2, failuresDF) }
      counter += 1
      println("═══════════════════════════")
      
    }) // foreach ends

    saveADLS(failuresDF, "SNP", "", targetDBName, FAILURES_TABLE, "", "")
    failuresDF = spark.emptyDataFrame
  } catch {
    case ex: Exception =>
      val currentTS = LocalDateTime.now().toString
      val errorMsg = s"⚠️ Exception occurred at $currentTS in getCurveData() function: $ex"
      throw new Exception(errorMsg, ex)
  }
}
