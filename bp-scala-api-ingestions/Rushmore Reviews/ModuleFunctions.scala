%md # Connections
val SQLServerSuffix = ".database.windows.net"
val StorageSuffix = ".core.windows.net"

case class AuditDBConfig(server: String, database: String, spKey: String, spKeyPwd: String)
case class SQLDWConfig(server: String, database: String)
case class StorageConfig(accountName: String, keySecretName: String)

val ENV_TenantId = "ea80952e-a476-42d4-aaf4-5457852b0f7e"
val ENV_SubscriptionID = "f55d4ef9-4d7f-4763-8661-9b82de6c08c9"
val ENV_ResourceGroup = "ZNE-UDL1-P-40-ING-RSG"
val ENV_KeyVaultScope = "ZNEUDL1P40INGing-kvl00"

val auditDBConfig = AuditDBConfig(
  server = s"zneudl1p12sqlsrv002$SQLServerSuffix",
  database = "zneudl1p12sqldb001",
  spKey = "spid-BI-zne-udl1-p-12-udl-rsg",
  spKeyPwd = "sppwd-BI-zne-udl1-p-12-udl-rsg"
) 

val sqlDWConfig = SQLDWConfig(
  server = s"zneudl1p33dessql01$SQLServerSuffix",
  database = "zneudl1p33desdwsqldb01"
)

val storageConfig = StorageConfig(
  accountName = s"zneudl1p33lakesstor$StorageSuffix",
  keySecretName = "AccKey-zneudl1p33lakesstor"
)

val JDBC_url = s"jdbc:sqlserver://${auditDBConfig.server}:1433;database=${auditDBConfig.database}"
%md # SQL SERVER Methods
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.time.LocalDateTime
import java.util.{Collections, Properties}
import java.util.concurrent.Executors

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
def printSeparator(char: String = "═", length: Int = 80): String = {
  return char * length
}
case class property(Message: String, Subject:String, EmailTo: String )
%md ##### sendMail()
// @throws(classOf[SQLException])
def createExtTbl(jdbcUrl:String, connectionProperty:Properties, datasource : String, db_name : String, tbl_name : String, extSchema: String, forceTblRecreate:String) = {
// try{
  val connection = DriverManager.getConnection(jdbcUrl,connectionProperty)
  var preparedStatement:PreparedStatement = null
  
  //Check external table exist or not
  val existQuery = "(SELECT count(*) as cnts FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '"+extSchema+"' AND TABLE_NAME = '"+tbl_name+"') as tab"
//   val exist = sqlContext.read.sqlDB(sqlDBTable(sqlDWConfig.server, sqlDWConfig.database, existQuery, sqlAccessToken)).rdd.map(_(0)).collect.toList.head
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
//     sqlContext.sqlDBQuery(sqlDBquery(sqlDWConfig.server, sqlDWConfig.database, final_statement, sqlAccessToken))
    preparedStatement = connection.prepareStatement(final_statement)
  preparedStatement.executeUpdate()
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
  auditDBSPId = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = auditDBConfig.spKey)
  auditDBSPPwd = dbutils.secrets.get(scope = ENV_KeyVaultScope, key = auditDBConfig.spKeyPwd)
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
//     sqlContext.sqlDBQuery( sqlDBquery(auditDBConfig.server, auditDBConfig.database, query, auditDBAccessToken))
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
import org.apache.spark.sql.types.{StructType, ArrayType, StringType}
import org.apache.spark.sql.functions.{col, explode_outer}

def flattenDataframe(df : DataFrame, TARGET_TABLE : String): DataFrame = {
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    val length = fields.length  
    
    for(i <- 0 to fields.length-1){
      val field = fields(i) // output ---> StructField(Data,StructType(StructField(AgeOfDeepestReservoir,StringType,true), StructField(BatchCampaignDrilled,StringType,true),
      val fieldtype = field.dataType // output ---> StructType(StructField(AgeOfDeepestReservoir,StringType,true), StructField(BatchCampaignDrilled,StringType,true), StructField(BurialDepth,DoubleType,true),
      val fieldName = field.name // output ---> Data_ExternalProblemsNPT

      fieldtype match {
        case arrayType: ArrayType =>
          val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
          val explodedDf = df.selectExpr(fieldNamesAndExplode:_*)
          return flattenDataframe(explodedDf, TARGET_TABLE)
          
        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName +"."+childname)
          val newfieldNames = fieldNames.filter(_!= fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))))
         val explodedf = df.select(renamedcols:_*)
          return flattenDataframe(explodedf, TARGET_TABLE)

        case _ => // Do nothing for other data types
      }
    }
    
 TARGET_TABLE match {
   case "well_cpr" | "well_dpr" =>
     val df_str = df.select(df.columns.map(c => col(c).cast(StringType)) : _*) //changes all columns to StringType due to data type mismatch which disallowed table & dataframe union later on in Concurrent
    // val cols = fieldNames.map(c => col(c.toString()).as(c.toString().substring(c.toString().indexOf("_")+1))) // removes all parents & underscores
     val cols = fieldNames.map(c => col(c.toString()).as(c.toString().substring(5, c.toString().length()))) // removes "Data_" only
     return df_str.select(cols : _*)
   
   case "time_depth" =>
     val cols = fieldNames.map(c => col(c.toString()).as(c.toString().substring(c.toString().indexOf("_")+1))) // removes ALL parents & underscores
     return df.select(cols : _*)
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
%md ##### incrementalLoad()
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
%md ##### getResp()
import scalaj.http._
import org.apache.spark.sql.functions._

def getResp(url:String, reqMethod:String , auth_type:String , auth_way:String, additionalParams:String, credentials:scala.collection.immutable.Map[String, String], contentType:String): HttpResponse[String]= {
  
  println(">>> Querying API...")
  var t = scala.collection.Seq[(String, String)]()
  var req:HttpRequest = Http(url).timeout(connTimeoutMs = 15000, readTimeoutMs = 600000)
  
  if("QueryParam".equals(auth_way) && ("Bearer Token".equals(auth_type) || "API Key".equals(auth_type)|| "OAuth2.0".equals(auth_type)|| "Basic".equals(auth_type) || "No Auth".equals(auth_type))) {
    println("\t•" + auth_type + " authentication via " + auth_way)
    for ((k,v) <- credentials){req = req.param(k, v)}   
  } else if("header".equals(auth_way) && ("Bearer Token".equals(auth_type) || "API Key".equals(auth_type)|| "OAuth2.0".equals(auth_type)|| "Basic".equals(auth_type) || "No Auth".equals(auth_type))) {
    println("\t• " + auth_type + " authentication via " + auth_way)
    req = req.headers(credentials)   
  } else if("PostForm".equals(auth_way)) {
    t = credentials.toSeq
    println("\t• POST Form = " + t.getClass)    
  }
  
  
  if(additionalParams != null && !additionalParams.isEmpty() && additionalParams.contains(";")){
    println("\t• Multiple parameters: " + additionalParams)
      for(s <- additionalParams.split(";")){
      req = req.param(s.split("=")(0), s.split("=")(1))
    }
  } else if(additionalParams != null && !additionalParams.isEmpty() && !additionalParams.contains(";") && !"null".equals(additionalParams)) {
     println("\t• One parameter: " + additionalParams)
     req = req.param(additionalParams.split("=")(0), additionalParams.split("=")(1))
   }
  
  req = req.header("Content-Type", contentType).header("Charset", "UTF-8")
  bypass()  
  println("\t• " + reqMethod + " request for " + contentType)
  reqMethod match {
    case "POST" =>
      return req.postForm(t).asString
    case "GET" =>
      return req.asString
    case _ =>
      return null
  }
}
%md ##### getTimeDepth()
- The time_depth data comes from the dpr API end-point.
- The time_depth table is to include the *WellID, Day, Depth, & HoleSize* columns. *WellID* is a main branch (Data.WellID), while the other 3 are sub-branches (Data.TimeDepth.Day & Data.TimeDepth.Depth & Data.TimeDepth.HoleSize).
- A cross-join occurred when we applied **flattenDataframe()** because it flattens all the columns *the same number of times*, but the *WellID* only needs exploding ONCE while the *Day, Depth, & HoleSize* columns need exploding TWICE followed by a ONE flattening.
- We have defined a function **getTimeDepth()** which takes in 1 argument (the dataframe), and is used instead of **flattenDataframe()**.
###### How the function works:
- Since *WellID* column need exploding ONCE, and *Day, Depth, & HoleSize* columns need exploding TWICE followed by ONE flattening, we split these into 2 steps to be done in 2 separate dataframes.
- The separate dataframes are given a monotonically increasing id number so that they can be re-unified based on the id numbers AFTER the correct number of explodings & flattenings are done.
// unique to Rushmore Reviews API ingestion
import org.apache.spark.sql.types.{StructType,ArrayType,StringType} // StringType added by rochelle
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode_outer
import scala.collection.mutable._
import org.apache.spark.sql.functions._
import spark.sqlContext.implicits._

// each "WellId" has a "TimeDepth" value -> "TimeDepth" is a list of dictionaries with keys "Day","Depth","HoleSize" -> need to explode "TimeDepth" independently of "WellId" otherwise cross-join occurs, giving incorrect data -> index columns are needed in the 2 separate dfs to do a final inner-join

def getTimeDepth(df:DataFrame, TARGET_TABLE:String): DataFrame = {
    var welliddf = df.withColumn("WellId",explode(col("Data.WellId"))).drop("Data").withColumn("id",monotonically_increasing_id()) // df with "WellId" & index "id" column -> 1 WellId & index id per row
    var tddf = df.withColumn("ExplodedTimeDepth",explode(col("Data.TimeDepth"))).drop("Data").withColumn("id2",monotonically_increasing_id()) // df with "ExplodedTimeDepth" & index "id2" column -> 1 list of dicts ("ExplodedTimeDepth") & index id2 per row
    var tddf_ex = tddf.withColumn("x",explode(col("ExplodedTimeDepth"))).drop("ExplodedTimeDepth") //exploding "ExplodedTimeDepth" column also explodes index "id2" column -> 1 dict per row with equivalent exploded index "id2"
    var tddf_flat = flattenDataframe(tddf_ex, TARGET_TABLE) // flatten dictionaries while maintaining equivalent index "id2"
    var tableDF = welliddf.join(tddf_flat,col("id") === col("id2"),"inner").drop("id","id2") // join 2 dfs on index columns --> .orderBy(col("WellId"),col("Day").cast("int")) effective here
  return tableDF
}
%md ##### getCasingData()
- The casing data comes from the dpr API end-point.
- The casing table is to include the WellID & NewCasingSizes (renamed as Size) & TotalCasingCount columns. WellID is a main branch (Data.WellID), whereas NewCasingSizes & TotalCasingCount are sub-branches (Data.Casings.NewCasingSizes & Data.Casings.TotalCasingCount).
- A cross-join occurred when we applied **flattenDataframe()** because it flattens all the columns *the same number of times*, but the WellID & TotalCasingCount columns only need exploding ONCE whereas the NewCasingSizes needs exploding TWICE - no JSON dictionary flattening is needed since NewCasingSizes is a list-of-lists.
- The WellID & TotalCasingCount columns cannot be exploded in the same dataframe - a cross-join occurs despite needing the same number of explodes. This is because the TotalCasingCount column is within a dictionary (1 level lower than WellID).
- We have defined a function **getCasingData()** which takes in 1 argument (the dataframe), and is used instead of **flattenDataframe()**.
###### How the function works:
- Since *WellID* column needs exploding ONCE, *TotalCasingCount* column needs exploding ONCE, & *NewCasingSizes* column needs exploding TWICE, we split the 3 columns into 3 separate dataframes.
- The separate dataframes are given a monotonically increasing id number so that they can be re-unified based on the id numbers AFTER the correct number of explodings & flattenings are done.
import org.apache.spark.sql.types.{StructType,ArrayType,StringType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode_outer
import scala.collection.mutable._
import org.apache.spark.sql.functions._
import spark.sqlContext.implicits._

// This function processes a nested DataFrame containing well and casing data, specifically extracting WellId, NewCasingSizes, ConductorCasing, and TotalCasingCount. It separates and transforms these nested fields, creates additional columns to indicate the casing type, and joins them into a structured format. The resulting DataFrame presents a clean and combined view of casing data for each well.

def getCasingData(df: DataFrame): DataFrame = {
  
    // Extract WellId and add index
    val wellid_df = df.withColumn("WellId", explode(col("Data.WellId")))
                      .withColumn("id", monotonically_increasing_id())
                      .drop("Data")
  
    // Extract NewCasingSizes and add index
    val new_casing_sizes_df = df.withColumn("NewCasingSizes", explode(col("Data.Casings.NewCasingSizes")))
                      .withColumn("isConductor", lit(false))
                      .withColumn("id2", monotonically_increasing_id())
                      .drop("Data")
                      .select(col("NewCasingSizes"), col("isConductor"), col("id2"))

    //exploding Sizes column also explodes index "id2" column - exploded df with "Size" & index "id2" column
    val sizes_df = new_casing_sizes_df.withColumn("Size",explode(col("NewCasingSizes")))
                                      .drop("NewCasingSizes")
                                      .select(col("Size"), col("isConductor"), col("id2"))

    // Extract ConductorCasing and add index
    val conductor_df = df.withColumn("Size", explode(col("Data.Casings.ConductorCasing")))
                          .withColumn("isConductor", lit(true))
                          .withColumn("id2", monotonically_increasing_id())
                          .drop("Data")
                          .select("Size", "isConductor", "id2")

    // Combine NewCasingSizes and ConductorCasing
    val combined_df = sizes_df.union(conductor_df)
    
    // Extract TotalCasingCount and add index
    val totalCasingCount_df = df.withColumn("TotalCasingCount", explode(col("Data.Casings.TotalCasingCount")))
                                .withColumn("id3", monotonically_increasing_id())
                                .drop("Data")

    // Join dataframes on index columns
    val tableDF = wellid_df.join(combined_df, col("id") === col("id2"), "inner")
                            .join(totalCasingCount_df, col("id") === col("id3"), "inner")
                            .drop("id", "id2", "id3")
                            .orderBy(col("WellId"), col("Size").cast("double").desc)
                            .where(!(col("Size").isNull) && !(trim(col("Size")) === ""))
                            .withColumn("Size", regexp_replace(col("Size"), "[^0-9./]", ""))
                            .select("WellId", "Size", "TotalCasingCount", "isConductor")

  return tableDF
}
%md ##### getAllPagesData()
import scalaj.http._
import org.apache.spark.sql.functions._
import scala.collection.mutable.Map
import org.apache.spark.sql._
var df: org.apache.spark.sql.DataFrame = null
var dfd: org.apache.spark.sql.DataFrame = null
var nurl = ""
var params = ""

def getAllPagesData(url:String, reqMethod:String, auth_type:String, auth_way:String, additionalParams:String, credentials:Map[String, String], contentType:String, selectCols:String, paginationType:String, paginationURLKeyword:String, paginationURLLocation:String, excludeCols:String, tableDF: org.apache.spark.sql.DataFrame, TARGET_TABLE:String):DataFrame = {
  println(s"${printSeparator("═", separatorLength)}")
  var response = getResp(url, reqMethod, auth_type, auth_way, additionalParams, credentials.toMap, contentType)
  if(response.isSuccess) {
    var json_string = response.body
    
  // Create Dataframe
   if(!("[]".equals(json_string))) { // change to if json_string does notcontains "Data" header
    dfd = spark.read.json(Seq(json_string).toDS).select(selectCols)
     
    if(!excludeCols.isEmpty()) {
      var df_ex = removingColumns(dfd, excludeCols)
      dfd = df_ex
    }
          
    var df_f = spark.emptyDataFrame
    TARGET_TABLE match{
      case "well_cpr" | "well_dpr" =>
        df_f = flattenDataframe(dfd, TARGET_TABLE)
      case "time_depth" =>
        df_f = getTimeDepth(dfd, TARGET_TABLE)
      case "casing" =>
        df_f = getCasingData(dfd)
      case _ =>
        throw new IllegalArgumentException(s"Unknown TARGET_TABLE: $TARGET_TABLE")
    }
    var dfd_f = df_f 
     
    println(s">>> ${tableDF.count()} rows existing, ${dfd_f.count()} rows fetched")
    println(s">>> Adding fetched rows to existing table...")
    df = tableDF.union(dfd_f)
    println(s">>> ${df.count()} cumulative rows")
      
    var checker = spark.read.json(Seq(json_string).toDS).select("PageInfo.Next").take(1)(0).toString() // prints out: [null] if current page is the final page
    if(checker.equals("[null]")) {  return df  }     
   }
    else {  return df  }
    
  // Next Page (pagination) 
  paginationType match{
    case "HEADER" =>
      val id = response.header(paginationURLKeyword).get
      if(!paginationURLLocation.equals(id)){
        credentials += (paginationURLKeyword->id)
        getAllPagesData(url, reqMethod, auth_type, auth_way, additionalParams, credentials, contentType, selectCols, paginationType, paginationURLKeyword, paginationURLLocation, excludeCols, df, TARGET_TABLE)
      }
       
    case "QueryParam" =>      
      var finalPage = spark.read.json(Seq(json_string).toDS).select("PageInfo.Last.Page").take(1)(0)(0).toString().toInt // gets value "PageInfo">"Last">"Page"
      var nextPage = spark.read.json(Seq(json_string).toDS).select(paginationURLKeyword).take(1)(0)(0).toString().toInt // gets value "PageInfo">"Next">"Page"
      println(">>> 📃 Next page: " + nextPage + "/" + finalPage)
      if((nextPage < finalPage) || (nextPage.equals(finalPage))) { // pagination not complete
//       if((nextPage == 2) || (nextPage == 3) || (nextPage == 4)) {
        var arr = additionalParams.split(";")
        var ln = arr.length
        arr(ln-1) = arr(ln-1).replace(additionalParams.substring(additionalParams.lastIndexOf("=")),"="+nextPage) // sets the page parameter to the next page -> arr(ln-1) = "page=2"
//         println("Now on " + arr(ln-1))
        
        for(x <- 0 to ln-1){
          if (x==0){
             params = arr(x)
          }
          else {
             params = params + ";" + arr(x)
          }
        }
     getAllPagesData(url, reqMethod, auth_type, auth_way, params, credentials, contentType, selectCols, paginationType, paginationURLKeyword, paginationURLLocation, excludeCols, df, TARGET_TABLE)
    }
    
    case "AbsoluteURL" =>
      if(json_string.contains(paginationURLLocation)){
        nurl = flattenDataframe(spark.read.json(Seq(json_string).toDS).select(excludeCols), TARGET_TABLE).where(paginationURLKeyword+"='"+paginationURLLocation+"'").select("href").head.get(0).toString()
        getAllPagesData(nurl, reqMethod, auth_type, auth_way, additionalParams, credentials, contentType, selectCols, paginationType, paginationURLKeyword, paginationURLLocation, excludeCols, df, TARGET_TABLE)
      }
    case _ =>
    }
   return df
  }

  else{
    println(response.statusLine)
    throw new Exception(s"⚠️ API response status was unsuccessful: ${response.statusLine}")
  }
  
}  
%md ##### changeColumnSchema()
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{to_date, to_timestamp}
import org.apache.spark.sql.functions._
// transformedColumnsInTarget = "x:y:z ; x2:y2:z2"

def changeColumnSchema(tableDF: org.apache.spark.sql.DataFrame, transformedColumnsInTarget:String):DataFrame = {
  
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
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
%md # NEW Testing Functions
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
    val strg_key = dbutils.secrets.get(ENV_KeyVaultScope, storageConfig.keySecretName)
    spark.conf.set("fs.azure.account.key." + storageConfig.accountName, strg_key)
    val isCp = dbutils.fs.cp("dbfs:/"+fileName, filePath+fileName)
    println(isCp+" IN : "+apiResponseType)
    if(isCp) dbutils.fs.rm("dbfs:/"+fileName)
    return String.valueOf(isCp)
  }
 return data
}
%md ##### getAllPagesTEXTData()
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
%md ##### parseXMLtoDF()
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

%md ##### secondaryBlobIngestion()
 def secondaryBlobIngestion(secondaryBlobStorageKey:String, target2Container:String,
                             target2StorageAccount:String, target2FilePath:String,
                             target2FileName:String,targetDBName:String, targetTblName:String)=
  {

    val cleanDF = spark.sql("select * from "+targetDBName+"."+targetTblName)
    val secondaryBlobStoragePath = "wasbs://"+target2Container+"@"+target2StorageAccount+target2FilePath

    spark.conf.set("fs.azure.account.key."+target2StorageAccount,secondaryBlobStorageKey)
    cleanDF.write.format("parquet").mode("overwrite").save(secondaryBlobStoragePath+targetTblName.toLowerCase())

    //target2FileName=ready.txt file. This is exclusive while pushing data to Palantir Blob Storage
    if(!(target2FileName.isEmpty))
    {
      val readyFilePath=secondaryBlobStoragePath+"ready"

      val readyFileDF = spark.sql("select cast( current_timestamp() as string)")
      readyFileDF.write.format("text").mode("overwrite").save(readyFilePath)

      for (file <- dbutils.fs.ls(readyFilePath))
        if (file.name.startsWith("part"))
          dbutils.fs.cp(file.path,secondaryBlobStoragePath+targetTblName.toLowerCase()+"/"+target2FileName)

      dbutils.fs.rm(readyFilePath,recurse=true)
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
