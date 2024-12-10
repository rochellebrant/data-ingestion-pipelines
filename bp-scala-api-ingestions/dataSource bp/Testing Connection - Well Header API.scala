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

def flattenDataframe(df: DataFrame, sourceChangeKeyCols: String): DataFrame = {

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
          return flattenDataframe(explodedDf, sourceChangeKeyCols)
        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName +"."+childname)
          val newfieldNames = fieldNames.filter(_!= fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))))
         val explodedf = df.select(renamedcols:_*)
          return flattenDataframe(explodedf, sourceChangeKeyCols)
        case _ =>
      }
    }
  // val cols = fieldNames.map(c => col(c.toString()).as(c.toString().substring(c.toString().indexOf("_")+1))) // removes prefix & underscore when flattening
  // return df.select(cols:_*)
  // val cols = fieldNames.map(c => col(c.toString()).as(c.toString().substring(sourceChangeKeyCols.length+1, c.toString().length()))) // removes sourceChangeKeyCols prefix & following underscore only
  // return df.select(cols : _*)
  return df
  }

def removePrefix(df: DataFrame, sourceChangeKeyCols: String): DataFrame = {

  val fields = df.schema.fields
  val fieldNames = fields.map(x => x.name)
  val cols = fieldNames.map(c => col(c.toString()).as(c.toString().substring(sourceChangeKeyCols.length+1, c.toString().length()))) // removes sourceChangeKeyCols prefix & following underscore only
  return df.select(cols : _*)
  }

import scalaj.http._
import org.apache.spark.sql._
import sys.process._
import org.apache.spark.sql.DataFrame

var tokenURL = "https://login.microsoftonline.com/ea80952e-a476-42d4-aaf4-5457852b0f7e/oauth2/token"

var response = Http(tokenURL).postForm(Seq("grant_type" -> "client_credentials", "client_id" -> "bb09b0d5-dd33-49b1-aea7-e9b4c5189f16", "client_secret" -> "client_secret" ,"requested_token_use" -> "on_behalf_of","resource" -> "https://wellheader-dsbp-api.bpweb.bp.com")).asString

var select_token = spark.read.json(Seq(response.body).toDS).select("access_token")
var token = select_token.select("access_token").take(1)(0)(0).toString()
// println(token)

bypass()

val sourceChangeKeyCols = "value"
var url = "https://wellheader-dsbp-api.bpweb.bp.com/odata/product/well_completions"

var response = Http(url).timeout(connTimeoutMs = 1000000, readTimeoutMs = 500000).header("Authorization","Bearer "+token).asString

var json = response.body
println(response)

var dfd = spark.read.json(Seq(json).toDS).select(sourceChangeKeyCols)
var tableDF = flattenDataframe(dfd, sourceChangeKeyCols)

// for(col <- tableDF.columns){  tableDF = tableDF.withColumnRenamed(col,col.replaceAll("\\s", "_"))  }
display(tableDF)


tableDF = removePrefix(tableDF, sourceChangeKeyCols)
display(tableDF)
