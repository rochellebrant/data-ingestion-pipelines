"""
Scala Section:

The code generates and executes SQL statements to create tables in a Spark SQL database, where each table's data is stored in Parquet format at a location specified by a path in Azure Data Lake Storage (ADLS).
It loops over the tableList (which contains table names), constructs the necessary location, and creates a table for each entry in the list.

Python Section:

The code takes a string of table names separated by spaces, splits it into a list, and then formats each table name by adding double quotes around it and a comma at the end. Finally, it prints each formatted table name.
"""

# This is a list of table names stored in the variable tableList. Each string represents the name of a table that the script will later use in SQL queries.
val tableList = List("vassetmanufacturers",
"vassets",
"vattributes",
"vchangerequests",
"vclassifications",
"vdocumentfiles",
"vdocuments",
"vengineeringproducts",
"vengineeringworkpackages",
"ventityauditinfo",
"ventitycrossreferences",
"ventitydocuments",
"ventityreadpermissions",
"ventitysegments",
"vgroupedvirtualitems",
"vlocations",
"vnotes",
"vorganizationmembership",
"vorganizations",
"vpersons",
"vprojects",
"vrelationships",
"vresponsibilities",
"vroles",
"vsegments",
"vsessions",
"vtasks",
"vtransmittals")

val temp = ""
# The loop iterates over each table name in tableList.
# For each table (temp), it creates a loc (location) string which is a path to a data file stored in Azure Data Lake Storage (ADLS), prefixed with abfss:// indicating Azure Blob File System (ABFS) protocol.
#The script prints out a SQL CREATE TABLE statement for each table, which essentially creates a table in the alim_refineries database using the parquet format located at the loc path.
# spark.sql(s"""CREATE TABLE $dbxDB.$temp...)` executes this SQL statement to actually create the table in the Spark environment.
# tableStmt.rdd.take(1) is used to retrieve the first row of the result (though it isn't used afterward).
for(temp <- tableList)
{
  var dbxDB = "alim_refineries"
  var loc = "abfss://udl-container@zneudl1p33lakesstor.dfs.core.windows.net/Raw/ALIM_REFINERIES/"+temp
  println(s"""CREATE TABLE $dbxDB.$temp using parquet location '$loc' """)
  
  val tableStmt = spark.sql(s"""CREATE TABLE $dbxDB.$temp using parquet location '$loc' """)
  val tableStmt_RDD=tableStmt.rdd.take(1)
}

%python
a = "vassetmanufacturers vassets vattributes vchangerequests vclassifications vdocumentfiles vdocuments vengineeringproducts vengineeringworkpackages ventityauditinfo ventitycrossreferences ventitydocuments ventityreadpermissions ventitysegments vgroupedvirtualitems vlocations vnotes vorganizationmembership vorganizations vpersons vprojects vrelationships vresponsibilities vroles vsegments vsessions vtasks vtransmittals"
a = a.split(' ')
b = []
for i in a:
  x = '"' + i + '",'
  b.append(x) # For each table name in the list a, this code wraps it in double quotes (") and adds a comma at the end. The result is stored in the list b.
  
for i in b:
  print(i)
