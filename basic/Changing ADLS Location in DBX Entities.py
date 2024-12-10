"""
# Changing the DBX metastore location
DBX does not provide a command to update the path of an existing database. The only way to update the location of  DBX database is by updating the corresponding DBX metastore table entry:

`update dbo.dbs`
`set DB_LOCATION_URI = '<new full path>' WHERE NAME = '<db name>'`
%md
### If the Database is empty...
You just need to run the update SQL query above.
%md
### If the Database contains tables...
1) Copy the P33 folder to the correct location.
2) Update the database location in DBX metastore (P40) using the update SQL query above.
3) Run `ALTER TABLE` statement in DBX for each table in the database using the modified path.
4) Delete the old/incorrect copy of the database folder in ADLS (P33).
"""

%sql
describe database dsbp_welllogs
#### Set parameters used in alter table query

# dbName = "dsbp_welllogs"
# dbName = "dsbp_wellboremarkers"
# dbName = "dsbp_wdpvsa"
# dbName = "dsbp_dpz"
# dbName = "dsbp_reservoirfluids"
dbName = "dsbp_geotechborehole"


currentPathPart = "/dataSourceBp/"
replaceWithPathPart = "/DATASOURCEBP/"
import pyspark
from pyspark.sql.types import *

#### Set parameters used in alter table query
# dbName = "gbl_ref_toledo_decomission_dev"
# currentPathPart = "/UserInformationModel/"
# replaceWithPathPart = "/UsersInformationModel/"


#### Define empty dataframe with the schema needed to capture the logs from the alter table query
emp_RDD = spark.sparkContext.emptyRDD()
columns1 = StructType([StructField('dbTable',StringType(),False),StructField('alterExecuted',StringType(),False),StructField('errorMessage',StringType(),False)])
resultdf = spark.createDataFrame(data = emp_RDD , schema = columns1)
# display(resultdf)


#### List tables in the database
df = spark.sql("SHOW TABLES IN {0}".format(dbName))
tablesList = df.select("tableName").toPandas()
tables = list(tablesList['tableName'])
print(f'Tables in database {dbName} ➜ {tables}')

print()

####Loop through each table and run alter location query. Location is N/A for views.
for t in tables:
  tdetaildf = spark.sql("DESCRIBE DETAIL {0}.{1}".format(dbName , t)) # df obj
  curLoc = tdetaildf.first()['location']
  print(f'Current location: {curLoc}')
  newLoc = curLoc.replace(currentPathPart , replaceWithPathPart)
  print(f'New location:     {newLoc}')
  altersql = "ALTER TABLE {0}.{1} SET LOCATION '{2}'".format(dbName, t, newLoc)
  print(altersql)
  # spark.sql(altersql)
  print()
