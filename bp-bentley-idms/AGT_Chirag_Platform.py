# %sql
# create or replace view realtime_agt.vw_agt_streaming_chirag_platform as select tag_name, ts_utc, COALESCE(stringValue, numeric_value, date_time_value, categorylabel+"::"+categoryValue, badValue) as tag_value, current_timestamp from realtime_agt.agt_streaming_chirag_platform

# select * from realtime_agt.vw_agt_streaming_chirag_platform

# select * from bentley_apm_reference.pi_tags_chirag_platform where is_active='Y' order by tagname
  
# %python
keyVaultScope = 'ZNEUDL1P40INGing-kvl00'
storageAccount = 'zneudl1pa3apmstore01.blob.core.windows.net'
storageKeySecret = 'accessKey-bentley-zneudl1pa3apmstore01'
container = 'data'
storagePath = 'AZERBAIJAN GEORGIA TURKEY/agt_streaming_chirag_platform/'
storageKey =  dbutils.secrets.get(scope=keyVaultScope,key=storageKeySecret)
spark.conf.set("fs.azure.account.key."+storageAccount,storageKey)
path = "wasbs://"+container+"@"+storageAccount+"/"+storagePath
print(path)

df = spark.sql("select tag_name, ts_utc, tag_value from realtime_agt.vw_agt_streaming_chirag_platform where tag_name in (select tagname from bentley_apm_reference.pi_tags_chirag_platform where is_active='Y') and current_timestamp >= '2020-01-01' and current_timestamp < '2024-09-12' ")
display(df)

if path == 'wasbs://data@zneudl1pa3apmstore01.blob.core.windows.net/AZERBAIJAN GEORGIA TURKEY/agt_streaming_chirag_platform/':
  # df.coalesce(1).write.format("parquet").mode("overwrite").save(path)
  # df.coalesce(1).write.format("parquet").mode("append").save(path)
