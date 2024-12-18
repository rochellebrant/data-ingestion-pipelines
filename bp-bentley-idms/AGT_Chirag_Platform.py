# %sql
# create or replace view realtime_agt.vw_agt_streaming_chirag_platform as select tag_name, ts_utc, COALESCE(stringValue, numeric_value, date_time_value, categorylabel+"::"+categoryValue, badValue) as tag_value, current_timestamp from realtime_agt.agt_streaming_chirag_platform

# select * from realtime_agt.vw_agt_streaming_chirag_platform

# select * from bentley_apm_reference.pi_tags_chirag_platform where is_active='Y' order by tagname
  
# %python
KEY_VAULT = 'ZNEUDL1P40INGing-kvl00'
STORAGE_ACCOUNT = 'zneudl1pa3apmstore01.blob.core.windows.net'
STORAGE_KEY_SECRET = 'accessKey-bentley-zneudl1pa3apmstore01'
CONTAINER = 'data'
STORAGE_PATH = 'AZERBAIJAN GEORGIA TURKEY/agt_streaming_chirag_platform/'

STORAGE_KEY =  dbutils.secrets.get(scope=KEY_VAULT, key=STORAGE_KEY_SECRET)
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}", STORAGE_KEY)
path = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}/{STORAGE_PATH}"

print(path)

start_date = '2022-01-01'
end_date = '2024-09-04'

query = f"""
SELECT tag_name, ts_utc, tag_value 
FROM realtime_agt.vw_agt_streaming_chirag_platform 
WHERE tag_name IN (
    SELECT tagname 
    FROM bentley_apm_reference.pi_tags_chirag_platform 
    WHERE asset = 'Chirag Platform' 
      AND is_active = 'Y'
) 
AND current_timestamp >= '{start_date}' 
AND current_timestamp < '{end_date}'
"""

df = spark.sql(query)
display(df)

if STORAGE_PATH == 'wasbs://data@zneudl1pa3apmstore01.blob.core.windows.net/AZERBAIJAN GEORGIA TURKEY/agt_streaming_chirag_platform/':
  # df.coalesce(1).write.format("parquet").mode("overwrite").save(STORAGE_PATH)
  # df.coalesce(1).write.format("parquet").mode("append").save(STORAGE_PATH)
