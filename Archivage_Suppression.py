# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Automatisation des Politiques de Conservation

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Identification des anciennes données et déplacement vers un répertoire d'archive

# COMMAND ----------

storage_account_name = "sabrilakestorage"
storage_account_access_key = "yzSzJ2rZJE0rsNI5rvCUYVDKAESX9SPhaeHNgAiRdm/y6b5nXn+c0hWIEYn67wZzBKUwnQXehjjl+ASt24YBKQ=="

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

from datetime import datetime

# Define the paths for raw and processed data
raw_path = "wasbs://public-transport-data@sabrilakestorage.blob.core.windows.net/raw/"
processed_path = "wasbs://public-transport-data@sabrilakestorage.blob.core.windows.net/processed/"
processed_archive_path = "wasbs://public-transport-data@sabrilakestorage.blob.core.windows.net/processed/Archive"
raw_archive_path = "wasbs://public-transport-data@sabrilakestorage.blob.core.windows.net/raw/Archive"

# Get the list of processed files
processed_files = dbutils.fs.ls(processed_path)
# Get the list of raw files
raw_files = dbutils.fs.ls(raw_path)

# Get the list of processed files
processed_archive_files = dbutils.fs.ls(processed_archive_path)
# Get the list of raw files
raw_archive_files = dbutils.fs.ls(raw_archive_path)

# Create a list to store the names of processed files and raw files 
processed_files = [p.name for p in processed_files]
raw_files = [r.name for r in raw_files]

processed_archive_names = [ap.name for ap in processed_files]
raw_archive_names = [ar.name for ar in raw_files]

for r in raw_files:

    modification_time_ms = r.modificationTime
    modification_time = datetime.fromtimestamp(modification_time_ms / 1000)  # Divide by 1000 to convert milliseconds to seconds
    datenow = datetime.now()
    duration =  datenow - modification_time

    if(duration.days > 30):
        #Archive data
        dbutils.fs.cp(r.path,'wasbs://public-transport-data@sabrilakestorage.blob.core.windows.net/raw/Archive/'+r.name,recurse=True)

        #Delete data after archive
        dbutils.fs.rm(r.path, recurse=True)
        print('Source Raw : '+r.name+' have moved to archive successfuly!')

for p in processed_files:

    modification_time_ms = p.modificationTime
    modification_time = datetime.fromtimestamp(modification_time_ms / 1000)  # Divide by 1000 to convert milliseconds to seconds
    datenow = datetime.now()
    duration =  datenow - modification_time

    if(duration.days > 30):
        # Archive data
        dbutils.fs.cp(p.path,'wasbs://public-transport-data@sabrilakestorage.blob.core.windows.net/processed/Archive/'+p.name,recurse=True)

        # Delete data after archive
        dbutils.fs.rm(p.path, recurse=True)
        print('Historique Treated File  : '+p.name+' have moved to processed archive successfuly!')


for ap in processed_archive_names:

    modification_time_ms = ap.modificationTime
    modification_time = datetime.fromtimestamp(modification_time_ms / 1000)  # Divide by 1000 to convert milliseconds to seconds
    datenow = datetime.now()
    duration =  datenow - modification_time

    if(duration.days > 30):
        # Delete data after archive
        dbutils.fs.rm(ap.path, recurse=True)
        print('Archive  File : '+ap.name+' deleted successfuly!')


