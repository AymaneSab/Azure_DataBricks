# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Set the data location and type
# MAGIC
# MAGIC There are two ways to access Azure Blob storage: account keys and shared access signatures (SAS).
# MAGIC
# MAGIC

# COMMAND ----------

storage_account_name = "sabrilakestorage"
storage_account_access_key = "yzSzJ2rZJE0rsNI5rvCUYVDKAESX9SPhaeHNgAiRdm/y6b5nXn+c0hWIEYn67wZzBKUwnQXehjjl+ASt24YBKQ=="

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth, dayofweek, to_date
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, expr
from pyspark.sql.functions import col, when
from pyspark.sql.functions import col, hour
from pyspark.sql.functions import col, avg, sum , count ;



def dataTransformationFunction(file_location):

    file_location = file_location
    file_type = "csv"   

    df = spark.read.format(file_type).option("header", "true").load(file_location)



    # Convert the 'Date' column to a date type
    df = df.withColumn("Date", to_date(df["Date"]))

    # Extract year, month, day, and day of the week
    df = df.withColumn("Year", year(df["Date"]).cast(IntegerType()))
    df = df.withColumn("Month", month(df["Date"]).cast(IntegerType()))
    df = df.withColumn("Day", dayofmonth(df["Date"]).cast(IntegerType()))
    df = df.withColumn("DayOfWeek", dayofweek(df["Date"]).cast(IntegerType()))


    # Split and convert "DepartureTime" and "ArrivalTime" to floats
    df = df.withColumn("DepartureTimeFloat", expr("float(split(DepartureTime, ':')[0]) * 60 + float(split(DepartureTime, ':')[1])"))
    df = df.withColumn("ArrivalTimeFloat", expr("float(split(ArrivalTime, ':')[0]) * 60 + float(split(ArrivalTime, ':')[1])"))

    # Calculate trip duration in minutes as float
    df = df.withColumn("TripDuration", col("ArrivalTimeFloat") - col("DepartureTimeFloat"))


    # Define conditions for delay categories
    no_delay_condition = col("Delay") == "0"
    short_delay_condition = (col("Delay").cast("int") >= 1) & (col("Delay").cast("int") <= 10)
    medium_delay_condition = (col("Delay").cast("int") >= 11) & (col("Delay").cast("int") <= 20)
    long_delay_condition = col("Delay").cast("int") > 20

    # Create a new column "DelayCategory" based on the conditions
    df = df.withColumn("DelayCategory", 
                    when(no_delay_condition, "Pas de Retard")
                    .when(short_delay_condition, "Retard Court")
                    .when(medium_delay_condition, "Retard Moyen")
                    .when(long_delay_condition, "Long Retard")
                    .otherwise("Unknown"))  # Set a default category for unexpected values
    

    # Extract the hour from the "DepartureTime" column
    df = df.withColumn("Hour", hour(df["DepartureTime"]))

    # Group by the hour and calculate the average number of passengers
    passenger_analysis = df.groupBy("Hour").agg({"Passengers": "avg"})



    # Group the DataFrame by the "Route" column
    route_analysis = df.groupBy("Route").agg(
        avg("Delay").alias("AverageDelay"),
        avg("Passengers").alias("AveragePassengers"),
        count("*").alias("TotalTrips")
    )

    storageAccountName = "sabrilakestorage"
    storage_account_access_key = "yLqmyFoSpNzY2+vc1xNWYgtfbwEe3RhIViMK7E5xryXECw9/4bxRFsO9zX3TECmSYwJy7RyTMa2G+AStmaRjbw=="

    sasToken = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-09-27T15:48:20Z&st=2023-09-27T07:48:20Z&spr=https&sig=s5gVSe%2FmaKRAsu7kI0AK%2Fp%2BZ7ZjY6yfAns89Wa9ukvc%3D"

    blobContainerName = "public-transport-data"
    mountPoint = "/mnt/data/public-transport-data"

    if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):

        try:
            dbutils.fs.mount(
            source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
            mount_point = mountPoint,
            #extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
            extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
            )
            print("mount succeeded!")
            
        except Exception as e:
            print("mount exception", e)

    pandas_df = df.toPandas()

    outpout_file_name = file_location.split("/")[-1]

    pandas_df.to_csv(f"/dbfs/mnt/data/public-transport-data/processed/Processed-{outpout_file_name}", index = False)

    dbutils.fs.unmount("/mnt/data/public-transport-data")



# COMMAND ----------

#Automatisation
#Connection configuration

raw_path = "wasbs://public-transport-data@sabrilakestorage.blob.core.windows.net/raw/"
processed_path = "wasbs://public-transport-data@sabrilakestorage.blob.core.windows.net/processed/"

# Get the list of processed files
processed_files = dbutils.fs.ls(processed_path)

# Get the list of raw files
raw_files = dbutils.fs.ls(raw_path)

# Create a list to store the names of processed files
processed_files_csv = [f.name for f in processed_files]

# Initialize variables
processed_items_count = 0
concatenated_raw_data = ""

# Iterate through raw files
for f_raw in raw_files:
    if processed_items_count == 2:
        break
    if f_raw.name not in processed_files_csv:
        processed_items_count+=1
        file_location = f"wasbs://public-transport-data@sabrilakestorage.blob.core.windows.net/raw/{f_raw.name}"
        dataTransformationFunction(file_location)
 

# COMMAND ----------


