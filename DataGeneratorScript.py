# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Data Generator Script 

# COMMAND ----------

import pandas as pd
import random
from datetime import datetime, timedelta

# COMMAND ----------

# Generate data for January 2023

start_date = datetime(2023, 1, 1)
end_date = datetime(2024, 1, 31)

date_generated = [start_date + timedelta(days=x) for x in range(0, (end_date-start_date).days)]

transport_types = ["Bus", "Train", "Tram", "Metro"]
routes = ["Route_" + str(i) for i in range(1, 11)]
stations = ["Station_" + str(i) for i in range(1, 21)]

# Randomly select 5 days as extreme weather days
extreme_weather_days = random.sample(date_generated, 5)

data = []

for date in date_generated:
    for _ in range(32):  # 32 records per day to get a total of 992 records for January
        transport = random.choice(transport_types)
        route = random.choice(routes)

        # Normal operating hours
        departure_hour = random.randint(5, 22)
        departure_minute = random.randint(0, 59)

        # Introducing Unusual Operating Hours for buses
        if transport == "Bus" and random.random() < 0.05:  # 5% chance
            departure_hour = 3

        departure_time = f"{departure_hour:02}:{departure_minute:02}"

        # Normal duration
        duration = random.randint(10, 120)

        # Introducing Short Turnarounds
        if random.random() < 0.05:  # 5% chance
            duration = random.randint(1, 5)

        # General delay
        delay = random.randint(0, 15)

        # Weather Impact
        if date in extreme_weather_days:
            # Increase delay by 10 to 60 minutes
            delay += random.randint(10, 60)

            # 10% chance to change the route
            if random.random() < 0.10:
                route = random.choice(routes)

        total_minutes = departure_minute + duration + delay
        arrival_hour = departure_hour + total_minutes // 60
        arrival_minute = total_minutes % 60
        arrival_time = f"{arrival_hour:02}:{arrival_minute:02}"

        passengers = random.randint(1, 100)
        departure_station = random.choice(stations)
        arrival_station = random.choice(stations)

        data.append([date, transport, route, departure_time, arrival_time, passengers, departure_station, arrival_station, delay])

df = pd.DataFrame(data, columns=["Date", "TransportType", "Route", "DepartureTime", "ArrivalTime", "Passengers", "DepartureStation", "ArrivalStation", "Delay"])

output_file_name = f"SourceRaw-{start_date.strftime('%Y-%m-%d')}.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Save the generated data into the data lake 

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/data/public-transport-data

# COMMAND ----------

df.to_csv(f"/dbfs/mnt/data/public-transport-data/raw/{output_file_name}", index = False)


# COMMAND ----------

dbutils.fs.unmount("/mnt/data/public-transport-data")
