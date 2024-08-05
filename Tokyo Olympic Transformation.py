# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DataType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "ID",
"fs.azure.account.oauth2.client.secret": 'SECRET',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/tanent_id/oauth2/token"}

dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@tokyoolympicdatahasan77.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolympichasan",
extra_configs = configs)



# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolympichasan"

# COMMAND ----------

spark

# COMMAND ----------

athletes = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("/mnt/tokyoolympichasan/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("/mnt/tokyoolympichasan/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("/mnt/tokyoolympichasan/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("/mnt/tokyoolympichasan/raw-data/medals.csv")
teams = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("/mnt/tokyoolympichasan/raw-data/teams.csv")

# COMMAND ----------

athletes.show()

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

coaches.show()

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

entriesgender.show()

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

entriesgender = entriesgender.withColumn("Female", col("Female").cast(IntegerType()))\
    .withColumn("Male", col("Male").cast(IntegerType()))\
        .withColumn("Total", col("Total").cast(IntegerType()))



# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

medals.show()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

teams.show()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

#Find the top countries with the highest number of gold medals
top_gold_medal_countries = medals.orderBy("Gold", ascending = False).select("TeamCountry", "Gold").show()

# COMMAND ----------

#Calculate the average number of entries by gender for each discipline
average_entries_by_gender = entriesgender.withColumn(
    'Avg_Female', entriesgender['Female'] / entriesgender['Total']
).withColumn(
    'Avg_Male', entriesgender['Male'] / entriesgender['Total']
)
average_entries_by_gender.show()


# COMMAND ----------

#load the data into the right location
athletes.repartition(1).write.mode("overwrite").option("header", 'true').csv("/mnt/tokyoolympichasan/transformed-data/atheletes")

# COMMAND ----------

#load the data into the right location
coaches.repartition(1).write.mode("overwrite").option("header", 'true').csv("/mnt/tokyoolympichasan/transformed-data/coaches")
entriesgender.repartition(1).write.mode("overwrite").option("header", 'true').csv("/mnt/tokyoolympichasan/transformed-data/entriesgender")
medals.repartition(1).write.mode("overwrite").option("header", 'true').csv("/mnt/tokyoolympichasan/transformed-data/medals")
teams.repartition(1).write.mode("overwrite").option("header", 'true').csv("/mnt/tokyoolympichasan/transformed-data/teams")
