# Databricks notebook source
# MAGIC %md
# MAGIC ## This notebook will explain how to connect to Snowflake and how to write data to and from it.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Setup Snowflake Connection:

# COMMAND ----------

user = "xxxxxxxxxxxxxxxxxxxxxx"
password = "xxxxxxxxxxxxxxxxxxxxxx!"

options = { 
"sfUrl": "ey57100.west-europe.azure.snowflakecomputing.com/",
"sfUser": user,
"sfPassword": password,
"sfDatabase": "DATABRICKS",
"sfSchema": "DIABETES",
"sfWarehouse": "COMPUTE_WH"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Write Databricks uploaded/existing data to DataFrame:

# COMMAND ----------

diabetesDF = spark.sql('SELECT * FROM diabetesnew')
diabetesDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Write data from Databricks DataFrame to Snowflake Table (this will create the table in Snowflake if it doesn't exist):

# COMMAND ----------

diabetesDF.write.format("snowflake").options(**options).option("dbtable", "diabetes").mode("append").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Load data from Snowflake Table/View to Databricks and save it as Temporary/Permanent table:

# COMMAND ----------

# first, load the data from Snowflake view/table
diabetesPositiveDF = spark.read.format("snowflake").options(**options).option("query", "SELECT * FROM POSITIVE_DIABETES").load()
# save imported data to temporary or permanent table
diabetesPositiveDF.write.saveAsTable("POSITIVE_DIABETES")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM POSITIVE_DIABETES
# MAGIC LIMIT 5;

# COMMAND ----------

user = "ArturK"
password = "DemoPassword1!"

options = { 
"sfUrl": "ey57100.west-europe.azure.snowflakecomputing.com/",
"sfUser": user,
"sfPassword": password,
"sfDatabase": "DATABRICKS",
"sfSchema": "SALARIES",
"sfWarehouse": "COMPUTE_WH"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Let's assume that we have multiple CSV files stored in Databricks and want to load it into one, master CSV file:

# COMMAND ----------

# load all partitions files into salaries DataFrame from DBFS

file_list = []
path = '/FileStore/tables/salaries/'
files = dbutils.fs.ls(path)

# loop through each file and add it into file_list if name starts with "partition"
for file in files:
    if(file.name.startswith("partition")):
        file_list.append(path + file.name)

salariesDF = spark.read.load(path=file_list, format="csv", sep=",", inferSchema="true", header="true")

# COMMAND ----------

# sort created DataFrame by id column
salariesDF = salariesDF.sort(salariesDF.id.asc())
salariesDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Right now, id values start from 0 and should from 1, let's change it using Python:

# COMMAND ----------

salariesDF = salariesDF.withColumn("id", salariesDF.id+1)
salariesDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write data to Snowflake:

# COMMAND ----------

salariesDF.write.format("snowflake").options(**options).option("dbtable", "ds_salaries").mode("append").save()
