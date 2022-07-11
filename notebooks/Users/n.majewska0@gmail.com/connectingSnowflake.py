# Databricks notebook source
# MAGIC %md
# MAGIC ## This notebook will explain how to connect to Snowflake and how to write data to and from it.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Setup Snowflake Connection:

# COMMAND ----------

user = "ArturK"
password = "DemoPassword1!"

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