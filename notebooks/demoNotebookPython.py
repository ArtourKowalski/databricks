# Databricks notebook source
# MAGIC %md
# MAGIC ##Getting started with Azure Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC #### Great reference on using Spark Dataframes: https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-python.html

# COMMAND ----------

# MAGIC %md
# MAGIC ##### First, let's use SQL to display the data and change some of the parameters:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM diabetes
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Let's rename the columns to get rid of all the "_" in column names by creating new table:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE diabetesnew AS
# MAGIC SELECT 
# MAGIC   pregnancies,
# MAGIC   plasma_glucose as glucose,
# MAGIC   blood_pressure as bloodpressure,
# MAGIC   triceps_skin_thickness as skinthickness,
# MAGIC   insulin,
# MAGIC   bmi,
# MAGIC   diabetes_pedigree as pedigree,
# MAGIC   age,
# MAGIC   diabetes
# MAGIC FROM diabetes;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM diabetesnew
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Let's create a dataframe out of external table using Python method instead:

# COMMAND ----------

sdf = spark.read.csv('/FileStore/tables/diabetes.csv', header=True, sep=',', inferSchema=True)

# COMMAND ----------

sdf.show(5) # show top 5 rows, works exactly the same as SQL query above

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Let's rename the columns to get rid of the spaces:

# COMMAND ----------

sdfnew = sdf.toDF("pregnancies",
                        "glucose",
                        "bloodpressure",
                        "skinthickness",
                        "insulin",
                        "bmi",
                        "pedigree",
                        "age",
                        "diabetes")
sdfnew.show(5)
sdfnew.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Using display function, we can sort the results or change format to charts / pivots.

# COMMAND ----------

display(sdfnew)

# COMMAND ----------

sdfnew.columns # show all dataframes columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### There are multiple methods to display the data in Spark: 
# MAGIC ##### https://sparkbyexamples.com/spark/show-top-n-rows-in-spark-pyspark/

# COMMAND ----------

sdfnew.cache # This command will put dataframes to cache memory for faster reuse and better performance

# COMMAND ----------

sdfnew = sdfnew.dropna() # drop rows with missing values
sdfclean = sdf.fillna("--") # we can also replace N/A values with chosen char

# COMMAND ----------

sdf.count() == sdfnew.count() # let's check if we dropped any rows

# COMMAND ----------

countsByAge = sdfnew.groupBy("age").count() # count number of people by their age
countsByAge.display()

# COMMAND ----------

filterDF = sdfnew.filter(sdf.age > 25).sort('age') # Filter rows on Age column and sort

# COMMAND ----------

filterDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DataFrames are not queriable using SQL, in order to do that, we can create temporary table from Spark dataframe - note that after termination of the Cluster, table will be gone and will have to be recreated again

# COMMAND ----------

sdfnew.createOrReplaceTempView("diabetesDF")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM diabetesDF
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### To create permanent table, use following command:

# COMMAND ----------

sdfnew.write.saveAsTable("diabetesDFnew")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM diabetesDFNEW
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Let's visualize maximum BMI for each age value:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT bmi, age from diabetesDFNEW
# MAGIC ORDER BY AGE;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Let's check the age distribution within the dataset:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) as population, age
# MAGIC FROM diabetesDFnew
# MAGIC GROUP BY age
# MAGIC ORDER BY AGE ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Let's check which age group has the biggest number of diabetes within the dataset:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AGE, SUM(diabetes) AS TOTAL_DIABETES
# MAGIC FROM diabetesDFnew
# MAGIC GROUP BY AGE
# MAGIC ORDER BY TOTAL_DIABETES DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### We can use SQL to create a PySpark DataFrame - let's create DataFrame that only contains people with diabetes:

# COMMAND ----------

dfdiabetes = spark.sql('SELECT * FROM diabetes WHERE DIABETES = 1')
dfdiabetes.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### To explore more SQL Capabilities within Databricks Notebook, please refer to:
# MAGIC ##### https://github.com/ArtourKowalski/databricks/blob/main/notebooks/demoNotebookSQL.sql