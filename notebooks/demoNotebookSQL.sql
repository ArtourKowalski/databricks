-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Getting started with Azure Databricks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Ingest Data to the Notebook
-- MAGIC 
-- MAGIC ##### • Ingest data from dbfs sources
-- MAGIC ##### • Create Sources as tables

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM employees_csv
-- MAGIC LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Let us change name of the table first (if by some reason we chose to leave _csv at the end of table name, we can simply rename it with following command):

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC ALTER TABLE employees_csv
-- MAGIC RENAME TO employees;

-- COMMAND ----------

SELECT * FROM EMPLOYEES
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### We can easily check the schema of created table with following command:

-- COMMAND ----------

DESCRIBE TABLE EMPLOYEES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### We can notice that HIRE_DATE column is of "string" format. Let's assume that we need to have it stored as "date" format for future reference:

-- COMMAND ----------

ALTER TABLE employees
ADD COLUMN HIRED_DATE DATE;

-- COMMAND ----------

UPDATE EMPLOYEES
SET HIRED_DATE = TO_DATE(HIRE_DATE, 'dd-MMM-yy');

-- COMMAND ----------

DESCRIBE EMPLOYEES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### We can easily create new tables based on already existing table:

-- COMMAND ----------

DROP TABLE IF EXISTS ANNUAL_SALARY;

CREATE TABLE annual_salary AS (
SELECT EMPLOYEE_ID, CONCAT(FIRST_NAME, ' ', LAST_NAME) AS EMPLOYEE_NAME, (SALARY * 12) AS ANNUAL_SALARY, DEPARTMENT_ID AS DEPARTMENT
FROM EMPLOYEES);

-- COMMAND ----------

SELECT * FROM annual_salary

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Let's run some analysis on the annual_salary data - let's check who are the top 5 of employees annual salary-wise:

-- COMMAND ----------

SELECT EMPLOYEE_ID, EMPLOYEE_NAME, ANNUAL_SALARY, DEPARTMENT
FROM ANNUAL_SALARY
ORDER BY ANNUAL_SALARY DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Databricks Notebook allows to display queried data as a charts:

-- COMMAND ----------

SELECT EMPLOYEE_ID, EMPLOYEE_NAME, ANNUAL_SALARY, DEPARTMENT
FROM ANNUAL_SALARY
ORDER BY ANNUAL_SALARY DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Let's see what is the total salary for each department:

-- COMMAND ----------

SELECT DEPARTMENT, SUM(ANNUAL_SALARY) AS DEPARTMENT_COST
FROM ANNUAL_SALARY
GROUP BY DEPARTMENT
ORDER BY DEPARTMENT_COST DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### If we want to save the results to CSV file (for future reference or to send it to other department), we can easily Export the file by pressing "download" button under query result

-- COMMAND ----------

SELECT DEPARTMENT, SUM(ANNUAL_SALARY) AS DEPARTMENT_COST
FROM ANNUAL_SALARY
GROUP BY DEPARTMENT
ORDER BY DEPARTMENT_COST DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Let's say that company wants to introduce the commission system based on employees total experience - currently we would have to manually measure the time, so let SQL do this for us:

-- COMMAND ----------

ALTER TABLE EMPLOYEES
ADD COLUMN seniority FLOAT;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Calculate hire_date - current_date total elapsed time and insert the values to the column:

-- COMMAND ----------

UPDATE EMPLOYEES
SET seniority = ROUND(((BIGINT(TO_TIMESTAMP(NOW())) - (BIGINT(TO_TIMESTAMP(HIRED_DATE)))) / 86400) / 365, 1)

-- COMMAND ----------

SELECT * FROM EMPLOYEES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Now, let's add the commission values based on the total experience:

-- COMMAND ----------

UPDATE EMPLOYEES
SET COMMISSION_PCT = 
CASE 
  WHEN SENIORITY BETWEEN 10 AND 15 THEN '3'
  WHEN SENIORITY BETWEEN 15.1 AND 17 THEN '5'
  WHEN SENIORITY BETWEEN 17.1 AND 20 THEN '10'
  WHEN SENIORITY > 20.1 THEN '15'
END;

-- COMMAND ----------

SELECT * FROM EMPLOYEES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Query result can also be displayed as a pivot table:

-- COMMAND ----------

SELECT JOB_ID, AVG(SALARY)
FROM EMPLOYEES
GROUP BY JOB_ID;