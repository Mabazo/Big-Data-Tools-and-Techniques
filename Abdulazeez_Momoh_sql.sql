-- Databricks notebook source
-- MAGIC %python
-- MAGIC # Year of clinical trial
-- MAGIC ct_year = "2021"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fileroot = "clinicaltrial_" + ct_year
-- MAGIC
-- MAGIC import os
-- MAGIC os.environ ['fileroot'] = fileroot

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC rm /tmp/$fileroot.csv
-- MAGIC rm /tmp/$fileroot.zip

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Clean the DBFS from the contents that the notebook needs to create again
-- MAGIC
-- MAGIC dbutils.fs.rm("/FileStore/tables/" + fileroot + ".csv", True)
-- MAGIC dbutils.fs.rm("/FileStore/tables/pharma.csv", True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("/FileStore/tables/" + fileroot + ".zip", "file:/tmp/")
-- MAGIC dbutils.fs.cp("/FileStore/tables/pharma.zip", "file:/tmp/")

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC unzip -d /tmp /tmp/$fileroot.zip
-- MAGIC unzip -d /tmp /tmp/pharma.zip

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("file:/tmp/" + fileroot + ".csv", "/FileStore/tables/" + fileroot + ".csv", True )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("file:/tmp/pharma.csv", "/FileStore/tables/pharma.csv", True )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables/")

-- COMMAND ----------

-- The way Databricks is configured. Variable defined in the cell/notebook of a language is not available in the REPL of another language/another cell. REPLs can share state only through external resources such as files in DBFS or objects in object storage.
-- This means that the year variable defined in RDD file is not available in this SQL notebook. Thus, we need to create the variable again in this notebook

SET year=2021;

-- COMMAND ----------

-- Loading the Data

-- COMMAND ----------

-- Remove the existing tables and views 

-- COMMAND ----------

DROP TABLE IF EXISTS clinicaltrial_${hivevar:year};
DROP VIEW IF EXISTS clinicaltrial_view;
DROP VIEW IF EXISTS conditions_view;
DROP VIEW IF EXISTS sponsors_view;
DROP VIEW IF EXISTS completion_view;

-- COMMAND ----------

DROP TABLE IF EXISTS pharma;
DROP VIEW IF EXISTS pharma_view;

-- COMMAND ----------

-- Creating clinical trial table, if it does not exist

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS clinicaltrial_${hivevar:year}(
  Id STRING,
  Sponsor STRING,
  Status STRING,
  Start STRING,
  Completion STRING,
  Type STRING,
  Submission STRING,
  Conditions STRING,
  Interventions STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/FileStore/clinicaltrial_tables';

-- COMMAND ----------

LOAD DATA INPATH '/FileStore/tables/clinicaltrial_${hivevar:year}.csv' OVERWRITE INTO TABLE clinicaltrial_${hivevar:year};

-- COMMAND ----------

SELECT * FROM clinicaltrial_${hivevar:year}

-- COMMAND ----------

-- Creating pharma table, if it does not exist

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS pharma(
  Company STRING, Parent_Company STRING, Penalty_Amount STRING,
  Subtraction_From_Penalty STRING, Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting STRING,
  Penalty_Year STRING, Penalty_Date STRING, Offense_Group STRING, Primary_Offense STRING,
  Secondary_Offense STRING, Description STRING, Level_of_Government STRING, Action_Type STRING,
  Agency STRING, `Civil/Criminal` STRING, Prosecution_Agreement STRING, Court STRING,
  Case_ID STRING, Private_Litigation_Case_Title STRING, Lawsuit_Resolution STRING,
  Facility_State STRING, City STRING, Address STRING, Zip STRING, NAICS_Code STRING,
  NAICS_Translation STRING, HQ_Country_of_Parent STRING, HQ_State_of_Parent STRING,
  Ownership_Structure STRING, Parent_Company_Stock_Ticker STRING, Major_Industry_of_Parent STRING, 
  Specific_Industry_of_Parent STRING, Info_Source STRING, Notes STRING
  )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/FileStore/pharma_tables';

-- COMMAND ----------

LOAD DATA INPATH '/FileStore/tables/pharma.csv' OVERWRITE INTO TABLE pharma;

-- COMMAND ----------

SELECT * FROM pharma

-- COMMAND ----------

-- Creating temporary views from tables created

-- COMMAND ----------

-- Creating the view for clinical trial data
CREATE TEMPORARY VIEW clinicaltrial_view
AS SELECT * FROM clinicaltrial_${hivevar:year} WHERE Id <> 'Id';

-- COMMAND ----------

SELECT * FROM clinicaltrial_view

-- COMMAND ----------

-- Creating the view for pharma data
-- Selecting only the two important coloumns
CREATE TEMPORARY VIEW pharma_view
AS SELECT REGEXP_REPLACE(Company, '["]','') AS Company, REGEXP_REPLACE(Parent_Company, '["]','') AS Parent_Company
FROM pharma WHERE Company != '"Company"';

-- COMMAND ----------

SELECT * FROM pharma_view

-- COMMAND ----------

-- Question 1. To find the number of Distinct studies

-- COMMAND ----------

-- Result
SELECT DISTINCT COUNT(*) AS TotalCount FROM clinicaltrial_view;

-- COMMAND ----------

-- Question 2. To list all the types and frequency

-- COMMAND ----------

-- Result
SELECT Type, COUNT(*) AS Frequency FROM clinicaltrial_view GROUP BY Type ORDER BY Frequency DESC;

-- COMMAND ----------

-- Question 3. Top 5 conditions with their frequencies

-- COMMAND ----------

-- Exploding Conditions from clinical trial data to separate rows
CREATE TEMPORARY VIEW conditions_view
AS 
SELECT *, explode(split(Conditions, ',')) AS explodedConditions 
FROM clinicaltrial_view WHERE Conditions <> '';

-- COMMAND ----------

-- Result
SELECT explodedConditions AS Conditions, COUNT(explodedConditions) AS Frequency
FROM conditions_view
GROUP BY explodedConditions ORDER BY Frequency DESC LIMIT 5;

-- COMMAND ----------

-- Question 4. 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored.

-- COMMAND ----------

CREATE TEMPORARY VIEW sponsors_view
AS
SELECT * FROM clinicaltrial_view
LEFT OUTER JOIN pharma_view
ON pharma_view.Parent_Company = clinicaltrial_view.Sponsor
WHERE clinicaltrial_view.Status <> "Active"
AND pharma_view.Parent_Company IS NULL;

-- COMMAND ----------

-- Result
SELECT Sponsor, COUNT(Sponsor) AS Frequency
FROM sponsors_view
GROUP BY Sponsor ORDER BY Frequency DESC LIMIT 10;

-- COMMAND ----------

-- Question 5. Number of completed studies each month in a given year

-- COMMAND ----------

CREATE TEMPORARY VIEW completion_view
AS
SELECT SUBSTRING(Completion,1,3) AS CompletionMonth, COUNT(Completion) AS Frequency
FROM clinicaltrial_view
WHERE Status=="Completed" AND Completion LIKE '%${hivevar:year}'
GROUP BY CompletionMonth
ORDER BY (unix_timestamp(CompletionMonth,'MMM'),'MM');

-- COMMAND ----------

-- Result
SELECT * from completion_view;

-- COMMAND ----------

-- Visualize

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Extracting the completion months and frequency for visualization
-- MAGIC completion_months = spark.sql("SELECT * FROM completion_view").select("CompletionMonth").rdd.map(lambda row: row["CompletionMonth"]).collect()
-- MAGIC
-- MAGIC frequency = spark.sql("SELECT * FROM completion_view").select("Frequency").rdd.map(lambda row: row["Frequency"]).collect()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC
-- MAGIC fig = plt.figure()
-- MAGIC ax = fig.add_axes([0,0,1.5,1.5])
-- MAGIC ax.bar(completion_months,frequency)
-- MAGIC
-- MAGIC plt.xlabel("Completion Months")
-- MAGIC plt.ylabel("Completed Studies Count")
-- MAGIC plt.title("Barchart of the Completed Studies")
-- MAGIC
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC FURTHER ANALYSIS - Top 10 conditions with the highest number of clinical trials.

-- COMMAND ----------

WITH clinicaltrial_view AS (
  SELECT
    Id,
    Sponsor,
    Status,
    Start,
    Completion,
    Type,
    Submission,
    Conditions,
    Interventions
  FROM clinicaltrial_2021
  WHERE Id != 'Id'
)
SELECT
  Conditions AS Condition,
  COUNT(*) AS Condition_Count
FROM
  clinicaltrial_view
WHERE
  Conditions IS NOT NULL
GROUP BY
  Conditions
ORDER BY
  Condition_Count DESC
LIMIT 10
