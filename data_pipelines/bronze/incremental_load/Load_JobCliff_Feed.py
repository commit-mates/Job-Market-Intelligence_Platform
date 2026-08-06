# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Load_Freshersworld_Feed
# MAGIC
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Sahithi Gudivada      |
# MAGIC | **Business Logic By** | Yateesh Chandra       |
# MAGIC | **Load Strategy**     | Append               |
# MAGIC | **Source**            | Scraping JobCliff         |
# MAGIC | **Target**            | jobsintel.bronze.raw_Jobcliff_jobs
# MAGIC  |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Aug 5th 2026| Sahithi Gudivada  | Created Initial Version|

# COMMAND ----------

# DBTITLE 1,importing libraries
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Initialize Variables
# Define the URL
URL = "https://api.jobcliff.com/api/employees/jobs/jobs/listing"
# Define the Empty List
payload_list = []

# COMMAND ----------

# DBTITLE 1,Define a function to Fetch response from APNA Jobs
def fetch_JobCliff_jobs(url, num = 1):
    response = requests.get(url + f"?page={num}&limit=12", timeout = 90)

    if response.status_code != 200:
        raise Exception(f"API Error : {response.status_code}")

    jobs_list = response.json()["data"]

    for row in jobs_list:
        payload = {
            "job_id" : row['id'],
            "title" : row['title'],
            "company_name" : row['company'],
            "created_on" : row['postedDate'],
            "salary" : row['salary'],
            "job-type" :row['type'],
            "location" : row['workingLocation']
        }
        payload_list.append(json.dumps(payload))
    return payload_list

# COMMAND ----------

# DBTITLE 1,Call the Function
# As per the requirement, we are targetting to capture around 100 jobs
for num in range(1,9):
    val = fetch_JobCliff_jobs(URL, num)


# COMMAND ----------

# DBTITLE 1,Reading the data into Dataframe
JobCliff_df = spark.createDataFrame(val,schema = ['PAYLOAD']) \
            .withColumn("BD_CREATE_DT_TM", current_timestamp()) \
            .withColumn("BD_UPDATE_DT_TM", current_timestamp())


# COMMAND ----------

# DBTITLE 1,Appending the data into Target Table
JobCliff_df.write.mode("append").saveAsTable("jobsintel.bronze.raw_JobCliff_jobs")
