# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Load_Joinsaarthi_Feed
# MAGIC
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Sahithi Gudivada      |
# MAGIC | **Business Logic By** | Yateesh Chandra       |
# MAGIC | **Load Strategy**     | Append               |
# MAGIC | **Source**            | Extracting Data from API `joinsaarthi` |
# MAGIC | **Target**            | jobsintel.bronze.raw_joinsaarthi_jobs
# MAGIC  |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Aug 14th 2026| Sahithi Gudivada  | Created Initial Version|

# COMMAND ----------

# DBTITLE 1,importing libraries
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Initialize Variables
# Define the URL
URL = "https://joinsaarthi.com/api/jobs?limit=10&offset=10"

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )
}
# Define the Empty List
payload_list = []

# COMMAND ----------

# DBTITLE 1,Define a function to Fetch response from Joinsaarthi Jobs

def fetch_joinsaarthi_jobs(url, num = 1):
    response = requests.get(url + f"{url}?limit=10&offset={offset}",timeout=90)

    if response.status_code != 200:
        raise Exception(f"API Error : {response.status_code}")

    jobs_list = response.json()

    for row in jobs_list:
        payload = {
            "job_id" : row['id'],
            "title" : row['title'],
            "company_id" : row['company']['id'],
            "company_name" : row['company']['name'],
            "created_on" : row['posted_at'],
            "expiry" : row['application_deadline'],
            "min_salary" : row['ctc_min'],
            "max_salary" : row['ctc_max'],
            "experience" : row['yoe'],
            "shift" : row['work_mode'],
            "job_type" :row['job_type'],
            "location" : row['location']
        }
        payload_list.append(json.dumps(payload))
    return payload_list

# COMMAND ----------

# DBTITLE 1,Call the Function
# As per the requirement, we are targetting to capture around 100 jobs
for offset in range(0, 100, 10):
    val = fetch_joinsaarthi_jobs(URL, offset)


# COMMAND ----------

# DBTITLE 1,Reading the data into Dataframe
joinsaarthi_df = spark.createDataFrame(val,schema = ['PAYLOAD']) \
            .withColumn("BD_CREATE_DT_TM", current_timestamp()) \
            .withColumn("BD_UPDATE_DT_TM", current_timestamp())


# COMMAND ----------

# DBTITLE 1,Appending the data into Target Table
joinsaarthi_df.write.mode("append").saveAsTable("jobsintel.bronze.raw_joinsaarthi_jobs")
