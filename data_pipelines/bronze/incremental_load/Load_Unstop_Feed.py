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
# MAGIC | **Source**            | Extracting Data from API jobcliff  |
# MAGIC | **Target**            | jobsintel.bronze.raw_unstop_jobs
# MAGIC  |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Aug 8th 2026| Sahithi Gudivada  | Created Initial Version|

# COMMAND ----------

# DBTITLE 1,importing libraries
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Initialize Variables
# Define the URL
URL = "https://unstop.com/api/public/opportunity/search-result"

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

# DBTITLE 1,Define a function to Fetch response from APNA Jobs
def fetch_unstop_jobs(url, num = 1):
    for page in range(1, 7):
        params = {
            "opportunity": "jobs",
            "page": num,
            "per_page": 18,
            "sortBy": "",
            "orderBy": "",
            "filter_condition": "",
            "undefined": "true"
    }
    response = requests.get(URL, params=params,  headers=headers, timeout=90)

    if response.status_code != 200:
        raise Exception(f"API Error : {response.status_code}")

    jobs_list = response.json()["data"]["data"]

    for row in jobs_list:
        payload = {
            "job_id" : row['id'],
            "title" : row['title'],
            "company_name" : row['organisation']['name'],
            "created_on" : row['regnRequirements']['start_regn_dt'],
            "min_salary" : row["jobDetail"].get("min_salary"),
            "max_salary" : row['jobDetail'].get('max_salary'),
            "min_experience" : row['jobDetail']['min_experience'],
            "max_experience" : row['jobDetail']['max_experience'],
            "expiry" : row['regnRequirements']['remain_days'],
            "job_type" :row['jobDetail']['timing'],
            "location": ", ".join(loc["city"] for loc in row["locations"])
        }
        payload_list.append(json.dumps(payload))
    return payload_list

# COMMAND ----------

# DBTITLE 1,Call the Function
# As per the requirement, we are targetting to capture around 100 jobs
for num in range(1,9):
    val = fetch_unstop_jobs(URL, num)


# COMMAND ----------

# DBTITLE 1,Reading the data into Dataframe
unstop_df = spark.createDataFrame(val,schema = ['PAYLOAD']) \
            .withColumn("BD_CREATE_DT_TM", current_timestamp()) \
            .withColumn("BD_UPDATE_DT_TM", current_timestamp())


# COMMAND ----------

# DBTITLE 1,Appending the data into Target Table
unstop_df.write.mode("append").saveAsTable("jobsintel.bronze.raw_unstop_jobs")
