# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Load_Cutshort_Feed
# MAGIC
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Sahithi Gudivada      |
# MAGIC | **Business Logic By** | Yateesh Chandra       |
# MAGIC | **Load Strategy**     | Append               |
# MAGIC | **Source**            | Scraping Cutshort        |
# MAGIC | **Target**            | jobsintel.bronze.raw_cutshort_jobs |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Aug 15th 2026| Sahithi Gudivada  | Created Initial Version|

# COMMAND ----------

# DBTITLE 1,Import Libraries
import requests
import json
from bs4 import BeautifulSoup
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# DBTITLE 1,Initialize Variables
# Define the URL
URL = "https://cutshort.io/jobs/python-jobs"

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )
}

# Define the Empty List 
payload_list = []
job_list = []
session = requests.Session()
session.headers.update(headers)

# COMMAND ----------

def extract_job_data(soup):

    extracted_jobs = []
    for script in soup.find_all("script"):
        script_type = script.get("type", "").lower()
        if script_type not in ("application/json","application/ld+json"):
            continue

        try:
            script_data = json.loads(
                script.string or script.get_text()
            )
            jobs = find_job_objects(script_data)
            extracted_jobs.extend(jobs)
        except (json.JSONDecodeError, TypeError):
            continue

    return extracted_jobs

# COMMAND ----------

# DBTITLE 1,Define a function to get list of Job URLs
def fetch_cutshort_job_list(url):
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    
    # Extract the Reponse
    soup = BeautifulSoup(response.text, "html.parser")
    scripts = soup.find_all("script", type="application/ld+json")
    for script in scripts:
        try:
            data = json.loads(script.string or script.get_text())
            if data.get("@type") == "ItemList":
                for item in data.get("itemListElement", []):
                    job_list.append(item.get("url"))
        except json.JSONDecodeError:
            continue

    return job_list

# COMMAND ----------

# DBTITLE 1,Use the function

jobs = fetch_cutshort_job_list(URL)


# COMMAND ----------

def find_job_objects(data):
    jobs = []
    if isinstance(data, dict):
        if (data.get("publicUrl") and data.get("headline")):
            jobs.append(data)
        for value in data.values():
            jobs.extend(find_job_objects(value))

    elif isinstance(data, list):
        for item in data:
            jobs.extend(find_job_objects(item))

    return jobs

# COMMAND ----------

# DBTITLE 1,Define a function to Fetch response from Cutshort web
def get_cutshort_job(jobs):

    for url in jobs:
        if not url:
            continue
        try:
            response = session.get(url, timeout=15)
            response.raise_for_status()

            soup = BeautifulSoup(response.text,"html.parser")
            extracted_jobs = extract_job_data(soup)

            if extracted_jobs:
                for job in extracted_jobs:
                    payload_list.append(json.dumps(job,ensure_ascii=False))
            else:
                print(f"No embedded job JSON found: {url}")

        except requests.RequestException as e:
            print(f"Failed to fetch {url}: {e}")

    return payload_list

# COMMAND ----------

# DBTITLE 1,Call the Function

# Extract 50 jobs
val = get_cutshort_job(job_list)

# COMMAND ----------

# DBTITLE 1,Reading the data into Dataframe
cutshort_df = spark.createDataFrame(val,schema = ['PAYLOAD']) \
            .withColumn("BD_CREATE_DT_TM", current_timestamp()) \
            .withColumn("BD_UPDATE_DT_TM", current_timestamp())
     

# COMMAND ----------

# DBTITLE 1,Appending the data into Target Table
cutshort_df.write.mode("append").saveAsTable("jobsintel.bronze.raw_cutshort_jobs")