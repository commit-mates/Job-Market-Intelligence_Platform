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

# COMMAND ----------

# DBTITLE 1,Define a function to get list of Job URLs

def fetch_cutshort_job_list(url):
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()

    # Extract the Reponse
    soup = BeautifulSoup(response.text, "html.parser")

    for card in soup.select("div.sc-8f06d440-0.ciLipX"):
        title = card.select_one("a.dIKnux")
        href = title.get("href", "") if title else ""
        job_list.append(href)
    return job_list

# COMMAND ----------

# DBTITLE 1,Use the function

jobs = fetch_cutshort_job_list(URL)

# COMMAND ----------

# DBTITLE 1,Define a function to Fetch response from Cutshort web
def get_cutshort_job(jobs):
    for url in jobs:
        if url :
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            # Extract the Reponse
            soup = BeautifulSoup(response.text, "html.parser")

            title = soup.select_one("h1.bMdtwg")
            company = soup.select_one(".cfBBKA a") if soup.select_one(".cfBBKA a") else soup.select_one(".fSEpsP a")
            company_url = soup.select_one(".cfBBKA a") if soup.select_one(".cfBBKA a") else soup.select_one(".fSEpsP a")
            job_url = url
            job_id = url.rsplit("-", 1)[-1]
            details = [
                    s.get_text(strip=True)
                    for s in soup.select("div.hZbKiR")
                ]
            experience = details[0] if details else None
            salary = details[1] if details else None
            location = details[2] if details else None
            skills = [
                    s.get_text(strip=True)
                    for s in soup.select("div.sc-c52be3c1-15 span")
                ]
            job_desc = soup.select_one(".sc-54ec2143-2")

            payload = {
                "job_id": job_id if job_id else None,
                "title": title.get_text(strip=True) if title else None,
                "job_url": job_url if job_url else None,
                "company_name": company.get_text(strip=True) if company else None,
                "company_url": company_url.get("href") if company else None,
                "location": location if location else None,
                "experience": experience if experience else None,
                "salary": salary if salary else None,
                "job_desc" : job_desc.get_text(strip = True, separator = "\n") if job_desc else None,
                "skills" : ", ".join(skills) if skills else None
            }
            payload_list.append(json.dumps(payload, ensure_ascii=False))
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
