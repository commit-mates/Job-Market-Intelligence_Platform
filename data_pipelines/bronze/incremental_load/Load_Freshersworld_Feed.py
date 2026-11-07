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
# MAGIC | **Source**            | Scraping Freshersworld         |
# MAGIC | **Target**            | jobsintel.bronze.raw_freshersworld_jobs |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Jun 3rd 2026| Sahithi Gudivada  | Created Initial Version|
# MAGIC | Nov 11th 2026| Sahithi Gudivada  | Migrated to type script compatible version|

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
URL = "https://www.freshersworld.com/jobs/category/it-software-job-vacancies"

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
job_payloads = []

# COMMAND ----------

def get_freshersworld_job_urls(url, num):
    response = requests.get(f"{url}?&offset={num}", headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    jobs = []
    for card in soup.select(
        "div.job-container, div.job-card, div.latest-jobs, div.job-new-title"
    ):
        job_url = card.get("job_display_url")
        if job_url:
            jobs.append(job_url)

    return jobs

# COMMAND ----------

def remove_key_recursive(data, key_to_remove):

    if isinstance(data, dict):
        return {
            key: remove_key_recursive(value, key_to_remove)
            for key, value in data.items()
            if key != key_to_remove
        }

    elif isinstance(data, list):
        return [
            remove_key_recursive(item, key_to_remove)
            for item in data
        ]

    return data

# COMMAND ----------

def fetch_jobposting_data(url):
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    scripts = soup.find_all("script", type="application/ld+json")

    url_payload_list = []

    for script in scripts:
        try:
            data = json.loads(script.string or script.get_text())   
            if isinstance(data, dict):
                job_postings = [data]
                if "@graph" in data:
                    job_postings = data["@graph"]
            elif isinstance(data, list):
                job_postings = data
            else:
                continue

            for job in job_postings:
                if not isinstance(job, dict):
                    continue
                if job.get("@type") != "JobPosting":
                    continue
                job_id = str(job.get("identifier", {}).get("value", ""))

                if not job_id or job_id in seen_job_ids:
                    continue
                seen_job_ids.add(job_id)
                job = remove_key_recursive(job, "@context")
                job = remove_key_recursive(job, "@type")

                url_payload_list.append(json.dumps(job, ensure_ascii=False))
        except (json.JSONDecodeError, TypeError):
            continue

    return url_payload_list

# COMMAND ----------

job_url_set = set()
seen_job_ids = set()

for num in range(0, 101, 20):
    job_urls = get_freshersworld_job_urls(URL, num)
    job_url_set.update(job_urls)
job_list = list(job_url_set)

for job_url in job_list:
    try:
        job_payloads.extend(fetch_jobposting_data(job_url))

    except requests.RequestException as e:
            print(f"Failed URL: {job_url} | Error: {e}")


# COMMAND ----------

val = [(payload,) for payload in job_payloads]

# COMMAND ----------

# DBTITLE 1,Reading the data into Dataframe
freshersworld_df = spark.createDataFrame(val,schema = ['PAYLOAD']) \
            .withColumn("BD_CREATE_DT_TM", current_timestamp()) \
            .withColumn("BD_UPDATE_DT_TM", current_timestamp())


# COMMAND ----------

# DBTITLE 1,Appending the data into Target Table
freshersworld_df.write.mode("append").saveAsTable("jobsintel.bronze.raw_freshersworld_jobs")