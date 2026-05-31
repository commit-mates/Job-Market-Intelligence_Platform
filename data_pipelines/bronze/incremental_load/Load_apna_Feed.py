# Databricks notebook source
# MAGIC %md
# MAGIC ## Load_APNA_Feed
# MAGIC
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Sahithi Gudivada      |
# MAGIC | **Business Logic By** | Yateesh Chandra       |
# MAGIC | **Load Strategy**     | Append               |
# MAGIC | **Source**            | Apna API             |
# MAGIC | **Target**            | jobsintel.bronze.jobs_raw |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | May 30th 2026| Sahithi Gudivada  | Created Initial Version|

# COMMAND ----------

# DBTITLE 1,importing libraries
import requests
import json
import logging
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# COMMAND ----------

# DBTITLE 1,Schema for  jobs_raw table
schema = StructType([
            StructField("source", StringType(), True),
            StructField("payload", StringType(), True),
            StructField("bd_create_dt_tm", TimestampType(), True),
            StructField("bd_update_dt_tm", TimestampType(), True),
        ])

# COMMAND ----------

URL = "https://production.apna.co/user-profile-orchestrator/public/v1/jobs/?department_id=&work_mode=&work_type=&work_shift=&page=1&page_size=25"
log = logging.getLogger(__name__)
logger = logging.getLogger("job_ingestion")

# COMMAND ----------

# Function to ingest data from apnajob jobsite
def ingest_jobs_to_bronze(url: str):

        logger.info("Starting API request...")
        response = requests.get(url, timeout=30)

        if response.status_code != 200:
            logger.error(f"API failed with status code: {response.status_code}")
            raise Exception(f"API Error: {response.status_code}")

        data = response.json()
        job_list = data["results"]["jobs"]

        payload_records = []

        for job in job_list:
            custom_payload = {
                        "job_id"         :  job["id"],
                        "title"          :  job["title"],
                        "company_name"   :  job["organization"]["name"],
                        "department"     :  job["department"]["name"],
                        "expiry_date"    :  job["expiry"],
                        "address"        :  job["address"].get("area", "") + ", " + job["address"]["city"]["json_data"]["state"],
                        "qualification"  :  job["education"],
                        "job_type"       :  job["type"],
                        "job_category"   :  job["category"],
                        "job_salary"     :  str(job["min_salary"]) + " - " + str(job["max_salary"]),
                        "job_experience" :  str(job["min_experience"]) + " - " + str(job["max_experience"]),
                        "job_shift"      :  job["shift"],
                    }
            
            payload_records.append({      
                "source": "apna_jobs_api",
                "payload": json.dumps(custom_payload),
                "bd_create_dt_tm": datetime.now(),
                "bd_update_dt_tm": datetime.now()
            })

        job_data_df = spark.createDataFrame(payload_records,schema=schema)

        # Load data into jobs_raw table
        job_data_df.write.mode("append").saveAsTable("jobsintel.bronze.jobs_raw")
        logger.info(f"Data successfully written to jobs_raw_table")

try:
    ingest_jobs_to_bronze(URL)
except Exception as e:
    logger.exception("Job ingestion failed")
    raise
