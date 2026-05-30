# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW CATALOGS;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS jobsintel;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS jobsintel.bronze;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS jobsintel.bronze.jobs_raw (
# MAGIC     message_id STRING,
# MAGIC     source STRING,
# MAGIC     payload STRING,
# MAGIC     bd_create_dt_tm TIMESTAMP,
# MAGIC     bd_update_dt_tm TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  jobsintel.bronze.jobs_raw;
# MAGIC

# COMMAND ----------

import requests
import json
from datetime import datetime
import logging
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


log = logging.getLogger(__name__)
logger = logging.getLogger("job_ingestion")

# Schema for he jobs_raw table
schema = StructType([
            StructField("message_id", StringType(), True),
            StructField("source", StringType(), True),
            StructField("payload", StringType(), True),
            StructField("bd_create_dt_tm", TimestampType(), True),
            StructField("bd_update_dt_tm", TimestampType(), True),
        ])

# Function to ingest data from apnajob jobsite
def ingest_jobs_to_bronze(url: str):

    try:
        logger.info("Starting API request...")

        response = requests.get(url, timeout=30)

        if response.status_code != 200:
            logger.error(f"API failed with status code: {response.status_code}")
            raise Exception(f"API Error: {response.status_code}")

        data = response.json()
        job_list = data["results"]["jobs"]

        payload_records = []
        i = 1
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
                "message_id": str(i),        
                "source": "apna_jobs_api",
                "payload": json.dumps(custom_payload),
                "bd_create_dt_tm": datetime.now(),
                "bd_update_dt_tm": datetime.now()
            })
            i += 1

        job_data_df = spark.createDataFrame(payload_records,schema=schema)
        
                      
      
        # Load data into jobs_raw table
        job_data_df.write.mode("append").saveAsTable("jobsintel.bronze.jobs_raw")
        
        
        logger.info(f"Data successfully written to {table_name}")

        return "Successfully created the jobs_raw table"
    
    except Exception as e:
        logger.exception("Job ingestion failed")
        raise e


# URL for apnajob
url = "https://production.apna.co/user-profile-orchestrator/public/v1/jobs/?department_id=&work_mode=&work_type=&work_shift=&page=1&page_size=25"

df = ingest_jobs_to_bronze(url)


