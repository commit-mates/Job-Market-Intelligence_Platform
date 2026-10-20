# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///


# COMMAND ----------

# MAGIC %md
# MAGIC ## Jobcliff_Silver_Transformation
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Sahithi Gudivada      |
# MAGIC | **Business Logic By** | Yateesh Chandra       |
# MAGIC | **Load Strategy**     | Merge                |
# MAGIC | **Source**            | Bronze_jobcliff_table           |
# MAGIC | **Target**            | jobsintel.silver.jobcliff_jobs |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Oct 14th 2026| Sahithi Gudivada  | Created Initial Version|

# COMMAND ----------

# DBTITLE 1,Import Requests
from pyspark.sql.functions import col, get_json_object, current_timestamp
from pyspark.sql.types import (
    StringType, IntegerType, DecimalType, TimestampType
)
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Read the Bronze table
jobcliff_bronze_df = spark.table("jobsintel.bronze.raw_jobcliff_jobs")

# COMMAND ----------

# DBTITLE 1,Extract fields from the JSON PAYLOAD
jobcliff_silver_df = jobcliff_bronze_df.select(
        get_json_object(col("PAYLOAD"), '$.job_id').alias("job_id"),
        get_json_object(col("PAYLOAD"), '$.title').alias("title"),
        get_json_object(col("PAYLOAD"), '$.job_type').alias("job_type"),
        get_json_object(col("PAYLOAD"), '$.company_name').alias("company_name"),
        get_json_object(col("PAYLOAD"), '$.location').alias("location"),
        get_json_object(col("PAYLOAD"), '$.created_on').alias("created_on"),
        get_json_object(col("PAYLOAD"), '$.salary').alias("salary"),
        
)
jobcliff_silver_df = (
    jobcliff_silver_df
    .withColumn("BD_CREATE_DT_TM", current_timestamp())
    .withColumn("BD_UPDATE_DT_TM", current_timestamp())
)


# COMMAND ----------

# DBTITLE 1,Load into the target table
targetDF = DeltaTable.forName(spark, "jobsintel.silver.jobcliff_jobs")
(
    targetDF.alias("target")
    .merge(
        jobcliff_silver_df.alias("source"),
        "target.job_id = source.job_id"
    )
    .whenMatchedUpdateAll(
        condition="""
            source.BD_CREATE_DT_TM > target.BD_CREATE_DT_TM
        """
    )
    .whenNotMatchedInsertAll()
    .execute()
)

