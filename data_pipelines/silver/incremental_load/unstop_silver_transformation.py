# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## Unstop_Silver_Transformation
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Sahithi Gudivada      |
# MAGIC | **Business Logic By** | Yateesh Chandra       |
# MAGIC | **Load Strategy**     | Merge                |
# MAGIC | **Source**            | Bronze_unstop_table           |
# MAGIC | **Target**            | jobsintel.silver.unstop_jobs |
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
unstop_bronze_df = spark.table("jobsintel.bronze.raw_unstop_jobs")

# COMMAND ----------

# DBTITLE 1,Extract fields from the JSON PAYLOAD
unstop_silver_df = unstop_bronze_df.select(
            get_json_object(col("PAYLOAD"), "$.job_id").alias("job_id"),
            get_json_object(col("PAYLOAD"), "$.title").alias("title"),
            get_json_object(col("PAYLOAD"), "$.company_name").alias("company_name"),
            get_json_object(col("PAYLOAD"), "$.created_on").alias("created_on"),
            get_json_object(col("PAYLOAD"), "$.expiry").alias("expiry"),
            get_json_object(col("PAYLOAD"), "$.min_salary").alias("min_salary"),
            get_json_object(col("PAYLOAD"), "$.max_salary").alias("max_salary"),
            get_json_object(col("PAYLOAD"), "$.min_experience").alias("min_experience"),
            get_json_object(col("PAYLOAD"), "$.max_experience").alias("max_experience"),
            get_json_object(col("PAYLOAD"), "$.shift").alias("shift"),
            get_json_object(col("PAYLOAD"), "$.job_type").alias("job_type"),
            get_json_object(col("PAYLOAD"), "$.location").alias("location"),
            col("BD_CREATE_DT_TM"),
            col("BD_UPDATE_DT_TM")
) 

# COMMAND ----------

# DBTITLE 1,Load into the target table
targetDF = DeltaTable.forName(spark, "jobsintel.silver.unstop_jobs")
(
    targetDF.alias("target")
    .merge(
        unstop_silver_df.alias("source"),
        "target.job_id = source.job_id"
    )
    .whenMatchedUpdateAll(
        condition="""
            source.BD_UPDATE_DT_TM > target.BD_UPDATE_DT_TM and 
            source.title != target.title or
            source.company_name != target.company_name or
            source.created_on != target.created_on or
            source.expiry != target.expiry or
            source.min_salary != target.min_salary or
            source.max_salary != target.max_salary or
            source.min_experience != target.min_experience or
            source.max_experience != target.max_experience or 
            source.shift != target.shift or 
            source.job_type != target.job_type or
            source.location != target.location 
        """
    )
    .whenNotMatchedInsertAll()
    .execute()
)

