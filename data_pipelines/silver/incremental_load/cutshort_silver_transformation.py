# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## Cutshort_Silver_Trransformation
# MAGIC
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Sahithi Gudivada      |
# MAGIC | **Business Logic By** | Yateesh Chandra       |
# MAGIC | **Load Strategy**     | Merge              |
# MAGIC | **Source**            | jobsintel.bronze.raw_cutshort_jobs          |
# MAGIC | **Target**            | jobsintel.silver.cutshort_jobs |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Oct 18th 2026| Sahithi Gudivada  | Created Initial Version|

# COMMAND ----------

from pyspark.sql.functions import col, get_json_object, current_timestamp
from pyspark.sql.types import (
    StringType, IntegerType, DecimalType, TimestampType
)
from delta.tables import DeltaTable

# COMMAND ----------

cutshort_bronze_df = spark.table("jobsintel.bronze.raw_cutshort_jobs")

# COMMAND ----------

cutshort_silver_df = cutshort_bronze_df.select(
        get_json_object(col("PAYLOAD"), "$.job_id").alias("JOB_ID"),
        get_json_object(col("PAYLOAD"), '$.title').alias("TITLE"),
        get_json_object(col("PAYLOAD"), '$.job_url').alias("JOB_URL"),
        get_json_object(col("PAYLOAD"),'$.company_name').alias("COMPANY_NAME"),
        get_json_object(col("PAYLOAD"), '$.company_url').alias("COMPANY_URl"),
        get_json_object(col("PAYLOAD"), '$.location').alias("LOCATION"),
        get_json_object(col("PAYLOAD"), '$.experience').alias("EXPERIENCE"),
        get_json_object(col("PAYLOAD"), '$.salary').alias("SALARY"),
        get_json_object(col("PAYLOAD"), '$.job_desc').alias("JOB_DESC"),
        get_json_object(col("PAYLOAD"), '$.skills').alias("SKILLS"),
        col("BD_CREATE_DT_TM"),
        col("BD_UPDATE_DT_TM")
)

# COMMAND ----------

targetDF = DeltaTable.forName(spark, "jobsintel.silver.cutshort_jobs")
(
    targetDF.alias("TARGET")
    .merge(
        cutshort_silver_df.alias("SOURCE"),
        "TARGET.JOB_ID = SOURCE.JOB_ID"
    )
    .whenMatchedUpdateAll(
        condition="""
            SOURCE.BD_UPDATE_DT_TM > TARGET.BD_UPDATE_DT_TM AND 
            SOURCE.TITLE != TARGET.TITLE OR
            SOURCE.JOB_URL != TARGET.JOB_URL OR
            SOURCE.COMPANY_NAME != TARGET.COMPANY_NAME OR
            SOURCE.COMPANY_URL != TARGET.COMPANY_URL OR
            SOURCE.LOCATION != TARGET.LOCATION OR
            SOURCE.EXPERIENCE != TARGET.EXPERIENCE OR
            SOURCE.SALARY != TARGET.SALARY OR
            SOURCE.JOB_DESC != TARGET.JOB_DESC OR
            SOURCE.SKILLS != TARGET.SKILLS 
        """
    )
    .whenNotMatchedInsertAll()
    .execute()
)

