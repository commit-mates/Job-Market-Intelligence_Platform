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
# MAGIC | **Source**            | Bronze_cutshort_table           |
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
        get_json_object(col("PAYLOAD"), "$.job_id").alias("job_id"),
        get_json_object(col("PAYLOAD"), '$.title').alias("title"),
        get_json_object(col("PAYLOAD"), '$.job_url').alias("job_url"),
        get_json_object(col("PAYLOAD"),'$.company_name').alias("company_name"),
        get_json_object(col("PAYLOAD"), '$.company_url').alias("company_url"),
        get_json_object(col("PAYLOAD"), '$.location').alias("location"),
        get_json_object(col("PAYLOAD"), '$.experience').alias("experience"),
        get_json_object(col("PAYLOAD"), '$.salary').alias("salary"),
        get_json_object(col("PAYLOAD"), '$.job_desc').alias("job_desc"),
        get_json_object(col("PAYLOAD"), '$.skills').alias("skills")
)
cutshort_silver_df = (
    cutshort_silver_df
    .withColumn("BD_CREATE_DT_TM", current_timestamp())
    .withColumn("BD_UPDATE_DT_TM", current_timestamp())
)


# COMMAND ----------

targetDF = DeltaTable.forName(spark, "jobsintel.silver.cutshort_jobs")
(
    targetDF.alias("target")
    .merge(
        cutshort_silver_df.alias("source"),
        "target.job_id = source.job_id"
    )
    .whenMatchedUpdateAll(
        condition="""
            source.BD_UPDATE_DT_TM > target.BD_UPDATE_DT_TM and 
            source.title != target.title or
            source.job_url != target.job_url or
            source.company_name != target.company_name or
            source.company_url != target.company_url or
            source.location != target.location or
            source.experience != target.experience or
            source.salary != target.salary or
            source.job_desc != target.job_desc or
            source.skills != target.skills 
        """
    )
    .whenNotMatchedInsertAll()
    .execute()
)

