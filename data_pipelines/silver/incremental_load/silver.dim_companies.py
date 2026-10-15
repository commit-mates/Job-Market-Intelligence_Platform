# Databricks notebook source
# MAGIC %md
# MAGIC ## Load_silver.dim_companies
# MAGIC
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Sahithi Gudivada      |
# MAGIC | **Business Logic By** | Yateesh Chandra       |
# MAGIC | **Load Strategy**     | Append                |
# MAGIC | **Source**            | Bronze                |
# MAGIC | **Target**            | jobsintel.silver.Load_silver.dim_companies |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Aug 20th 2026| Sahithi Gudivada  | Created Initial Version|

# COMMAND ----------

# DBTITLE 1,import libraries
from pyspark.sql.functions import col, trim, row_number,lit
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Read bronze tables
apna_df = spark.table("jobsintel.bronze.raw_apna_jobs")
cutshort_df = spark.table("jobsintel.bronze.raw_cutshort_jobs")
freshersworld_df = spark.table("jobsintel.bronze.raw_freshersworld_jobs")
jobcliff_df = spark.table("jobsintel.bronze.raw_jobcliff_jobs")
joinsaarthi_df = spark.table("jobsintel.bronze.raw_joinsaarthi_jobs ")
unstop_df = spark.table("jobsintel.bronze.raw_unstop_jobs ")

# COMMAND ----------

# DBTITLE 0,selecting necessary columns from bronze tables
from pyspark.sql.functions import col, trim, lit, get_json_object

apna_companies = apna_df.select(
    get_json_object(col("PAYLOAD"), "$.company_id").alias("source_company_id"),
    trim(get_json_object(col("PAYLOAD"), "$.company_name")).alias("company_name")
).withColumn("source_system", lit("APNA"))


cutshort_companies = cutshort_df.select(
    get_json_object(col("PAYLOAD"), "$.company_id").alias("source_company_id"),
    trim(get_json_object(col("PAYLOAD"), "$.company_name")).alias("company_name")
).withColumn("source_system", lit("CUTSHORT"))


freshersworld_companies = freshersworld_df.select(
    get_json_object(col("PAYLOAD"), "$.company_id").alias("source_company_id"),
    trim(get_json_object(col("PAYLOAD"), "$.company_name")).alias("company_name")
).withColumn("source_system", lit("FRESHERSWORLD"))


jobcliff_companies = jobcliff_df.select(
    get_json_object(col("PAYLOAD"), "$.company_id").alias("source_company_id"),
    trim(get_json_object(col("PAYLOAD"), "$.company_name")).alias("company_name")
).withColumn("source_system", lit("JOBCLIFF"))


joinsaarthi_companies = joinsaarthi_df.select(
    get_json_object(col("PAYLOAD"), "$.company_id").alias("source_company_id"),
    trim(get_json_object(col("PAYLOAD"), "$.company_name")).alias("company_name")
).withColumn("source_system", lit("JOINSAARTHI"))


unstop_companies = unstop_df.select(
    get_json_object(col("PAYLOAD"), "$.company_id").alias("source_company_id"),
    trim(get_json_object(col("PAYLOAD"), "$.company_name")).alias("company_name")
).withColumn("source_system", lit("UNSTOP"))

# COMMAND ----------

all_companies = (
    apna_companies
    .unionByName(cutshort_companies)
    .unionByName(freshersworld_companies)
    .unionByName(jobcliff_companies)
    .unionByName(joinsaarthi_companies)
    .unionByName(unstop_companies)
)

all_companies = all_companies.dropDuplicates(
    ["source_company_id", "source_system"]
)

existing_companies = spark.table(
    "jobsintel.silver.dim_companies"
)

new_companies = all_companies.join(
                    existing_companies,
                    on=["source_company_id", "source_system"],
                    how="left_anti"
)

new_companies = new_companies.select(
                    "company_name",
                    "source_company_id",
                    "source_system"
                )

new_companies.write.mode("append").saveAsTable("jobsintel.silver.dim_companies")
