# Databricks notebook source
# MAGIC %md
# MAGIC ## Create_Silver_dim_companies
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Sep 5th 2026| Sahithi Gudivada  | Creating the Table : dim_companies  |

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS jobsintel.silver.dim_companies (

        COMPANY_KEY BIGINT GENERATED ALWAYS AS IDENTITY
            COMMENT "Surrogate key for the company",

        COMPANY_NAME STRING
            COMMENT "Cleaned and trimmed company name",

        SOURCE_COMPANY_ID STRING
            COMMENT "Company ID from the source job platform",

        SOURCE_SYSTEM STRING
            COMMENT "Source job platform such as APNA, CUTSHORT, UNSTOP, etc."

    )
    USING DELTA
""")
