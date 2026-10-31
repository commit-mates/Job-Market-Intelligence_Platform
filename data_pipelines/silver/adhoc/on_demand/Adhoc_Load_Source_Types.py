# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## Adhoc_Load_Source_Types
# MAGIC
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Sahithi Gudivada      |
# MAGIC | **Business Logic By** | Yateesh Chandra       |
# MAGIC | **Load Strategy**     | Insert        |
# MAGIC | **Source**            | Manual Insertion of Source Types     |
# MAGIC | **Target**            | jobsintel.silver.dim_source |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Oct 29th 2026| Sahithi Gudivada  | Created Initial Version|

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO jobsintel.silver.dim_source(SOURCE_ID,SOURCE_TYPE,IS_ACTIVE,BD_CREATE_DT,BD_UPDATE_DT) 
# MAGIC VALUES 
# MAGIC (1,'Apna',1,"2026-05-30","2026-05-30"),
# MAGIC (2,'Freshersworld',1,"2026-06-03","2026-06-03"),
# MAGIC (3,'Jobcliff',1,"2026-08-05","2026-08-05"),
# MAGIC (4,"Unstop",1,"2026-08-08","2026-08-08"),
# MAGIC (5,'Joinsaarthi',1,"2026-08-14","2026-08-14"),
# MAGIC (6,'Cutshort',1,"2026-08-15","2026-08-15");
# MAGIC
