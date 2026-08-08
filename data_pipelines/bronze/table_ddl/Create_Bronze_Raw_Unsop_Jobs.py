# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
spark.sql("""
    CREATE TABLE IF NOT EXISTS jobsintel.bronze.raw_unstop_jobs (
        MESSAGE_ID BIGINT GENERATED ALWAYS AS IDENTITY COMMENT "The identity of the message entering the table sourcing from JobCliff",
        PAYLOAD STRING  COMMENT "The Json format of the data called from the website",
        BD_CREATE_DT_TM TIMESTAMP COMMENT "timestamp loaded",
        BD_UPDATE_DT_TM TIMESTAMP COMMENT "timestamp updated"
    )
    USING DELTA
""")
