# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## Load_silver.dim_location
# MAGIC
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Sahithi Gudivada      |
# MAGIC | **Business Logic By** | Yateesh Chandra       |
# MAGIC | **Load Strategy**     | Append                |
# MAGIC | **Source**            | Silver                |
# MAGIC | **Target**            | jobsintel.silver.Load_silver.dim_location |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | Nov 02th 2026| Sahithi Gudivada  | Created Initial Version|

# COMMAND ----------

# DBTITLE 1,import statements
from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window
from pyspark.sql import functions as sf

# COMMAND ----------

# DBTITLE 1,Read bronze tables
apna_df = spark.table("jobsintel.silver.apna_jobs")
freshersworld_df = spark.table("jobsintel.silver.fresherworld_jobs")
job_cliff_df = spark.table("jobsintel.silver.jobcliff_jobs")
unstop_df = spark.table("jobsintel.silver.unstop_jobs")
joinsaarthi_df = spark.table("jobsintel.silver.joinsaarthi_jobs")

# COMMAND ----------

# DBTITLE 1,checking columns
target_columns = [
    "CITY",
    "STATE",
    "PINCODE",
    "LATITUDE",
    "LONGITUDE",
    "CREATED_ON"
]
def align_columns(df):

    if "STATE" not in df.columns:
        df = df.withColumn("STATE", lit(None).cast("string"))

    if "PINCODE" not in df.columns:
        df = df.withColumn("PINCODE", lit(None).cast("string"))

    if "LATITUDE" not in df.columns:
        df = df.withColumn("LATITUDE", lit(None).cast("double"))

    if "longitude" not in df.columns:
        df = df.withColumn("LONGITUDE", lit(None).cast("double"))

    if "CREATED_ON" not in df.columns:
        df = df.withColumn("CREATED_ON", current_timestamp())
    else:
        df = df.withColumn(
            "CREATED_ON",
            coalesce(
                expr("try_cast(CREATED_ON AS TIMESTAMP)"),
                current_timestamp()
            )
    )

    return df.select(*target_columns)

# COMMAND ----------

# DBTITLE 1,exploding the city column
def clean_city(df):

    df = df.withColumn(
        "CITY",
        when(
            trim(col("CITY")).startswith("["),
            from_json(col("CITY"), ArrayType(StringType()))
        ).otherwise(
            split(col("CITY"), ",")
        )
    )\
    .withColumn(
        "CITY",
        explode_outer(col("CITY"))
    )\
    .withColumn(
        "CITY",
        trim(col("CITY"))
    )

    return df

# COMMAND ----------

# DBTITLE 1,Apply city cleaning to all source DataFrames
apna_df = clean_city(apna_df)
freshersworld_df = clean_city(freshersworld_df)
job_cliff_df = clean_city(job_cliff_df)
unstop_df = clean_city(unstop_df)
joinsaarthi_df = clean_city(joinsaarthi_df)

# COMMAND ----------

apna_data_df = align_columns(apna_df)
freshersworld_data_df = align_columns(freshersworld_df)
job_cliff_data_df = align_columns(job_cliff_df)
unstop_data_df = align_columns(unstop_df)
joinsaarthi_data_df = align_columns(joinsaarthi_df)

# COMMAND ----------

apna_site_silver_df = apna_data_df.select(
                        col("CITY"),
                        col("STATE"),
                        col("PINCODE"),
                        col("LATITUDE"),
                        col("LONGITUDE"),
                        col("CREATED_ON")
                    ) 
freshersworld_site_silver_df = freshersworld_data_df.select(
                                col("CITY"),
                                col("STATE"),
                                col("PINCODE"),
                                col("LATITUDE"),
                                col("LONGITUDE"),
                                col("CREATED_ON")
                            ) 
job_cliff_site_silver_df = job_cliff_data_df.select(
                                col("CITY"),
                                col("STATE"),
                                col("PINCODE"),
                                col("LATITUDE"),
                                col("LONGITUDE"),
                                col("CREATED_ON")
                            )
unstop_site_silver_df = unstop_data_df.select(
                            col("CITY"),
                            col("STATE"),
                            col("PINCODE"),
                            col("LATITUDE"),
                            col("LONGITUDE"),
                            col("CREATED_ON")
                        )
joinsaarthi_city_df = joinsaarthi_data_df\
                            .withColumn("CITY",sf.explode_outer(split(col("CITY"), ",")))\
                            .withColumn("CITY",trim(col("CITY")))

joinsaarthi_site_silver_df = joinsaarthi_city_df.select(
                                col("CITY"),
                                col("STATE"),
                                col("PINCODE"),
                                col("LATITUDE"),
                                col("LONGITUDE"),
                                col("CREATED_ON")
                            ) 

# COMMAND ----------

all_locations_df = (
    apna_site_silver_df
    .unionByName(freshersworld_site_silver_df)
    .unionByName(job_cliff_site_silver_df)
    .unionByName(unstop_site_silver_df)
    .unionByName(joinsaarthi_site_silver_df)
)

all_locations_df = all_locations_df.filter(
    col("CITY").isNotNull() & (trim(col("CITY")) != "")
)
all_locations = all_locations_df.dropDuplicates(["CITY", "PINCODE"])
existing_locations = spark.table("jobsintel.silver.site_profile")
new_locations_jnr = all_locations.join(
                    existing_locations.select("CITY", "PINCODE"),
                    on=["CITY","PINCODE"],
                    how="left_anti"
                )
new_locations_site_nbr= new_locations_jnr.withColumn(
                        "SITE_NBR",
                        concat(
                            date_format(
                                coalesce(col("CREATED_ON"), current_date()),
                                "yyMMdd"
                            ),
                            lpad(
                                row_number().over(Window.orderBy("CITY")).cast("string"),
                                2,
                                "0"
                            )
                        ).cast("long")
                    )\
                   .withColumn("BD_CREATE_DT_TM", current_timestamp()) \
                   .withColumn("BD_UPDATE_DT_TM", current_timestamp())
final_new_locations = new_locations_site_nbr.select(
                    "CITY",
                    "STATE",
                    "PINCODE",
                    "LATITUDE",
                    "LONGITUDE",
                    "SITE_NBR",
                    "BD_CREATE_DT_TM",
                    "BD_UPDATE_DT_TM"

                )

# COMMAND ----------

final_new_locations.write.mode("append").saveAsTable("jobsintel.silver.site_profile")