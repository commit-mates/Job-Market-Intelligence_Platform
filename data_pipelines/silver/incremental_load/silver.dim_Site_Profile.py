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
from pyspark.sql.functions import col, trim,lit,split,from_json,when, explode_outer,concat, date_format,row_number
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
cutshort_df = spark.table("jobsintel.silver.cutshort_jobs ")

# COMMAND ----------

# DBTITLE 1,checking columns
target_columns = [
    "CITY",
    "STATE",
    "PINCODE",
    "LATITUDE",
    "LONGITUDE"
]
def align_columns(df):

    if "state" not in df.columns:
        df = df.withColumn("state", lit(None).cast("string"))

    if "pincode" not in df.columns:
        df = df.withColumn("pincode", lit(None).cast("string"))

    if "latitude" not in df.columns:
        df = df.withColumn("latitude", lit(None).cast("double"))

    if "longitude" not in df.columns:
        df = df.withColumn("longitude", lit(None).cast("double"))

    # if "area_id" not in df.columns:
    #     df = df.withColumn("area_id", lit(None).cast("string"))

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
    )

    df = df.withColumn(
        "CITY",
        explode_outer(col("CITY"))
    )

    df = df.withColumn(
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
cutshort_df = clean_city(cutshort_df)

# COMMAND ----------

apna_site_silver_df = apna_df.select(
                        col("CITY"),
                        col("STATE"),
                        col("PINCODE"),
                        col("LATITUDE"),
                        col("LONGITUDE")
                    ) 
apna_data_df = align_columns(apna_df)
freshersworld_site_silver_df = freshersworld_df.select(
                                col("CITY"),
                                col("STATE"),
                                col("PINCODE"),
                                col("LATITUDE"),
                                col("LONGITUDE")
                            ) 
freshersworld_data_df = align_columns(freshersworld_df)
job_cliff_site_silver_df = job_cliff_df.select(
                                col("CITY"),
                                col("STATE"),
                                col("PINCODE"),
                                col("LATITUDE"),
                                col("LONGITUDE")
                            )
job_cliff_data_df = align_columns(job_cliff_df) 
unstop_site_silver_df = unstop_df.select(
                            col("CITY"),
                            col("STATE"),
                            col("PINCODE"),
                            col("LATITUDE"),
                            col("LONGITUDE")
                        )
unstop_data_df = align_columns(unstop_df) 
joinsaarthi_city_df = joinsaarthi_df.withColumn(
                            "CITY",
                            from_json(col("CITY"), ArrayType(StringType()))
                        )

joinsaarthi_city_df = joinsaarthi_df.withColumn(
                        "CITY",
                        sf.explode_outer(col("CITY"))
                    )
joinsaarthi_site_silver_df = joinsaarthi_df.select(
                                col("CITY"),
                                col("STATE"),
                                col("PINCODE"),
                                col("LATITUDE"),
                                col("LONGITUDE")
                            ) 
joinsaarthi_data_df = align_columns(joinsaarthi_df)
cutshort_city_df = cutshort_df.withColumn(
                        "CITY",
                        sf.explode_outer(split(col("LOCATION"), ","))
                    )
cutshort_city_df = cutshort_city_df.withColumn(
                        "CITY",
                        trim(col("CITY"))
                    )
cutshort_site_silver_df = cutshort_city_df.select(
                            col("CITY"),
                            col("STATE"),
                            col("PINCODE"),
                            col("LATITUDE"),
                            col("LONGITUDE")
                    )
cutshort_data_df = align_columns(cutshort_df)

# COMMAND ----------

all_locations_df = (
    apna_data_df
    .unionByName(freshersworld_data_df)
    .unionByName(job_cliff_data_df)
    .unionByName(unstop_data_df)
    .unionByName(joinsaarthi_data_df)
    .unionByName(cutshort_data_df)
)
all_locations_df = all_locations_df.filter(
    col("CITY").isNotNull() & (trim(col("CITY")) != "")
)
all_locations = all_locations_df.dropDuplicates(["CITY", "PINCODE"])
existing_locations = spark.table("jobsintel.silver.site_profile")
new_locations = all_locations.join(
                    existing_locations.select("CITY", "PINCODE"),
                    on=["CITY"],
                    how="left_anti"
               )
new_locations = new_locations.withColumn(
                    "SITE_NBR",
                    concat(
                        date_format(col("CREATED_ON"), "yyMMdd"),
                        row_number().over(Window.orderBy("CITY"))
                    )
)
new_locations = new_locations.select(
                    "CITY",
                    "STATE",
                    "PINCODE",
                    "LATITUDE",
                    "LONGITUDE"
                )

# COMMAND ----------

new_locations.write.mode("append").saveAsTable("jobsintel.silver.site_profile")
