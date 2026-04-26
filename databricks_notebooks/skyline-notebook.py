# Databricks notebook source
# 1. Read the raw Parquet data from Bronze layer
# No config needed because of the External Location we just set up
bronze_path = "gs://skyline-landing-zone-marmaris/bronze/*.parquet"
df_bronze = spark.read.parquet(bronze_path)

# 2. Transformation Logic (Silver Layer)
from pyspark.sql.functions import col, when, current_timestamp

# - Removing rows without CustomerId
# - Dropping duplicates to ensure data quality
# - Adding age_group for business insights
# - Adding a processing timestamp
df_silver = df_bronze \
    .filter(col("CustomerId").isNotNull()) \
    .dropDuplicates(["CustomerId"]) \
    .withColumn("age_group", 
        when(col("Age") < 30, "Young")
        .when((col("Age") >= 30) & (col("Age") < 50), "Middle-Aged")
        .otherwise("Senior")) \
    .withColumn("processed_at", current_timestamp())

# 3. Write the cleaned data to Silver layer in Delta format
# Delta format allows features like Time Travel and ACID transactions
silver_output_path = "gs://skyline-landing-zone-marmaris/silver/bank_churn_silver"

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_output_path)

print(f"Silver Layer successfully created at: {silver_output_path}")
display(df_silver.limit(10))

# COMMAND ----------

from pyspark.sql.functions import avg, count

# Aggregate data to find churn rate per age group
df_gold_age_group = df_silver.groupBy("age_group") \
    .agg(
        count("CustomerId").alias("total_customers"),
        avg("Exited").alias("churn_rate")
    ) \
    .orderBy("churn_rate", ascending=False)

# Save the Gold table (This will be our source for BigQuery and Streamlit)
gold_output_path = "gs://skyline-landing-zone-marmaris/gold/churn_by_age_group"

df_gold_age_group.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_output_path)

print("Gold Layer (Business Insights) is ready!")
display(df_gold_age_group)