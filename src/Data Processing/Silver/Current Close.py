# Databricks notebook source
env = dbutils.jobs.taskValues.get(taskKey="Config", key="env")

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TABLE {env}.silver.current_close_silver AS
SELECT Index, Close, Adj_Close
FROM {env}.silver.close_price_silver
WHERE current = TRUE;"""
)