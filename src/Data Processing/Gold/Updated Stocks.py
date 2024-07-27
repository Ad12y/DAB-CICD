# Databricks notebook source
env = dbutils.jobs.taskValues.get(taskKey="Config", key="env")

# COMMAND ----------

spark.sql(
    f"""
    CREATE VIEW IF NOT EXISTS {env}.gold.updated_stocks_gold AS (
        SELECT 
            'Index' AS Index,
            Date,
            Close,
            Adj_Close,
            Latest_Close,
            Latest_Adj_Close,
            CAST(Latest_Close AS DOUBLE) - CAST(Close AS DOUBLE) AS Close_Diff
        FROM 
            {env}.silver.stocks_close_silver
    )"""
)