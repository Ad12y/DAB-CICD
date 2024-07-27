# Databricks notebook source
from pyspark.sql import functions as F

checkpoint_path = dbutils.jobs.taskValues.get(taskKey="Config", key="checkpoint_path")
env = dbutils.jobs.taskValues.get(taskKey="Config", key="env")

# COMMAND ----------

json_schema = "struct<Index: STRING, Date: DATE, Open: STRING, High: STRING, Low: STRING, Close: STRING, `Adj Close`: STRING, Volume: STRING>"

query = (
    spark.readStream.table(f"{env}.bronze.bronze")
    .select(F.from_json(F.col("value"), json_schema).alias("v"))
    .select(
        "v.Index",
        "v.Date",
        "v.Open",
        "v.High",
        "v.Low",
        "v.Close",
        F.col("v.`Adj Close`").alias("Adj_Close"),
        "v.Volume",
    )
    .writeStream.option(
        "checkpointLocation", f"{checkpoint_path}/checkpoints/stocks_silver"
    )
    .trigger(availableNow=True)
    .table(f"{env}.silver.stocks_silver")
)

query.awaitTermination()