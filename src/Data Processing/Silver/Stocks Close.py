# Databricks notebook source
from pyspark.sql import functions as F
checkpoint_path = dbutils.jobs.taskValues.get(taskKey="Config", key="checkpoint_path")
env = dbutils.jobs.taskValues.get(taskKey="Config", key="env")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {env}.silver.stocks_close_silver
(Index STRING, Date DATE,Close STRING, Adj_Close STRING, Latest_Close STRING, Latest_Adj_Close STRING)""")

# COMMAND ----------

from pyspark.sql import functions as F

# Define the DataFrame for current_close_silver
current_close_df = spark.sql(
    f"SELECT `Index` as Index_cc, Close as Close_cc, Adj_Close as Adj_Close_cc FROM {env}.silver.stocks_close_silver"
)

# Create the streaming query
query = (
    spark.readStream.table(f"{env}.silver.stocks_silver")
    .join(current_close_df, F.col("Index_cc") == F.col("Index"))
    .select(
        "Index",
        "Date",
        "Close",
        "Adj_Close",
        F.col("Close_cc").alias("Latest_Close"),
        F.col("Adj_Close_cc").alias("Latest_Adj_Close"),
    )
    .writeStream.option(
        "checkpointLocation", f"{checkpoint_path}/checkpoints/stocks_close_silver"
    )
    .trigger(availableNow=True)
    .table(f"{env}.silver.stocks_close_silver")
)

# Await termination of the query
query.awaitTermination()