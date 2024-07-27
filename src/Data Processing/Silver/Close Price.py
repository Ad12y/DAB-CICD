# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from pyspark.sql.window import Window
from pyspark.sql import functions as F

checkpoint_path = dbutils.jobs.taskValues.get(taskKey="Config", key="checkpoint_path")
env = dbutils.jobs.taskValues.get(taskKey="Config", key="env")

# COMMAND ----------

def type2_upsert(microBatchDF, batch):
    window = Window.partitionBy("Index").orderBy(F.col("Date").desc())
    microBatchDF = (
        microBatchDF.dropDuplicates(["Index", "Date"])
        .withColumn("updated", current_timestamp())
        .withColumn("rank", F.rank().over(window))
        .filter("rank = 1")
    )
    microBatchDF.createOrReplaceTempView("updates")
    sql_query = f"""
        MERGE INTO {env}.silver.close_price_silver AS cps
        USING (
            SELECT
                updates.Index as merge_key,
                updates.*
            FROM
                updates
            UNION ALL
            SELECT
                NULL as merge_key,
                updates.*
            FROM
                updates
                JOIN {env}.silver.close_price_silver AS cps2 ON updates.Index = cps2.Index
            WHERE
                cps2.current = true
        ) staged_updates
        ON cps.Index = staged_updates.merge_key
        WHEN MATCHED AND cps.current = true
        THEN
            UPDATE SET
                current = false,
                end_date = staged_updates.updated
        WHEN NOT MATCHED THEN
            INSERT (
                Index,
                Date,
                Close,
                Adj_Close,
                current,
                effective_date,
                end_date
            ) VALUES (
                staged_updates.Index,
                staged_updates.Date,
                staged_updates.Close,
                staged_updates.Adj_Close,
                true,
                staged_updates.updated,
                NULL
            )
    """

    microBatchDF.sparkSession.sql(sql_query)

# COMMAND ----------

spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {env}.silver.close_price_silver (Index STRING, Date DATE,Close STRING, Adj_Close STRING, current BOOLEAN, effective_date TIMESTAMP, end_date TIMESTAMP)"""
)

# COMMAND ----------

json_schema = "struct<Index: STRING, Date: DATE, Open: STRING, High: STRING, Low: STRING, Close: STRING, `Adj Close`: STRING, Volume: STRING>"


def process_close_price():
    query = (
        spark.readStream.table(f"{env}.bronze.bronze")
        .select(F.from_json(F.col("value"), json_schema).alias("v"))
        .select(
            "v.Index",
            "v.Date",
            "v.Close",
            F.col("v.`Adj Close`").alias("Adj_Close"),
        )
        .writeStream.foreachBatch(type2_upsert)
        .option(
            "checkpointLocation", f"{checkpoint_path}/checkpoints/close_price_silver"
        )
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()


process_close_price()