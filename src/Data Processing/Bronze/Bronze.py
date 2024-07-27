# Databricks notebook source
from pyspark.sql import functions as F

checkpoint_path = dbutils.jobs.taskValues.get(taskKey="Config", key="checkpoint_path")
kafka_bootstrap_servers = dbutils.jobs.taskValues.get(taskKey="Config", key="kafka_bootstrap_servers")
kafka_topic = dbutils.jobs.taskValues.get(taskKey="Config", key="kafka_topic")
kafka_sasl_mechanism = dbutils.jobs.taskValues.get(taskKey="Config", key="kafka_sasl_mechanism")
kafka_security_protocol = dbutils.jobs.taskValues.get(taskKey="Config", key="kafka_security_protocol")
kafka_sasl_username = dbutils.jobs.taskValues.get(taskKey="Config", key="kafka_sasl_username")
kafka_sasl_password = dbutils.jobs.taskValues.get(taskKey="Config", key="kafka_sasl_password")
kafka_sasl_jaas_config = dbutils.jobs.taskValues.get(taskKey="Config", key="kafka_sasl_jaas_config")
env = dbutils.jobs.taskValues.get(taskKey="Config", key="env")

# COMMAND ----------

query = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .option("kafka.sasl.mechanism", kafka_sasl_mechanism)
    .option("kafka.security.protocol", kafka_security_protocol)
    .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config)
    .option("startingOffsets", "earliest")
    .load()
    .withColumn("key", F.col("key").cast("string"))
    .withColumn("value", F.col("value").cast("string"))
    .withColumn("timestamp", F.col("timestamp").cast("timestamp"))
    .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_path}/checkpoints/bronze")
    .partitionBy("year_month")
    .outputMode("append")
    .trigger(availableNow=True)
    .table(f"{env}.bronze.bronze")
)

query.awaitTermination()
