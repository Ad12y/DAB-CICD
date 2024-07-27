# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, DateType

checkpoint_path = dbutils.jobs.taskValues.get(taskKey="Config", key="checkpoint_path")
kafka_bootstrap_servers = dbutils.jobs.taskValues.get(taskKey="Config", key="kafka_bootstrap_servers")
kafka_topic = dbutils.jobs.taskValues.get(taskKey="Config", key="kafka_topic")
kafka_sasl_mechanism = dbutils.jobs.taskValues.get(taskKey="Config", key="kafka_sasl_mechanism")
kafka_security_protocol = dbutils.jobs.taskValues.get(taskKey="Config", key="kafka_security_protocol")
kafka_sasl_username = dbutils.jobs.taskValues.get(taskKey="Config", key="kafka_sasl_username")
kafka_sasl_password = dbutils.jobs.taskValues.get(taskKey="Config", key="kafka_sasl_password")
kafka_sasl_jaas_config = dbutils.jobs.taskValues.get(taskKey="Config", key="kafka_sasl_jaas_config")

# COMMAND ----------

csv_path = "dbfs:/FileStore/Data"

# Define the schema
schema = StructType(
    [
        StructField("Index", StringType(), True),
        StructField("Date", DateType(), True),
        StructField("Open", StringType(), True),
        StructField("High", StringType(), True),
        StructField("Low", StringType(), True),
        StructField("Close", StringType(), True),
        StructField("Adj Close", StringType(), True),
        StructField("Volume", StringType(), True),
    ]
)

query = (
    spark.readStream.format("csv")
    .schema(schema)
    .option("header", "true")
    .option("dateFormat", "yyyy-MM-dd")
    .load(csv_path)
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("topic", kafka_topic)
    .option("kafka.sasl.mechanism", kafka_sasl_mechanism)
    .option("kafka.security.protocol", kafka_security_protocol)
    .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config)
    .trigger(availableNow=True)
    .option("checkpointLocation", f"{checkpoint_path}/checkpoints/producer/")
    .start()
)

query.awaitTermination()