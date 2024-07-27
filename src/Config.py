# Databricks notebook source
dbutils.widgets.text("env", "")

# COMMAND ----------

# Chrckpoint configurations 
env = dbutils.widgets.get("env")
spark.sql(f""" USE CATALOG {env} """)
checkpoints = 'checkpoints_' + env
checkpoint_path = spark.sql(f"""DESCRIBE EXTERNAL LOCATION {checkpoints}""").collect()[0]['url']

# Kafka configurations
kafka_bootstrap_servers = "resolved-cobra-6658-us1-kafka.upstash.io:9092"
kafka_topic = "Stocks"
kafka_sasl_mechanism = "SCRAM-SHA-256"
kafka_security_protocol = "SASL_SSL"
kafka_sasl_username = dbutils.secrets.get(scope='kafka', key='kafka-sasl-username')
kafka_sasl_password = dbutils.secrets.get(scope='kafka', key='kafka-sasl-password')

# Construct the JAAS config string
kafka_sasl_jaas_config = (
    f'org.apache.kafka.common.security.scram.ScramLoginModule required '
    f'username="{kafka_sasl_username}" password="{kafka_sasl_password}";'
)

# COMMAND ----------

# Schema creation
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

dbutils.jobs.taskValues.set("checkpoint_path",checkpoint_path)
dbutils.jobs.taskValues.set("kafka_bootstrap_servers",kafka_bootstrap_servers)
dbutils.jobs.taskValues.set("kafka_topic",kafka_topic)
dbutils.jobs.taskValues.set("kafka_sasl_mechanism",kafka_sasl_mechanism)
dbutils.jobs.taskValues.set("kafka_security_protocol",kafka_security_protocol)
dbutils.jobs.taskValues.set("kafka_sasl_username",kafka_sasl_username)
dbutils.jobs.taskValues.set("kafka_sasl_password",kafka_sasl_password)
dbutils.jobs.taskValues.set("kafka_sasl_jaas_config",kafka_sasl_jaas_config)
dbutils.jobs.taskValues.set("env", env)
