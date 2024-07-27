# Databricks notebook source
# MAGIC %sql 
# MAGIC truncate table close_price_silver

# COMMAND ----------

# MAGIC %sql 
# MAGIC truncate table bronze

# COMMAND ----------

# MAGIC %sql 
# MAGIC truncate table stocks_silver

# COMMAND ----------

# MAGIC %sql 
# MAGIC truncate table current_close_silver

# COMMAND ----------

# MAGIC %sql 
# MAGIC truncate table stocks_close_silver

# COMMAND ----------

# Define the path to the directory you want to delete
directory_to_delete = "abfss://unity-catalog-storage@dbstorage4xlafmlg5yunq.dfs.core.windows.net/500647211230238/checkpoints/"

# Delete the directory recursively
dbutils.fs.rm(directory_to_delete, recurse=True)
print(f"Deleted directory {directory_to_delete}")