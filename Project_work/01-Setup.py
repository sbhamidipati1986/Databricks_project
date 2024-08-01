# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create external location if not exists raw_data
# MAGIC url 'abfss://project@dbricksproject.dfs.core.windows.net/'
# MAGIC with (credential `project_cred`);

# COMMAND ----------

class bronze_setup():
    def __init__(self,env='dev',layer='bronze'):
        self.path = spark.sql("describe external location raw_data").select('url').collect()[0][0]
        self.catalog_name=env
        self.db_name=layer

        
