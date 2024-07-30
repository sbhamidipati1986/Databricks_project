# Databricks notebook source
env=dbutils.notebook.get(./Enter_enviourment)

# COMMAND ----------

def create_location(env):
    spark.sql("create external location env url 'abfss://project@dbricksprojectlakehouse.dfs.core.windows.net/dbs/'\+env+'_db'\
    with (credential 'project_cred')")

# COMMAND ----------

def create_catalog()
