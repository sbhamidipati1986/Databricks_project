# Databricks notebook source
dbutils.widgets.dropdown("env",defaultValue = "dev",choices=['dev','qa','prod'])

# COMMAND ----------

dbutils.notebook.exit(dbutils.widgets.get("env"))
