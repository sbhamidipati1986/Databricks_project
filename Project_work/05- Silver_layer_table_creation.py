# Databricks notebook source
# MAGIC %run ./01-Setup

# COMMAND ----------

class silver_tables_creation():

    def __init__(self):
        config=database_setup(layer='silver')
        self.catalog_name = config.env
        self.db_name = config.db_name
        self.path = config.path
        self.env=config.env

    def create_silver_database(self):
        db_path=f"'{self.path}/{self.env}/silver'"
        spark.sql(f"create catalog if not exists {self.catalog_name}.{self.db_name} managed location {db_path}")

    def user_profile(self):
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.db_name}.user_profile\
        (user_id bigint,
        update_type string,
        timestamp timestamp,
        first_name string,
        last_name string,
        gender string,
        dob string,
        street_address string,
        city string,
        state string,
        zip bigint)""")



