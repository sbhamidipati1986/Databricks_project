# Databricks notebook source
# MAGIC %run ./01-Setup

# COMMAND ----------

conf=bronze_setup('dev')

# COMMAND ----------

# MAGIC %md
# MAGIC # **Data discovery**

# COMMAND ----------

users_data=conf.path+'landing_zone/raw/stream_data/'
df=spark.read.format('json').option('inferSchema','true').load(users_data)

# COMMAND ----------

class create_bronze():
    
    def __init__(self,env):
        conf=bronze_setup(env)
    def create_catalog(self):
        catalog_path=f"'{conf.path}project/dbs/dev/'"
        spark.sql(f"CREATE CATALOG  IF NOT EXISTS {conf.catalog_name} managed location {catalog_path}")
    def create_database(self):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {conf.catalog_name}.{conf.db_name}")
    def user_table(self):
        spark.sql(f"""create table if not exists {conf.catalog_name}.{conf.db_name}.user_info\
                  (user_id int,device_id int,mac_address string,registration_timestamp double,load_time timestamp,
                    source_file string)""")
    def gym_info(self):
        spark.sql(f"""create table if not exists {conf.catalog_name}.{conf.db_name}.gym_info\
                  (mac_address string,gym int,login double,logout double,load_time timestamp,
                    source_file string)""")
    def multiple_data(self):
        spark.sql(f"""create table if not exists {conf.catalog_name}.{conf.db_name}.multiple_data\
                  (key string,
                    offset bigint,
                    partition bigint,
                    timestamp double,
                    topic string,
                    value string,
                    load_time timestamp,
                    source_file string)""")
    def run(self):
        self.create_catalog()
        self.create_database()
        self.user_table()
        self.gym_info()
        self.multiple_data()


# COMMAND ----------

bronze_data=create_bronze('dev')
bronze_data.run()

# COMMAND ----------


