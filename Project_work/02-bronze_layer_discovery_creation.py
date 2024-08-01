# Databricks notebook source
# MAGIC %run ./01-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC # **Data discovery**

# COMMAND ----------

users_data=conf.path+'landing_zone/raw/stream_data/'
df=spark.read.format('json').option('inferSchema','true').load(users_data)

# COMMAND ----------

class create_bronze():
    
    def __init__(self,env='dev'):
        conf=bronze_setup(env=env)
        self.dbname=conf.db_name
        self.catalogname=conf.catalog_name

    def create_catalog(self):
        catalog_path=f"'{conf.path}dbs/dev/'"
        spark.sql(f"CREATE CATALOG  IF NOT EXISTS {self.catalogname} managed location {catalog_path}")

    def create_database(self):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalogname}.{self.dbname}")

    def user_table(self):
        spark.sql(f"""create table if not exists {self.catalogname}.{self.dbname}.user_info\
                  (user_id int,device_id int,mac_address string,registration_timestamp double,load_time timestamp,
                    source_file string)""")
        
    def gym_info(self):
        spark.sql(f"""create table if not exists {self.catalogname}.{self.dbname}.gym_info\
                  (mac_address string,gym int,login int,logout int,load_time timestamp,
                    source_file string)""")
        
    def multiple_data(self):
        spark.sql(f"""create table if not exists {self.catalogname}.{self.dbname}.multiple_data\
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
    
    def cleanup(self):
        
        spark.sql(f"""drop table if Exists  {self.catalogname}.{self.dbname}.multiple_data""")
        spark.sql(f"drop table if Exists  {self.catalogname}.{self.dbname}.gym_info")
        spark.sql(f"drop table if Exists  {self.catalogname}.{self.dbname}.user_info")
        spark.sql(f"DROP DATABASE IF EXISTS {self.catalogname}.{self.dbname} Cascade")
        spark.sql(f"DROP CATALOG IF EXISTS {self.catalogname} Cascade")




# COMMAND ----------


bronze_data=create_bronze()

bronze_data.run()
