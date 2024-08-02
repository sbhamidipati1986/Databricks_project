# Databricks notebook source
# MAGIC %run ./01-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC # **Data discovery**

# COMMAND ----------

class create_bronze():
    
    def __init__(self,env='dev'):
        conf=database_setup(env=env,layer='bronze')
        self.dbname=conf.db_name
        self.catalogname=conf.catalog_name
        self.path=conf.path

    def create_catalog(self):
        catalog_path=f"'{self.path}dbs/dev/{self.dbname}/'"
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
                    value string,
                    topic string,
                    partition bigint,
                    offset bigint,
                    timestamp double,
                    date date, 
                    week_part string, 
                    load_time timestamp,
                    source_file string)
                    PARTITIONED BY (topic, week_part)""")
        
 
            
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
