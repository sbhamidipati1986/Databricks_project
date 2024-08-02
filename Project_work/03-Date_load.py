# Databricks notebook source
# MAGIC %run ./01-Setup

# COMMAND ----------

from pyspark.sql import functions as fu
from pyspark.sql.types import IntegerType

class date_load():
    def __init__(self,env='dev',db_name='bronze'):
        config=database_setup(env=env)
        self.dbname=db_name
        self.catalogname=config.catalog_name
        self.path=config.path

    def create_date_lookup(self):
        spark.sql(f"""Drop  TABLE IF  EXISTS {self.catalogname}.{self.dbname}.date_lookup""")
        spark.sql(f"""CREATE  TABLE IF NOT EXISTS {self.catalogname}.{self.dbname}.date_lookup\
                (date date, 
                week int, 
                year int, 
                month int, 
                dayofweek int, 
                dayofmonth int, 
                dayofyear int, 
                week_part string)
                """) 
        
    def date_loader(self,over=True,processing_time='5 seconds'):
        self.create_date_lookup()
        data_path=self.path+"landing_zone/raw/datelookup"
        checkpoint_path=self.path+"metadata/checkpoint/datelookup"
        schema_location=self.path+"metadata/schema/datelookup"
        schema="""date date, 
                    week string, 
                    year string, 
                    month string, 
                    dayofweek string, 
                    dayofmonth string, 
                    dayofyear string,  
                    week_part string"""
        df_datestream=spark.readStream.format("cloudFiles")\
            .schema(schema)\
            .option("cloudFiles.format","json")\
            .option("cloudFiles.inferSchema", "true")\
            .option("cloudFiles.schemaLocation",schema_location)\
            .option("maxFilesPerTrigger", 1)\
            .load(data_path)\
            .withColumn("week",fu.col('week').cast(IntegerType()))\
            .withColumn("year",fu.col('year').cast(IntegerType()))\
            .withColumn("month",fu.col('month').cast(IntegerType()))\
            .withColumn("dayofweek",fu.col('dayofweek').cast(IntegerType()))\
            .withColumn("dayofmonth",fu.col('dayofmonth').cast(IntegerType()))\
            .withColumn("dayofyear",fu.col('dayofyear').cast(IntegerType()))
    
        
        df_datewritestream=df_datestream.writeStream.format("delta").\
                            outputMode("append").\
                            option("checkpointLocation",checkpoint_path)
                            
        if over==True:
            df_datewritestream.trigger(availableNow=True).toTable(f"{self.catalogname}.{self.dbname}.date_lookup")
        else:
            df_datewritestream.trigger(processingTime=processing_time).toTable(f"{self.catalogname}.{self.dbname}.date_lookup")

# COMMAND ----------

inst=date_load()
inst.date_loader()
