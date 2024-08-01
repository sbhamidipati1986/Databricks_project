# Databricks notebook source
# MAGIC %run ./01-Setup

# COMMAND ----------

from pyspark.sql import functions as fu
from pyspark.sql.types import IntegerType

class date_load():
    def __init__(self,env='dev'):
        config=bronze_setup(env=env)
        self.dbname=config.db_name
        self.catalogname=config.catalog_name
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
        data_path=config.path+"landing_zone/raw/datelookup"
        checkpoint_path=config.path+"metadata/checkpoint/datelookup"
        schema_location=config.path+"metadata/schema/datelookup"
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
            df_datewritestream.trigger(availableNow=True).toTable(f"{config.catalog_name}.{config.db_name}.date_lookup")
        else:
            df_datewritestream.trigger(processingTime=processing_time).toTable(f"{config.catalog_name}.{config.db_name}.date_lookup")

# COMMAND ----------

from pyspark.sql import functions as fu
class bronze_ingestion():
    
    




    def user_data_ingestion(self, once=True, processing_time="5 seconds"):
        schema="user_id int,device_id int,mac_address string,registration_timestamp double"
        data_path=config.path+"landing_zone/raw/users"
        checkpoint_path=config.path+"metadata/checkpoint/users"
        schema_path=config.path+"metadata/schema/users"
        df=spark.readStream.\
            format("cloudFiles")\
                .option("maxFilesPerTrigger", 1)\
                .option("cloudFiles.format", "csv")\
                .option("cloudFiles.schemaLocation", schema_path)\
                .schema(schema)\
                .option("header", "true")\
                .load(f"{data_path}")\
                .withColumn("load_time",fu.current_timestamp())\
                .withColumn("source_file",fu.input_file_name())


        write_stream = df.writeStream.format("delta").\
            option("checkpointLocation",checkpoint_path).\
            outputMode("append").\
            queryName("users_ingestion_stream")
        
        if once == True:
            write_stream.trigger(availableNow=True).toTable(config.catalog_name+"."+config.db_name+".user_info").awaitTermination()
        else:
            write_stream.trigger(processingTime=processing_time).toTable(config.catalog_name+"."+config.db_name+".user_info").awaitTermination()

    def gym_data_insertion(self, once=True, processing_time="5 seconds"):
        schema="mac_address string,gym int,login int,logout int"
        data_path=config.path+"landing_zone/raw/gym_logins"
        checkpoint_path=config.path+"metadata/checkpoint/gym"
        schema_path=config.path+"metadata/schema/gym"
        df_gym=spark.readStream.\
            format("cloudFiles").\
            schema(schema).\
            option("maxFilesPerTrigger", 1).\
            option("cloudFiles.format","csv").\
            option("cloudFiles.schemaLocation",schema_path).\
            option("header","true").\
            load(data_path).\
            withColumn("load_time",fu.current_timestamp()).\
            withColumn("source_file",fu.input_file_name())
        
        gym_write_stream=df_gym.writeStream.format("delta").\
                        option("checkpointLocation",checkpoint_path).\
                        outputMode("append").\
                        queryName("gym_active_stream") 
        if once==True:
            gym_write_stream.trigger(availableNow=True).\
                toTable(config.catalog_name+"."+config.db_name+".gym_info").awaitTermination()
        else:
            gym_write_stream.trigger(processingTime=processing_time).\
                to_table(config.catalog_name+"."+config.db_name+".gym_info").awaitTermination()

    def multiple_data_insertion(self,once=True,processing_time='5 seconds'):
        schema="""key string,
                offset bigint,
                partition bigint,
                timestamp double,
                topic string,
                value string"""
        data_path=config.path+"landing_zone/raw/stream_data"
        checkpoint_path=config.path+"metadata/checkpoint/multiple_data"
        schema_path=config.path+"metadata/schema/multiple_data"
        dt = spark.table(f"{config.catalog_name}.{config.db_name}.date_lookup").select("*")

        df_multiple_data=spark.readStream.\
            format("cloudFiles").\
            option("cloudFiles.format","json").\
            option("cloudFiles.schemaLocation",schema_path).\
            option("maxFilesPerTrigger", 1).\
            schema(schema).\
            load(data_path).\
            withColumn("load_time",fu.current_timestamp()).\
            withColumn("source_file",fu.input_file_name()).\
            join(fu.broadcast(dt),(fu.col("timestamp")/1000).cast("timestamp")==fu.col("date"),"left")
      
        df_write_multiple_data =df_multiple_data.writeStream.format("delta").\
                option("checkpointLocation",checkpoint_path).\
                outputMode("append").\
                queryName("multiple_data_stream")
        
        if once == True:
            df_write_multiple_data.trigger(availableNow=True).\
                toTable(config.catalog_name+"."+config.db_name+".multiple_data").awaitTermination()
        else:
            df_multiple_data.trigger(processingTime=processing_time).\
                                toTable(config.catalog_name+"."+config.db_name+".multiple_data").awaitTermination()
                
    def run(self,once=True,processing_time='5 seconds'):
        
        self.user_data_ingestion(once,processing_time)
        self.gym_data_insertion(once,processing_time)
        self.multiple_data_insertion(once,processing_time)






                


                        
                


    


# COMMAND ----------

inst = bronze_ingestion()
inst.multiple_data_insertion()

# COMMAND ----------

inst=date_load()
inst.create_date_lookup()
inst.date_loader()
