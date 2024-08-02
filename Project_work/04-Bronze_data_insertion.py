# Databricks notebook source
# MAGIC %run ./01-Setup

# COMMAND ----------

from pyspark.sql import functions as fu
class bronze_ingestion():
    def __init__(self,env='dev'):
        config=database_setup(env=env)
        self.db_name=config.db_name
        self.catalog_name=config.catalog_name
        self.path=config.path

    def user_data_ingestion(self, once=True, processing_time="5 seconds"):
        schema="user_id int,device_id int,mac_address string,registration_timestamp double"
        data_path=self.path+"landing_zone/raw/users"
        checkpoint_path=self.path+"metadata/checkpoint/bronze/users"
        schema_path=self.path+"metadata/schema/bronze/users"
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
            write_stream.trigger(availableNow=True).toTable(self.catalog_name+"."+self.db_name+".user_info").awaitTermination()
        else:
            write_stream.trigger(processingTime=processing_time).toTable(self.catalog_name+"."+self.db_name+".user_info").awaitTermination()

    def gym_data_insertion(self, once=True, processing_time="5 seconds"):
        schema="mac_address string,gym int,login int,logout int"
        data_path=self.path+"landing_zone/raw/gym_logins"
        checkpoint_path=self.path+"metadata/checkpoint/bronze/gym/"
        schema_path=self.path+"metadata/schema/bronze/gym"
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
                toTable(self.catalog_name+"."+self.db_name+".gym_info").awaitTermination()
        else:
            gym_write_stream.trigger(processingTime=processing_time).\
                to_table(self.catalog_name+"."+self.db_name+".gym_info").awaitTermination()

    def multiple_data_insertion(self,once=True,processing_time='5 seconds'):
        schema="""key string,
                    value string,
                    topic string,
                    partition bigint,
                    offset bigint,
                    timestamp double"""
        data_path=self.path+"landing_zone/raw/stream_data"
        checkpoint_path=self.path+"metadata/checkpoint/bronze/multiple_data/"
        schema_path=self.path+"metadata/schema/bronze/multiple_data/"
        dt = spark.table(f"{self.catalog_name}.{self.db_name}.date_lookup").select(fu.col("date"),fu.col("week_part"))

        df_multiple_data=spark.readStream.\
            format("cloudFiles").\
            option("cloudFiles.format","json").\
            option("cloudFiles.schemaLocation",schema_path).\
            option("maxFilesPerTrigger", 1).\
            schema(schema).\
            load(data_path).\
            withColumn("load_time",fu.current_timestamp()).\
            withColumn("source_file",fu.input_file_name()).\
            join(fu.broadcast(dt),fu.to_date(fu.col("timestamp").cast("timestamp"))==fu.col("date"),"left")
      
        df_write_multiple_data =df_multiple_data.writeStream.format("delta").\
                option("checkpointLocation",checkpoint_path).\
                outputMode("append").\
                queryName("multiple_data_stream")
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p1")
        
        if once == True:
            df_write_multiple_data.trigger(availableNow=True).\
                toTable(self.catalog_name+"."+self.db_name+".multiple_data").awaitTermination()
        else:
            df_multiple_data.trigger(processingTime=processing_time).\
                                toTable(self.catalog_name+"."+self.db_name+".multiple_data").awaitTermination()
                
    def run(self,once=True,processing_time='5 seconds'):
        
        self.user_data_ingestion(once,processing_time)
        self.gym_data_insertion(once,processing_time)
        self.multiple_data_insertion(once,processing_time)



# COMMAND ----------

inst = bronze_ingestion()
inst.run()
