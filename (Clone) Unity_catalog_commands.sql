-- Databricks notebook source
https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-privileges/privileges#--privilege-types-by-securable-object-in-unity-catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **# Create External location**

-- COMMAND ----------

create external location stream_input
url 'abfss://inbound@dbstreamingdata.dfs.core.windows.net/'
with (credential `azuredbricksproject`)

-- COMMAND ----------

create external location stream_output
url 'abfss://outbound@dbstreamingdata.dfs.core.windows.net/'
with (credential `azuredbricksproject`)

-- COMMAND ----------

create external location streamingdb  
url "abfss://dbs@dbstreamingdata.dfs.core.windows.net/"
with (credential `azuredbricksproject`)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # **Create Catalog**

-- COMMAND ----------

drop database if exists dev.dev_db;
drop catalog dev;
create  catalog dev managed location 'abfss://inbound@saindiadbricksunityadls.dfs.core.windows.net/dev';

-- COMMAND ----------

drop database if exists qa_db.default;
drop catalog qa_db ;
create catalog if not exists qa managed location 'abfss://inbound@saindiadbricksunityadls.dfs.core.windows.net/qa';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # **Create Database**

-- COMMAND ----------

create database if not exists dev.dev_db;

-- COMMAND ----------

create database if not exists qa.qa_db;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **# Create Volume**

-- COMMAND ----------

create volume if not exists qa.qa_db.qa_db_vol;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.format('csv').option("header",True).option("inferSchema",True).csv('abfss://inbound@saindiadbricksunityadls.dfs.core.windows.net/qa/__unitystorage/catalogs/f1f34037-ca32-4860-90d0-3bb74685c0e5/volumes/5fb956ea-7c5b-4f63-a05c-8fef7440f7a7/unemp_data.csv')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC l1 = df.dtypes

-- COMMAND ----------

-- MAGIC %python
-- MAGIC l2=[]
-- MAGIC for i in l1:
-- MAGIC     l2.append(i[0]+" "+i[1]+",")
-- MAGIC l2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for i in l2:
-- MAGIC   print(i) 

-- COMMAND ----------

use catalog qa;
use qa_db;
describe Volume qa_db_vol

-- COMMAND ----------

SELECT * FROM qa.qa_db.unemp_data;

-- COMMAND ----------

-- Set the catalog to 'dev'
USE CATALOG dev;

DROP  DATABASE IF  EXISTS dev_db; 
-- Create database if it does not exist
CREATE DATABASE IF NOT EXISTS dev_db MANAGED LOCATION  'abfss://databases@saindiadbricksunityadls.dfs.core.windows.net/dev';;

-- Drop table if it exists
DROP TABLE IF EXISTS dev_db.unemployed;

-- Create table with Delta format, partitioned and optionally bucketed
CREATE EXTERNAL TABLE dev_db.unemployed (
    Education_Level STRING,
    Line_Number INT,
    Year INT,
    Month STRING,
    State STRING,
    Labor_Force String,
    Employed INT,
    Unemployed INT,
    Industry STRING,
    Gender STRING,
    Date_Inserted STRING,
    Aggregation_Level STRING,
    Data_Accuracy STRING,
    UnEmployed_Rate_Percentage String,
    Min_Salary_USD INT,
    Max_Salary_USD INT,
    dense_rank INT
)
USING DELTA
PARTITIONED BY (State)
location 'abfss://databases@saindiadbricksunityadls.dfs.core.windows.net/dev/db';



-- COMMAND ----------

use catalog dev;
use dev_db;
insert into unemployed select Education_Level,
Line_Number,
Year,
Month,
State,
Labor_Force,
Employed,
Unemployed,
Industry,
Gender,
Date_Inserted,
Aggregation_Level,
Data_Accuracy,
UnEmployed_Rate_Percentage,
Min_Salary_USD,
Max_Salary_USD,
dense_rank from qa.qa_db.unemp_data;

-- COMMAND ----------

use catalog dev;
use dev_db;
select * from unemployed where Year is null;
-- delete  from unemployed where Year is null;

-- COMMAND ----------

DESCRIBE qa.qa_db.unemp_data;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC l2 = spark.sql("DESCRIBE qa.qa_db.unemp_data;").collect()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC l3=[]
-- MAGIC for i in l2:
-- MAGIC     l3.append(i.col_name)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for i in l3:
-- MAGIC     print(i+",")

-- COMMAND ----------

describe history dev_db.unemployed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # **Copy into Ingestion**

-- COMMAND ----------

create catalog streaming managed location 'abfss://dbs@dbstreamingdata.dfs.core.windows.net/' ;
create database streaming.streaming_db;
create table streaming.streaming_db.output;

-- COMMAND ----------

set spark.databricks.delta.copyInto.formatCheck.enabled =false

-- COMMAND ----------

copy into streaming.streaming_db.output from "abfss://outbound@dbstreamingdata.dfs.core.windows.net/" 
fileformat = delta
format_options("header"="true","inferschema"="true","mergeSchema"="true")
copy_options("mergeSchema"="true");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # **Batch Streaming ingestion**

-- COMMAND ----------

drop table if exists  streaming.streaming_db.output;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def ingest():
-- MAGIC     df=(spark.readStream\
-- MAGIC         .format("csv")\
-- MAGIC         .option("inferschema","true")\
-- MAGIC         .option("header","true")\
-- MAGIC         .option("mergeschema","true")\
-- MAGIC         .load("abfss://inbound@dbstreamingdata.dfs.core.windows.net/")
-- MAGIC     )
-- MAGIC     display(df.printSchema())
-- MAGIC     writedf =   (df.writeStream.format("delta") \
-- MAGIC         .option("checkpointLocation", "abfss://outbound@dbstreamingdata.dfs.core.windows.net/checkpoints") \
-- MAGIC         .option("mergeSchema", "true") \
-- MAGIC         .outputMode("append") \
-- MAGIC         .trigger(availableNow = True) \
-- MAGIC         .toTable("streaming.streaming_db.output")
-- MAGIC     )

-- COMMAND ----------

set spark.sql.streaming.schemaInference=true

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ingest()

-- COMMAND ----------

drop table if exists  streaming.streaming_db.output;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # **Continous streaming didn't work for megeschema**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def ingest():
-- MAGIC     # Read streaming data from a directory
-- MAGIC     df = (spark.readStream
-- MAGIC           .format("csv")
-- MAGIC           .option("header", "true")
-- MAGIC           .option("inferSchema", "true")
-- MAGIC           .option("schemaLocation", "abfss://outbound@dbstreamingdata.dfs.core.windows.net/checkpoints/schemas")
-- MAGIC           .load("abfss://inbound@dbstreamingdata.dfs.core.windows.net/")
-- MAGIC     )
-- MAGIC     
-- MAGIC     # Display the schema
-- MAGIC     df.printSchema()
-- MAGIC
-- MAGIC     # Write the streaming DataFrame to a Delta table with a processing time trigger
-- MAGIC     query = (df.writeStream
-- MAGIC              .format("delta")
-- MAGIC              .option("checkpointLocation", "abfss://outbound@dbstreamingdata.dfs.core.windows.net/checkpoints")
-- MAGIC              .option("mergeSchema", "true")
-- MAGIC              .outputMode("append")
-- MAGIC              .trigger(processingTime="1 minute")  # Set trigger interval to 1 minute
-- MAGIC              .toTable("streaming.streaming_db.output")
-- MAGIC              )  # Start the streaming query
-- MAGIC
-- MAGIC     # Return the query to monitor its status
-- MAGIC     return query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # **Autoloader**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def cloud_ingest_autoloader():
-- MAGIC     # Read streaming data from a directory using Autoloader
-- MAGIC     df = (spark.readStream
-- MAGIC           .format("cloudFiles")
-- MAGIC           .option("cloudFiles.format", "csv")  # Ensure correct format is specified
-- MAGIC           .option("header", "true")
-- MAGIC           .option("cloudFiles.inferSchema", "true")  # Use correct option for schema inference
-- MAGIC           .option("cloudFiles.schemaLocation", "abfss://outbound@dbstreamingdata.dfs.core.windows.net/checkpoints/schemas")  # Schema location for evolution
-- MAGIC           .load("abfss://inbound@dbstreamingdata.dfs.core.windows.net/")
-- MAGIC     )
-- MAGIC     
-- MAGIC     # Write the streaming DataFrame to a Delta table with a processing time trigger
-- MAGIC     query = (df.writeStream
-- MAGIC              .format("delta")
-- MAGIC              .option("checkpointLocation", "abfss://outbound@dbstreamingdata.dfs.core.windows.net/checkpoints")  # Checkpoint location
-- MAGIC              .option("mergeSchema", "true")
-- MAGIC              .outputMode("append")
-- MAGIC              .trigger(processingTime="1 minute")  # Set trigger interval to 1 minute
-- MAGIC              .table("streaming.streaming_db.output")  # Ensure the table exists or is created
-- MAGIC     )
-- MAGIC     

-- COMMAND ----------

-- MAGIC %python
-- MAGIC cloud_ingest_autoloader()
