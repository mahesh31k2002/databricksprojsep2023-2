# Databricks notebook source
# MAGIC %md
# MAGIC ### In order to do for all tables use the below code to apply date only format for all parquet files

# COMMAND ----------

table_name = [i.name.split('/')[0] for i in dbutils.fs.ls("/mnt/bronze/SalesLT/")]


# COMMAND ----------

table_name

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

for i in table_name:
    path = '/mnt/bronze/SalesLT/'+i+'/'+i+'.parquet'
    df = spark.read.format('parquet').load(path)
    column = df.columns
    for col in column:
        if "Date" in col or "date" in col:
            df = df.withColumn(col,date_format(from_utc_timestamp(df[col].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))
    output_path = '/mnt/silver/SalesLT/'+i+'/'
    df.write.format('delta').mode('overwrite').save(output_path) 
            

# COMMAND ----------

display(df)

# COMMAND ----------


