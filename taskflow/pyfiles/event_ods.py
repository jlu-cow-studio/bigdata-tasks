#!/usr/bin/env python
# coding: utf-8

# In[1]:


# mysql connection
import mysql.connector

mydb = mysql.connector.connect(
  host="cowstudio.wayne-lee.cn",
  user="cowstudio",
  password="cowstudio_2119",
  database="cowstudio"
)


# In[2]:


# get spark session, 2g mem per executor
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# set python env
os.environ['PYSPARK_PYTHON'] = "/opt/conda3/envs/lab2/bin/python"
spark = SparkSession.builder     .appName("event_ods")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "1g")     .config("spark.executor.memory", "1g")     .config("spark.cores.max", "1")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[3]:


# get all user from db, and create an rdd
from datetime import datetime
date_string = datetime.today().strftime('%Y-%m-%d')
cur = mydb.cursor()
cur.execute(f"SELECT tracking_events.*,{date_string} as date from tracking_events")
result = cur.fetchall()
all_event = sc.parallelize(result)
print(all_event.count())


# In[4]:


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
schema = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("page_depth", IntegerType(), True),
    StructField("current_url", StringType(), True),
    StructField("date", StringType(), True)
])
event_df = all_event.toDF(schema)


# In[5]:


spark.sql("drop table if exists event_ods").show()


# In[6]:


event_df.write.mode("overwrite").saveAsTable("event_ods")


# In[7]:


spark.sql("show tables").show()


# In[8]:


spark.stop()
mydb.close()


# In[ ]:




