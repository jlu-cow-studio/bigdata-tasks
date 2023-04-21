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
spark = SparkSession.builder     .appName("item_ods")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "1g")     .config("spark.executor.memory", "1g")     .config("spark.cores.max", "1")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[3]:


# get all items from db, and create an rdd
from datetime import datetime
date_string = datetime.today().strftime('%Y-%m-%d')
cur = mydb.cursor()
cur.execute(f"SELECT items.*,{date_string} as date from items")
result = cur.fetchall()
all_items = sc.parallelize(result)
print(all_items.count())


# In[4]:


from pyspark.sql.types import DecimalType, TimestampType, StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DecimalType(10, 2), True),
    StructField("stock", IntegerType(), True),
    StructField("province", StringType(), True),
    StructField("city", StringType(), True),
    StructField("district", StringType(), True),
    StructField("image_url", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("user_type", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("specific_attributes", StringType(), True),
    StructField("date", StringType(), True) 
])
item_df = all_items.toDF(schema)


# In[5]:


spark.sql("drop table if exists item_ods").show()


# In[6]:


item_df.write.mode("overwrite").saveAsTable("item_ods")


# In[7]:


spark.sql("show tables").show()


# In[8]:


spark.stop()
mydb.close()


# In[ ]:




