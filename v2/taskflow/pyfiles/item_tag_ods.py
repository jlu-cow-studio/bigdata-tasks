#!/usr/bin/env python
# coding: utf-8

# In[1]:


from datetime import datetime
date_string = datetime.today().strftime('%Y-%m-%d')
print(date_string)
spark_app_name = "item_tag_ods"
sql = f"SELECT item_tag.*,'{date_string}' as date from item_tag"
hive_table_name = "item_tag_ods"


# In[2]:


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, DecimalType
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("tag_id", IntegerType(), True),
    StructField("weight", DecimalType(), True),
    StructField("date", StringType(), True)
])


# In[3]:


# mysql connection
import mysql.connector

mydb = mysql.connector.connect(
  host="cowstudio.wayne-lee.cn",
  user="cowstudio",
  password="cowstudio_2119",
  database="cowstudio"
)


# In[4]:


# get spark session, 2g mem per executor
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# set python env
os.environ['PYSPARK_PYTHON'] = "/opt/conda3/envs/lab2/bin/python"
spark = SparkSession.builder     .appName(spark_app_name)     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "1g")     .config("spark.executor.memory", "1g")     .config("spark.cores.max", "1")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[5]:


# get all user from db, and create an rdd
print(sql)
cur = mydb.cursor()
cur.execute(sql)
result = cur.fetchall()


# In[6]:


rdd = sc.parallelize(result)
df = rdd.toDF(schema)
df.show()


# In[7]:


# spark.sql(f"drop table if exists {hive_table_name}").show()


# In[8]:


df.write.mode("overwrite").partitionBy("date").saveAsTable(hive_table_name)


# In[9]:


spark.sql("show tables").show()


# In[10]:


spark.stop()
mydb.close()


# In[ ]:




