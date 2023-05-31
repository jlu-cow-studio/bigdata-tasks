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
spark = SparkSession.builder     .appName("tag_ods")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "1g")     .config("spark.executor.memory", "1g")     .config("spark.cores.max", "1")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[3]:


from datetime import datetime
date_string = datetime.today().strftime('%Y-%m-%d')
print(date_string)


# In[5]:


# get all user from db, and create an rdd
cur = mydb.cursor()
cur.execute(f'''
SELECT 
    tag.*,
    tag_category.*,
    '{date_string}' as date 
from 
    tag 
left join 
    tag_category on tag.category_id = tag_category.tag_category_id
''')
print(cur.column_names)
result = cur.fetchall()
all_tags = sc.parallelize(result)
print(all_tags.count())


# In[6]:


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
schema = StructType([
    StructField("tag_id", IntegerType(), True),
    StructField("tag_name", StringType(), True),
    StructField("tag_weight", DecimalType(8, 4), True),
    StructField("mark_object", StringType(), True),
    StructField("category_id", IntegerType(), True),
    StructField("tag_category_id", IntegerType(), True),
    StructField("category_name", StringType(), True),
    StructField("category_parent_id", IntegerType(), True),
    StructField("level", IntegerType(), True),
    StructField("date", StringType(), True)
])
tag_df = all_tags.toDF(schema)


# In[7]:


# spark.sql("drop table if exists tag_ods").show()


# In[8]:


tag_df.write.mode("overwrite").partitionBy("date").saveAsTable("tag_ods")


# In[9]:


spark.sql("show tables").show()
spark.sql("select * from tag_ods").show()


# In[10]:


spark.stop()
mydb.close()


# In[ ]:




