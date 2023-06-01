#!/usr/bin/env python
# coding: utf-8

# In[1]:


# get spark session, 2g mem per executor
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# set python env
os.environ['PYSPARK_PYTHON'] = "/opt/conda3/envs/lab2/bin/python"
spark = SparkSession.builder     .appName("ProcessCattleProdText")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "3")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[2]:


# import jieba and set stop word set
import jieba
import jieba.analyse

stop_words_rdd = sc.textFile("hdfs:///user/spark_temp/stopwords.dat")
stop_words_set = set(stop_words_rdd.collect())


# In[3]:


# define map functions 
from datetime import datetime

date_string = datetime.today().strftime('%Y-%m-%d')
# cut item name and description
def cut_name_and_desc(item):
    id, name, desc = item
    name_cut = set(jieba.cut(name))
    name_cut_pure = set(jieba.cut(name)) - stop_words_set
    desc_cut = set(jieba.cut(desc))
    desc_cut_pure = set(jieba.cut(desc)) - stop_words_set
    return (id,name_cut,name_cut_pure,desc_cut,desc_cut_pure)

# map item's cut list to word count
def to_count(item):
    id,name_cut,name_cut_pure,desc_cut,desc_cut_pure = item
    for i in name_cut_pure:
        yield ((id, i),1)
    for i in desc_cut_pure:
        yield ((id,i),1)
        
# trnasfer (id, key), count to id,key,count, date
def split_key_set_date(item):
    key1,count = item
    id,key = key1
    date = date_string
    return id,key,count,date


# In[4]:


all_items = spark.sql("select id,name,description from item_ods where category='cattle_product'").rdd
print(all_items.count())


# In[5]:


# cut item's name and description
cut_items = all_items.map(cut_name_and_desc)
print(all_items.count())


# In[6]:


# do word count
item_word_count = cut_items.flatMap(to_count)                    .reduceByKey(lambda a,b:a+b)                    .map(split_key_set_date)
print(item_word_count.count())


# In[7]:


# create a table for wordcount
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
schema = StructType([
    StructField("item_id", IntegerType(), True),
    StructField("key_word", StringType(), True),
    StructField("word_count", IntegerType(), True),
    StructField("date", StringType(),True)
])
item_word_count = spark.createDataFrame(item_word_count, schema)
item_word_count.createOrReplaceTempView("cattle_prod_word_count")
item_word_count.show()


# In[8]:


# 将DataFrame写入Hive
item_word_count.write.mode("overwrite").partitionBy("date").saveAsTable("cattle_prod_word_count")
spark.sql("show tables").show()


# In[9]:


spark.stop()

