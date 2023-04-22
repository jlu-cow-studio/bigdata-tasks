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
spark = SparkSession.builder     .appName("item_tfidf")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "3")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[3]:


# import jieba and set stop word set
import jieba
import jieba.analyse

stop_words_rdd = sc.textFile("hdfs:///user/spark_temp/stopwords.dat")
stop_words_set = set(stop_words_rdd.collect())


# In[4]:


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


# In[5]:


# get all item data from db, set result to an RDD
cur = mydb.cursor()
cur.execute("SELECT id,name,description from items order by RAND() ")
result = cur.fetchall()
all_items = sc.parallelize(result)
print(all_items.count())


# In[6]:


all_items = spark.sql("select id,name,description from item_ods").rdd
print(all_items.count())


# In[7]:


# cut item's name and description
cut_items = all_items.map(cut_name_and_desc)
print(all_items.count())


# In[8]:


# do word count
item_word_count = cut_items.flatMap(to_count)                    .reduceByKey(lambda a,b:a+b)                    .map(split_key_set_date)
print(item_word_count.count())


# In[9]:


# create a table for wordcount
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
schema = StructType([
    StructField("item_id", IntegerType(), True),
    StructField("key_word", StringType(), True),
    StructField("word_count", IntegerType(), True),
    StructField("date", StringType(),True)
])
item_word_count = spark.createDataFrame(item_word_count, schema)
item_word_count.createOrReplaceTempView("item_word_count")
item_word_count.show()


# In[10]:


# 将DataFrame写入MySQL
item_word_count.write.mode("overwrite").partitionBy("date").saveAsTable("item_word_count")
spark.sql("show tables").show()


# In[11]:


# compute IDF
item_word_idf = spark.sql('''
select
    key_word,
    count(distinct item_id) as item_num_has_word,
    max(a.item_num) as item_num_all,
    log10(max(a.item_num)/ count(distinct item_id)) as idf,
    max(date) as date
from
    item_word_count,(
        select
            count(distinct item_id) as item_num
        from
            item_word_count
    ) as a
group by
    key_word
order by
    idf desc
''')
item_word_idf.createOrReplaceTempView("item_word_idf")
item_word_idf.show()
item_word_idf.write.mode("overwrite").saveAsTable("item_word_idf")
spark.sql("show tables").show()


# In[12]:


# compute TF
item_word_tf = spark.sql(f'''
with item_word_total_num as(
    select
        item_id,
        sum(word_count) as word_total
    from
        item_word_count
    group by
        item_id
), item_all as(
    select
        distinct id as item_id
    from
        item_ods
), word_all as(
    select
        distinct key_word
    from
        item_word_count
), item_word_all as(
    select
        item_id,
        key_word
    from
        item_all,
        word_all
)
select 
    a.item_id,
    a.key_word,
    if(b.item_id is null or c.word_count is null or b.item_id = 0, 0, c.word_count/b.word_total) as tf,
    '{date_string}' as date
from
    item_word_all a
left join
    item_word_total_num b on a.item_id = b.item_id
left join
    item_word_count c on a.item_id = c.item_id and a.key_word = c.key_word
order by
    tf desc
''')
item_word_tf.createOrReplaceGlobalTempView("item_word_tf")
item_word_tf.write.mode("overwrite").saveAsTable("item_word_tf")
spark.sql("show tables").show()


# In[13]:


# compute tf-idf
item_word_tfidf = spark.sql('''
select 
    tf.item_id,
    tf.key_word,
    tf.tf * idf.idf as tfidf,
    tf.date
from
    item_word_tf as tf
left join
    item_word_idf as idf on idf.key_word = tf.key_word
order by
    tfidf desc
''')
item_word_tfidf.createOrReplaceGlobalTempView("item_word_tfidf")
item_word_tfidf.show()
item_word_tfidf.write.mode("overwrite").saveAsTable("item_word_tfidf")
spark.sql("show tables").show()


# In[14]:


# close spark session
spark.stop()
mydb.close()


# In[ ]:




