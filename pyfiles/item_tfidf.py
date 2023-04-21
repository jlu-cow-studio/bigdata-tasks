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
spark = SparkSession.builder \
    .appName("tiem_tfidf") \
    .master("spark://node01:10077") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.cores.max", "3") \
    .config("spark.sql.shuffle.partitions", "12") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

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


# cut item's name and description
cut_items = all_items.map(cut_name_and_desc)
print(all_items.count())


# In[7]:


# do word count
item_word_count = cut_items.flatMap(to_count)\
                    .reduceByKey(lambda a,b:a+b)\
                    .map(split_key_set_date)
print(item_word_count.count())


# In[8]:


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


# In[ ]:


# 将DataFrame写入MySQL
item_word_count.write.format("jdbc") \
    .option("url", "jdbc:mysql://cowstudio.wayne-lee.cn:3306/cowstudio") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "item_word_count") \
    .option("user", "cowstudio") \
    .option("password", "cowstudio_2119") \
    .save(mode="overwrite")


# In[9]:


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


# In[10]:


# compute TF
item_word_tf = spark.sql('''
with item_word_num as(
    select
        item_id,
        sum(word_count) as word_total_count
    from
        item_word_count
    group by
        item_id
)
select 
    item_word_count.item_id,
    item_word_count.key_word,
    item_word_count.word_count / item_word_num.word_total_count as tf
from
    item_word_count
left join 
    item_word_num on item_word_count.item_id = item_word_num.item_id
''')
item_word_tf.createGlobalTempView("item_word_tf")
item_word_tf.show()


# In[11]:


# compute tf-idf
# item_word_tfidf = spark.sql('''
# select 
#     tf.item_id,
#     tf.key_word,
#     if(tf.tf is )
# from
#     item_word_idf as idf
# left join
#     item_word_tf as tf on idf.key_word = tf.key_word
# ''')


# In[5]:


# close spark session
spark.stop()


# In[ ]:




