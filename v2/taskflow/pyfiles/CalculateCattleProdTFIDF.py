#!/usr/bin/env python
# coding: utf-8

# In[1]:


# get spark session, 2g mem per executor
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# set python env
os.environ['PYSPARK_PYTHON'] = "/opt/conda3/envs/lab2/bin/python"
spark = SparkSession.builder     .appName("CalculateCattleProdTFIDF")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "3")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[2]:


from datetime import datetime

date_string = datetime.today().strftime('%Y-%m-%d')


# In[3]:


# compute IDF
item_word_idf = spark.sql('''
select
    key_word,
    count(distinct item_id) as item_num_has_word,
    max(a.item_num) as item_num_all,
    log10(max(a.item_num)/ count(distinct item_id)) as idf,
    max(date) as date
from
    cattle_prod_word_count,(
        select
            count(distinct item_id) as item_num
        from
            cattle_prod_word_count
    ) as a
group by
    key_word
order by
    idf desc
''')
item_word_idf.createOrReplaceTempView("cattle_prod_idf")
item_word_idf.show()
item_word_idf.write.mode("overwrite").partitionBy("date").saveAsTable("cattle_prod_idf")
spark.sql("show tables").show()


# In[4]:


# compute TF
item_word_tf = spark.sql(f'''
with item_word_total_num as(
    select
        item_id,
        sum(word_count) as word_total
    from
        cattle_prod_word_count
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
        cattle_prod_word_count
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
    cattle_prod_word_count c on a.item_id = c.item_id and a.key_word = c.key_word
order by
    tf desc
''')
item_word_tf.createOrReplaceGlobalTempView("cattle_prod_tf")
item_word_tf.write.mode("overwrite").partitionBy("date").saveAsTable("cattle_prod_tf")
spark.sql("show tables").show()


# In[5]:


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
item_word_tfidf.createOrReplaceGlobalTempView("cattle_prod_tfidf")
item_word_tfidf.show()
item_word_tfidf.write.mode("overwrite").saveAsTable("cattle_prod_tfidf")
spark.sql("show tables").show()


# In[6]:


spark.stop()

