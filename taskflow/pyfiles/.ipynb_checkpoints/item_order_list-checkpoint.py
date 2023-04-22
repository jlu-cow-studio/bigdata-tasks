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
spark = SparkSession.builder     .appName("item_order_list")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "3")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[3]:


# define map functions 
from datetime import datetime, timedelta

today_string = datetime.today().strftime('%Y-%m-%d')
print(today_string)

fresh_list_thr = 10
host_list_size = 30


# In[4]:


item_order = spark.sql(f'''
with item_all as(
    select
        id as item_id,
        category
    from
        item_ods
)
select
    item_all.item_id,
    item_all.category,
    item_score.score,
    row_number() over(
        partition by
            category
        order by
            score desc
    ) as rn
from
    item_all
left join
    item_score on item_all.item_id = item_score.item_id
where
    date = '{today_string}'
order by
    rn
''')
item_order.show()
item_order.createOrReplaceTempView("item_order")


# In[5]:


item_fresh_list = spark.sql(f'''
select
    *
from
    item_order
where
    score < {fresh_list_thr}
''')
item_fresh_list.show()
item_fresh_list.createOrReplaceTempView("item_fresh_list")


# In[6]:


item_hot_list = spark.sql(f'''
select
    a.*
from
    item_order a
left join
    item_fresh_list b on a.item_id = b.item_id 
where
    a.rn <= {host_list_size}
and
    b.item_id is null
''')
item_hot_list.show()
item_hot_list.createOrReplaceTempView("item_hot_list")


# In[7]:


# spark.sql("drop table if exists ").show()


# In[10]:


item_hot_list.write.mode("overwrite").partitionBy("date").s.partitionBy("date")aveAsTable("item_hot_list")


# In[10]:


item_hot_list.write.mode("overwrite").partitionBy("date").s.partitionBy("date")aveAsTable("item_hot_list")


# In[10]:


item_hot_list.write.mode("overwrite").partitionBy("date").s.partitionBy("date")aveAsTable("item_hot_list")


# In[11]:


spark.sql("show tables").show()


# In[12]:


spark.stop()
mydb.close()


# In[ ]:




