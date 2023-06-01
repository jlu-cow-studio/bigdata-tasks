#!/usr/bin/env python
# coding: utf-8

# In[1]:


# get spark session, 2g mem per executor
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# set python env
os.environ['PYSPARK_PYTHON'] = "/opt/conda3/envs/lab2/bin/python"
spark = SparkSession.builder     .appName("CalculateCattleProdTextFeature")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "1")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[2]:


from datetime import datetime

date_string = datetime.today().strftime('%Y-%m-%d')


# In[3]:


cattle_prod_item_text_feature = spark.sql(f'''
with key_words as(
    select
        key_word
    from
        cattle_prod_tfidf
    where
        date = '{date_string}'
    group by
        key_word
    order by
        sum(tfidf) desc
    limit 30
), all_items as(
    select
        distinct id as item_id
    from
        item_ods
    where
        date = '{date_string}'
    and
        category = 'twitte'
), item_words_cross(
    select
        item_id,
        key_word
    from
        key_words,
        all_items
)
select
    a.item_id,
    a.key_word,
    if(b.tfidf is null,0,b.tfidf) as val,
    '{date_string}' as date
from
    item_words_cross a
left join
    cattle_prod_tfidf b on a.key_word = b.key_word and a.item_id = b.item_id
''')
cattle_prod_item_text_feature.write.mode("overwrite").partitionBy("date").saveAsTable("cattle_prod_item_text_feature")


# In[8]:


from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DoubleType

# 定义schema
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("key_word", StringType(), True),
    StructField("val", DoubleType(), True),
    StructField("date", StringType(), True)
])

# 创建一个空的DataFrame
spark.createDataFrame([],schema).write.mode("overwrite").partitionBy("date").saveAsTable("cattle_prod_user_text_feature")


# In[4]:


from pyspark.mllib.linalg.distributed import CoordinateMatrix, MatrixEntry
#创建矩阵
table_a = spark.table("cattle_prod_user_action_matrix")
table_b = spark.table("cattle_prod_item_text_feature")
rdd_title_user_feature = table_a.filter(table_a.date == date_string).select('user_id','item_id','val').rdd.map(lambda row: MatrixEntry(row.user_id, row.item_id, row.val))
rdd_title_item_feature = table_b.filter(table_b.date == date_string).select('item_id','key_word','val').rdd.map(lambda row:MatrixEntry(row.item_id, row.key_word, row.val))
mat_user_item = CoordinateMatrix(rdd_title_user_feature)
mat_item_word = CoordinateMatrix(rdd_title_item_feature)

#用户-物品， 物品-关键字矩阵相乘
mat_user_item = mat_user_item.toBlockMatrix()
mat_item_word = mat_item_word.toBlockMatrix()
result = mat_user_item.multiply(mat_item_word)

#用户-关键字 特征存储
cattle_prod_user_text_feature = result.toCoordinateMatrix().entries.map(lambda e: (e.i,e.j.e.value)).toDF(["user_id","key_word","val"])
cattle_prod_user_text_feature.write.mode("overwrite").partitionBy("date").saveAsTable("cattle_prod_user_text_feature")


# In[ ]:


spark.stop()


# In[ ]:




