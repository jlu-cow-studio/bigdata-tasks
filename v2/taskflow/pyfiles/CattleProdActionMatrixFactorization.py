#!/usr/bin/env python
# coding: utf-8

# In[1]:


# get spark session, 2g mem per executor
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# set python env
os.environ['PYSPARK_PYTHON'] = "/opt/conda3/envs/lab2/bin/python"
spark = SparkSession.builder     .appName("CattleProdActionMatrixFactorization")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "2")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[3]:


from datetime import datetime

date_string = datetime.today().strftime('%Y-%m-%d')


# In[4]:


from pyspark.ml.recommendation import ALS
from pyspark.ml.linalg import DenseVector

# 从Hive中读取数据
df = spark.sql(f'''
SELECT
    user_id, 
    item_id, 
    val 
FROM 
    cattle_prod_user_action_matrix 
where 
    date = '{date_string}'
''')

# 初始化ALS模型
als = ALS(rank=15, maxIter=10,userCol="user_id", itemCol="item_id", ratingCol="val")

# 训练模型
model = als.fit(df)

# 获取用户和物品的因子
userFactors = model.userFactors
itemFactors = model.itemFactors

# 将因子分解为单独的列
def extract(row):
    return (row.id,) + tuple(float(x) for x in row.features)

userFactors = userFactors.rdd.map(extract).toDF(["user_id"] + ["fac_" + str(i) for i in range(model.rank)])
itemFactors = itemFactors.rdd.map(extract).toDF(["item_id"] + ["fac_" + str(i) for i in range(model.rank)])

# 将因子数据保存回Hive
userFactors.write.mode("overwrite").saveAsTable("cattle_prod_user_fact_matrix")
itemFactors.write.mode("overwrite").saveAsTable("cattle_prod_item_fact_matrix")


# In[ ]:


spark.stop()


# In[ ]:





# In[ ]:




