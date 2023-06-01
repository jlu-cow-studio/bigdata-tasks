#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# get spark session, 2g mem per executor
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# set python env
os.environ['PYSPARK_PYTHON'] = "/opt/conda3/envs/lab2/bin/python"
spark = SparkSession.builder     .appName("VetTwitteGenerateCFRecommendation")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "1")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[ ]:


from pyspark.ml.linalg import DenseVector
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from pyspark.sql import functions as F

# 读取Hive表
user_factors = spark.table("vet_twitte_user_fact_matrix")
item_factors = spark.table("vet_twitte_item_fact_matrix")

# 转换因子数据到向量格式
user_factors_vec = user_factors.rdd.map(lambda row: IndexedRow(row.user_id, DenseVector(row[1:])))
item_factors_vec = item_factors.rdd.map(lambda row: IndexedRow(row.item_id, DenseVector(row[1:])))

# 创建IndexedRowMatrix对象
user_matrix = IndexedRowMatrix(user_factors_vec)
item_matrix = IndexedRowMatrix(item_factors_vec)

# 计算矩阵乘法
product_matrix = user_matrix.multiply(item_matrix.toBlockMatrix().transpose().toIndexedRowMatrix())

# 将结果转换回DataFrame
result_df = product_matrix.rows.toDF(["user_id", "features"])
result_df = result_df.select(F.col("user_id"), F.posexplode(F.col("features"))).selectExpr("user_id", "pos as item_id", "col as val")

# 存储结果回Hive
result_df.createOrReplaceTempView("vet_twitte_user_item_product")
spark.sql(f'''
select
    user_id,
    collect_list(item_id) over (partition by user_id order by val desc) as rec_list
from
    vet_twitte_user_item_product
''').write.mode("overwrite").partitionBy("date").saveAsTable("vet_twitte_cf_based_rec")


# In[ ]:


spark.stop()


# In[ ]:





# In[ ]:




