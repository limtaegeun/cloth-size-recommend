from pyspark import SparkConf, SparkContext
import pandas as pd
from pyspark.sql import SQLContext
import json
from  pyspark.sql import Row
#set spark conf
conf = SparkConf().setMaster("local").setAppName("classifyCloth")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
# read csv
file_path = 'siba1.csv'
pdf = pd.read_csv( file_path, encoding='utf-8')

df = sqlContext.createDataFrame(pdf)
df.show()

a = df.flatMap(lambda row: json.loads(row.size_detail)[0].keys())

print a.take(20)

dis = a.distinct()
utf = dis.map(lambda x: Row(cate=unicode(x)))
ddf = sqlContext.createDataFrame(utf)
pan = ddf.toPandas()

pan.to_csv('cate.csv', encoding='utf-8')