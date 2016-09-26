from pyspark import SparkConf, SparkContext
import pandas as pd
from pyspark.sql import SQLContext
import json
from  pyspark.sql import Row

#set spark conf
conf = SparkConf().setMaster("local").setAppName("classifyCloth")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)


a = sc.parallelize([1,2,3,4,5,6,7,8])


def filtering(x):
    if x == 3:
        return

    return 2 * x

b = a.map(filtering)


print b.collect()
