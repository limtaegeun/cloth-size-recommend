# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql import Row
import copy
import json
import requests
import helpFunc
import preprocess
import analytics



# set spark conf
conf = SparkConf().setMaster("local").setAppName("classifyCloth")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

requestDf = requests.get('http://ec2-52-78-22-186.ap-northeast-2.compute.amazonaws.com:8282/info/all/sizedata')

pdf = json.loads(requestDf.text)
df = sqlContext.createDataFrame(pdf)
#df.show()
df_to_rdd = df.rdd

clear_rdd = df_to_rdd.filter(preprocess.filter_error)

preprocess_rdd = preprocess.size_preprocess(clear_rdd)

helpFunc.show_rdd(preprocess_rdd, sqlContext)

reduceRdd = preprocess.colorFiltering(preprocess_rdd).sortBy(lambda row : row.key)
helpFunc.show_rdd(reduceRdd, sqlContext)

integRdd = preprocess.integration(reduceRdd).persist()

helpFunc.show_rdd(integRdd, sqlContext)







# TODO: get user's detail size by clothes

file_path = 'teamData.csv'
user_pdf = pd.read_csv(file_path, encoding='utf-8')
user_df = sqlContext.createDataFrame(user_pdf)
all_user = user_df.rdd.collect()

#user_df.show()

# get error Data By Clothes



error_rdd = analytics.get_data(integRdd, all_user, [u'티셔츠'],[u'male'])
helpFunc.show_rdd(error_rdd,sqlContext)

