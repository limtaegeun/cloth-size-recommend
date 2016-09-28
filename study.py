# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql import Row
import copy
import json
import requests
import helpFunc



# ---------------------------- Row models ------------------------------
clothes_model = Row("index", "key", "brand", "gender", "category", "size", "name",  "shoulder", "chest", "waist", "pelvis",
                    "hip", "thigh")
parsed_clothes_model =Row("key", "brand", "gender", "category", "size", "name",  "shoulder", "chest", "waist", "pelvis",
                    "hip", "thigh")

error_model_basic = Row('size', 'shoulder', 'chest', 'waist', 'pelvis', 'hip', 'thigh')
error_model_option = Row('size', 'shoulder', 'chest', 'waist', 'pelvis', 'hip', 'thigh', 'arm', 'leg', 'calf')

user_size_basic = ['shoulder', 'chest', 'waist', 'pelvis', 'hip', 'thigh']
user_size_option = ['shoulder', 'chest', 'waist', 'pelvis', 'hip', 'thigh', 'arm', 'leg', 'calf']
# ----------------------------------------------------------------------



# set spark conf
conf = SparkConf().setMaster("local").setAppName("classifyCloth")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
'''
import pymysql
conn = pymysql.connect("jaem-db.cds2i1qh3v1m.ap-northeast-2.rds.amazonaws.com","root","qkrwjdxo1","cloth_test", cursorclass=pymysql.cursors.DictCursor)
cursor = conn.cursor()
cursor.execute('SET NAMES utf8;')
cursor.execute('SET CHARACTER SET utf8;')
cursor.execute('SET character_set_connection=utf8;')
query = 'SELECT Cloths.id AS ProductUrlId,  ' \
        'ClothInfos.id AS ProductId, ' \
        'ClothInfos.clothName AS name, ' \
        'ClothInfos.clothGender AS gender,  ' \
        'ClothInfos.clothCategory AS category, ' \
        'ClothSizes.size AS size, ' \
        'ClothInfos.clothBrand AS brand, ' \
        'Cloths.color AS color , ' \
        'ClothSizes.shoulderWidth AS shoulderWidth, ' \
        'ClothSizes.chestWidth AS chestWidth,' \
        'ClothSizes.chestRound AS chestRound,' \
        'ClothSizes.waistWidth AS waistWidth,' \
        'ClothSizes.waistRound AS waistRound,' \
        'ClothSizes.pelvisRound AS pelvisRound, ' \
        'ClothSizes.hipWidth AS hipWidth,   ' \
        'ClothSizes.hipRound AS hipRound,' \
        'ClothSizes.thighWidth AS thighWidth,' \
        'ClothSizes.thighRound AS thighRound  ' \
        'FROM Cloths LEFT JOIN ClothInfos ON ClothInfos.id = Cloths.ClothInfoId ' \
        'LEFT JOIN ClothSizes ON ClothSizes.id = Cloths.ClothSizeId '


df = pd.read_sql(query, conn)
print(df)
'''

requestDf = requests.get('http://ec2-52-78-22-186.ap-northeast-2.compute.amazonaws.com:8282/info/all/sizedata')

pdf = json.loads(requestDf.text)

df = sqlContext.createDataFrame(pdf)
#df.show()
df_to_rdd = df.rdd

# error data filtering
def filter_error(row):
    columns = list(row)

    # //check title type is None
    if type(row.name) == type(None):
        return False

    # //check gender data
    if (row.gender != 'male') and (row.gender != 'female'):
        return False

    dic = row.asDict()
    for key in dic:
        if key not in ['ProductUrlId', 'ProductId', 'brand', 'gender', 'category', 'size', 'name', 'color']:
            if not helpFunc.isfloat(dic[key]):
                return False


    return True


clear_rdd = df_to_rdd.filter(filter_error)


# preprocess
def size_preprocess(rdd):
    def parse_size(row):

        shoulder = float(row.shoulderWidth)
        chest = (float(row.chestWidth) * 2) if ( float(row.chestWidth) != 0) else float(row.chestRound)
        waist = (float(row.waistWidth) * 2) if (float(row.waistWidth) != 0) else float(row.waistRound)
        pelvis = float(row.pelvisRound)
        hip = (float(row.hipWidth) * 2) if (float(row.hipWidth) != 0) else float(row.hipRound)
        thigh = (float(row.thighWidth)* 2) if (float(row.thighWidth) != 0) else float(row.thighRound)

        result_row = clothes_model(row.ProductUrlId, row.ProductId, row.brand.strip(), row.gender.strip(),
                                   row.category.strip(), row.size.strip(), row.name.strip(),
                                   shoulder, chest, waist, pelvis, hip, thigh)

        return result_row

    parse_rdd = rdd.map(parse_size)

    return parse_rdd


preprocess_rdd = size_preprocess(clear_rdd)

helpFunc.show_rdd(preprocess_rdd, sqlContext)

def colorFiltering(rdd):

    def reduce_by_color(fist_rdd, other):
        return parsed_clothes_model(fist_rdd.key, fist_rdd.brand, fist_rdd.gender,
                                    fist_rdd.category, fist_rdd.size, fist_rdd.name, fist_rdd.shoulder, fist_rdd.chest,
                                    fist_rdd.waist, fist_rdd.pelvis, fist_rdd.hip, fist_rdd.thigh)

    pairRdd = rdd.map(lambda row: (str(row.key) + 's' + row.size.encode('utf-8'), row))

    foldRdd = pairRdd.reduceByKey(reduce_by_color)

    return foldRdd.map(lambda pair : pair[1])

reduceRdd = colorFiltering(preprocess_rdd).sortBy(lambda row : row.key)
helpFunc.show_rdd(reduceRdd, sqlContext)

def integration(rdd):
    # // map rdd to keyPairRdd
    pairRdd = rdd.map(lambda row: (row.key, row))

    # //set userData Type
    global userData

    # // create closer to run action 'combineByKey'
    def combiner(row):

        return parsed_clothes_model(row.key, row.brand, row.gender, row.category, [row.size], row.name,
                                    [row.shoulder], [row.chest], [row.waist], [row.pelvis], [row.hip], [row.thigh])


    def mergeValue(row1, row2):

        return parsed_clothes_model(row1.key, row1.brand, row1.gender, row1.category, row1.size + [row2.size], row1.name,
                             row1.shoulder + [row2.shoulder], row1.chest + [row2.chest], row1.waist + [row2.waist],
                             row1.pelvis + [row2.pelvis], row1.hip + [row2.hip], row1.thigh + [row2.thigh])


    def mergeTwoCombiners(row1, row2):
        return parsed_clothes_model(row1.key, row1.brand, row1.gender, row1.category, row1.size + row2.size, row1.name,
                             row1.shoulder + row2.shoulder, row1.chest + row2.chest, row1.waist + row2.waist,
                             row1.pelvis + row2.pelvis, row1.hip + row2.hip, row1.thigh + row2.thigh)

        # //run action 'combineByKey'

    integRdd = pairRdd.combineByKey(combiner, mergeValue, mergeTwoCombiners)

    # //return rdd that maped key pair rdd to rdd
    return integRdd.map(lambda pair: pair[1])

integRdd = integration(reduceRdd).persist()

helpFunc.show_rdd(integRdd, sqlContext)


def filter_category(rdd, categorys,):
    def filter_func(row):

        if row.category not in categorys:
            return False

        if row.gender != u'male':
            return False
        return True

    return rdd.filter(filter_func)

filtering_rdd = filter_category(integRdd, [u'티셔츠'])
helpFunc.show_rdd(filtering_rdd,sqlContext)


# TODO: get user's detail size by clothes

file_path = 'teamData.csv'
user_pdf = pd.read_csv(file_path, encoding='utf-8')
user_df = sqlContext.createDataFrame(user_pdf)
all_user = user_df.rdd.collect()

user_df.show()

# get error Data By Clothes

def get_error(row):

    # --------------matched detail measure---------------
    """
    user              -   clothes
    shoulder          -   shoulderWidth * 2
    chest             -   chestRound / chestWidth * 2
    waist / pelvis    -   waistRound / chestWidth * 2 / pelvisRound
    hip               -   hipRound /  hipWidth * 2
    thigh             -   thighRound / thighWidth * 2
    arm               -   sleeveLength
    leg               -   legLength
    calf              -   calfRound / calfWidth * 2
    """
    # ---------------------------------------------------

    row_list = []

    for user in all_user:

        user_dic = user.asDict()

        if user_dic[u'sex'] == u'm':

            fit_size = unicode(user_dic['top_size'])  # fit_size = user['fit_size']

            try:
                size_index = row.size.index(fit_size)
            except ValueError:
                continue

            shoulder = (row.shoulder[size_index] * 2 - user_dic['shoulder']) if row.shoulder[size_index] != 0 else 0
            chest = (row.chest[size_index] - user_dic['chest']) if row.chest[size_index] != 0 else 0
            waist = (row.waist[size_index] - user_dic['waist']) if row.waist[size_index] != 0 else 0
            hip = (row.hip[size_index] - user_dic['hip']) if row.hip[size_index] != 0 else 0
            thigh = (row.thigh[size_index] - user_dic['thigh']) if row.thigh[size_index] != 0 else 0

            pelvis = 0
            if row.waist[size_index] != 0:
                pelvis = (row.waist[size_index] - user_dic['pelvis'])
            elif row.pelvis[size_index] != 0:
                pelvis = (row.pelvis[size_index] - user_dic['pelvis'])
            else:
                pelvis = 0

            # error_model_basic = Row('size', 'shoulder', 'chest', 'waist', 'pelvis', 'hip', 'thigh')
            row_list.append(error_model_basic(row.size[size_index], shoulder, chest, waist, pelvis, hip, thigh))


    return row_list

error_rdd = filtering_rdd.flatMap(get_error)

helpFunc.show_rdd(error_rdd,sqlContext)

