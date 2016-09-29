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

error_model_basic = Row('name','size', 'shoulder', 'chest', 'waist', 'pelvis', 'hip', 'thigh')
error_model_option = Row('name','size', 'shoulder', 'chest', 'waist', 'pelvis', 'hip', 'thigh', 'arm', 'leg', 'calf')

user_size_basic = ['shoulder', 'chest', 'waist', 'pelvis', 'hip', 'thigh']
user_size_option = ['shoulder', 'chest', 'waist', 'pelvis', 'hip', 'thigh', 'arm', 'leg', 'calf']
# ----------------------------------------------------------------------

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




def colorFiltering(rdd):

    def reduce_by_color(fist_rdd, other):
        return parsed_clothes_model(fist_rdd.key, fist_rdd.brand, fist_rdd.gender,
                                    fist_rdd.category, fist_rdd.size, fist_rdd.name, fist_rdd.shoulder, fist_rdd.chest,
                                    fist_rdd.waist, fist_rdd.pelvis, fist_rdd.hip, fist_rdd.thigh)

    pairRdd = rdd.map(lambda row: (str(row.key) + 's' + row.size.encode('utf-8'), row))

    foldRdd = pairRdd.reduceByKey(reduce_by_color)

    return foldRdd.map(lambda pair : pair[1])



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

