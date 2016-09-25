from pyspark import SparkConf, SparkContext
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql import Row
import copy
import json

# ---------------------------- Row models ------------------------------
clothes_model = Row("index", "brand", "gender", "size", "title", "category", "shoulder", "chest", "waist", "pelivs",
                    "hip", "thigh")

error_model_basic = Row('shoulder', 'chest', 'waist', 'pelvis', 'hip', 'thigh')
error_model_option = Row('shoulder', 'chest', 'waist', 'pelvis', 'hip', 'thigh', 'arm', 'leg', 'calf')

user_size_basic = ['shoulder', 'chest', 'waist', 'pelvis', 'hip', 'thigh']
user_size_option = ['shoulder', 'chest', 'waist', 'pelvis', 'hip', 'thigh', 'arm', 'leg', 'calf']
# ----------------------------------------------------------------------



# set spark conf
conf = SparkConf().setMaster("local").setAppName("classifyCloth")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# read csv
file_path = 'uniqloData.csv'
pdf = pd.read_csv(file_path, encoding='utf-8')
print(pdf.head())

df = sqlContext.createDataFrame(pdf)
df.show()
df_to_rdd = df.rdd


# error data filtering


def filter_error(row):
    columns = list(row)

    # //check title type is None
    if type(row.title) == type(None):
        return False

    # //check gender data
    if (row.gender != 'male') and (row.gender != 'female'):
        return False




    return True


clear_rdd = df_to_rdd.filter(filter_error)


# preprocess
def size_preprocess(rdd):
    def parse_size(row):
        # //chest Length To Round
        measure = row.size_detail

        shoulder = []
        chest = []
        waist = []
        pelvis = []
        hip = []
        thigh = []


        for size in measure :
            #size is dictionary

            for key in size:

                def get_shoulder():
                    shoulder.append(size[key])

                def get_chest():
                    if key == 'chestWidth':
                        chest.append(size[key] * 2)
                    else:
                        chest.append(size[key])

                def get_waist():
                    if key == 'waistWidth':
                        waist.append(size[key] * 2)
                    else:
                        waist.append(size[key])

                def get_pelvis():
                    if key == 'pelvisWidth':
                        pelvis.append(size[key] * 2)
                    else:
                        pelvis.append(size[key])

                def get_hip():
                    if key == 'hipWidth':
                        hip.append(size[key] * 2)
                    else:
                        hip.append(size[key])

                def get_thigh():
                    if key == 'thighWidth':
                        thigh.append(size[key] * 2)
                    else:
                        thigh.append(size[key])

                switch_dic = {
                    "shoulderWidth" : get_shoulder,
                    "chestWidth" : get_chest,
                    "chestRound" : get_chest,
                    "waistWidth" : get_waist,
                    "waistRound" : get_waist,
                    "pelvisWidth" : get_pelvis,
                    "pelvisRound" : get_pelvis,
                    "hipWidth" : get_hip,
                    "hipRound" : get_hip,
                    "thighWidth" : get_thigh,
                    "thighRound" : get_thigh
                }

                select = switch_dic.get(key)()

        # check size count and all column count is same
        size_count = len(row.size)

        if shoulder.count() != size_count:



        result_row = clothes_model(row.index, row.brand, row.gender, row.size, row.title, row.category,shoulder,chest,
                                   waist,pelvis,hip,thigh)


        return result_row

    parse_rdd = rdd.map(parse_size)

    return parse_rdd


preprocess_rdd = size_preprocess(clear_rdd)
'''
# TODO: get user's detail size by clothes

file_path = ''
user_pdf = pd.read_csv('file://' + file_path, encoding='utf-8')
user_df = sqlContext.createDataFrame(pdf)
user_rdd = user_df.rdd

# get error Data By Clothes
for clothes in preprocess_rdd.collect():
    def get_error(user):
        clothes_measure_list = clothes['measurement']
        clothes_measure_count = len(clothes_measure_list)

        clothes_size_list = clothes['size']
        clothes_size_count = len(clothes_size_list)

        fit_size = user['top_size']  # fit_size = user['fit_size']
        fit_clothes_index = clothes_size_list.index(fit_size)
        fit_clothes_measure = clothes_measure_list[fit_clothes_index]

        # --------------matched detail measure---------------
        """
        user              -   clothes
        shoulder          -   shoulderWicth * 2
        chest             -   chestRound / chestWidth * 2
        waist / pelvis    -   waistRound / chestWidth * 2
        hip               -   hipRound /  hipWidth * 2
        thigh             -   thighRound / thighWidth * 2
        arm               -   sleeveLength
        leg               -   legLength
        calf              -   calfRound / calfWidth * 2
        """
        # ---------------------------------------------------

        if fit_clothes_measure['shoulderWidth'] != None:
            user.shoulderWidth =











        return


    error_rdd = user_rdd.map(get_error)
'''

