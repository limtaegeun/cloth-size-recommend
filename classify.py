from pyspark import SparkConf, SparkContext
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql import Row
import copy
import json

# ---------------------------- Row models ------------------------------
clothes_model = Row("index", "brand", "gender", "size", "title", "category", "size_detail")

error_model_basic = Row('shoulder', 'chest', 'waist', 'pelvis', 'hip', 'thigh')
error_model_option = Row('shoulder', 'chest', 'waist', 'pelvis', 'hip', 'thigh', 'arm', 'leg', 'calf')

user_size_basic = ['shoulder', 'chest', 'waist', 'pelvis', 'hip', 'thigh']
user_size_option = ['shoulder', 'chest', 'waist', 'pelvis', 'hip', 'thigh', 'arm', 'leg', 'calf']
# ----------------------------------------------------------------------



# set spark conf
conf = SparkConf().setMaster("local").setAppName("classifyCloth")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# read csv
file_path = '/Users/stevelim/spark/siba.csv'
pdf = pd.read_csv('file://' + file_path, encoding='utf-8')
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

        # //check if every size is zero
    sum_of_size = 0

    for index in range(columns.index(row.ShoulderWidth), len(columns)):
        if type(columns[index]) != int and type(columns[index]) != float:
            # //columns[index] = int(columns[index])
            return False
        sum_of_size += columns[index]

    if sum_of_size == 0:
        return False

    return True

clear_rdd = df_to_rdd.filter(filter_error)


# preprocess
def size_preprocess(rdd):
    def parse_size(row):

        # //chest Length To Round
        chest = (row.BreastSide * 2) if (row.BreastSide != 0) else row.BreastPeripheral
        waist = (row.WaistSection * 2) if (row.WaistSection != 0) else row.WaistCircumference
        hip = (row.HipSection * 2) if (row.HipSection != 0) else row.HipCircumference
        thigh = (row.Thighsection * 2) if (row.Thighsection != 0) else row.ThighCircumference

        new_row = clothes_model(row.key, row.brand, row.category, row.title, row.gender, row.size, row.material,
                                row.ShoulderWidth, chest, waist, hip, thigh, None)
        return new_row

    parse_rdd = rdd.map(parse_size)

    return parse_rdd

preprocess_rdd = size_preprocess(clear_rdd)

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
        '''
        user              -   clothes
        shoulder          -   shoulderWith * 2
        chest             -   chestRound / chestWidth * 2
        waist / pelvis    -   waistRound / chestWidth * 2
        hip               -   hipRound /  hipWidth * 2
        thigh             -   thighRound / thighWidth * 2
        arm               -   sleeveLength
        leg               -   legLength
        calf              -   calfRound / calfWidth * 2
        '''
        # ---------------------------------------------------

        







        return
    error_rdd = user_rdd.map(get_error)


