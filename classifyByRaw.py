# -*- coding: utf-8 -*-
import numpy as np
import matplotlib as mlp
import pandas as pd

fileName = 'categoryStudyData.csv'
df = pd.read_csv(fileName)
print df.head()

df.to_csv('categoryStudyData.csv')

import MySQLdb
import pandas as pd

con = MySQLdb.connect("jaem-db.cds2i1qh3v1m.ap-northeast-2.rds.amazonaws.com","root","qkrwjdxo1","cloth_test")
cursor = con.cursor()
df = pd.read_sql("SELECT UserSizes.shoulderM, UserSizes.chestM, UserSizes.waistM, UserSizes.hipM, UserSizes.thighM, \
 Users.gender, Users.age FROM Users INNER JOIN UserSizes WHERE Users.key=UserSizes.userKey AND Users.age > 18 AND \
 Users.age < 60",con=con)

# sqlContext = SQLContext(sc)
dataf = sqlContext.createDataFrame(df)
dataf.show()

# **************************************************************************************************************
# 데이터 파일 csv 불러오기
# **************************************************************************************************************
import json
from pyspark.sql import HiveContext, Row
from pyspark.sql import SQLContext
#sqlContext = SQLContext(sc)

#//TODO file read -> db read
pdf = pd.read_csv("file:///home/ec2-user/data/clothData.csv",encoding = 'utf-8')
df = sqlContext.createDataFrame(pdf)

#//df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("file:///home/ec2-user/testData.csv")

initTable = df.select("key","brand","title","gender")

"""filteredDF = df.filter(df.gender == "male")
filteredDF.show()
seletdf=df.select("size")
seletdf.show()
df.registerTempTable("cloth")
dftordd = df.rdd
print(dftordd)
for line in dftordd.take(30):
   print(line)
"""

# **************************************************************************************************************
# 쓰레기 데이터 필터링
# **************************************************************************************************************

# //trash filter

dfToRdd = df.rdd


def filterError(row):
    columns = list(row)

    # //check title type is None
    if type(row.title) == type(None):
        return False

    # //check gender data
    if (row.gender != 'male') and (row.gender != 'female'):
        return False

        # //check if every size is zero
    sum = 0

    for index in range(columns.index(row.ShoulderWidth), len(columns)):
        if type(columns[index]) != int and type(columns[index]) != float:
            # //columns[index] = int(columns[index])
            return False
        sum += columns[index]

    if sum == 0:
        return False

    return True


clearRdd = dfToRdd.filter(filterError)
"""
for line in clearRdd.take(20):
    print line

"""


def splitByBlank(row):
    str = row.title.split(' ')
    value = 'null' if (len(str) < 2) else str[-2] + ' ' + str[-1]
    return Row(title=row.title, category=value)


studyDataRdd = clearRdd.map(splitByBlank)

studyDf = sqlContext.createDataFrame(studyDataRdd)
# ddf = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("categoryStudyData.csv")
# ddf.show()

# **************************************************************************************************************
# 데이터 전처
# **************************************************************************************************************


# //detail value organize filter funcition
import copy

# //userData Struct
userData = Row("key", "brand", "category", "title", "gender", "size", "material", "shoulder", "chest", "waist", "hip",
               "thigh", "group")


def sizeOrganizer(rdd):
    def parseSize(row):

        # //chest Length To Round
        chest = (row.BreastSide * 2) if (row.BreastSide != 0) else row.BreastPeripheral
        waist = (row.WaistSection * 2) if (row.WaistSection != 0) else row.WaistCircumference
        hip = (row.HipSection * 2) if (row.HipSection != 0) else row.HipCircumference
        thigh = (row.Thighsection * 2) if (row.Thighsection != 0) else row.ThighCircumference

        size = [row.ShoulderWidth, chest, waist, hip, thigh]

        for index in range(0, len(size)):
            size[index] = 0 if type(size[index]) == type(None) else size[index]

        sum = 0
        for value in size:
            sum += value

        newRow = userData(row.key, row.brand, row.category, row.title, row.gender, row.size, row.material,
                          round(size[0] / sum, 1), round(size[1] / sum, 1), round(size[2] / sum, 1),
                          round(size[3] / sum, 1), round(size[4] / sum, 1), "null")
        return newRow

    parseRdd = rdd.map(parseSize)

    return parseRdd


SizeOrgRdd = sizeOrganizer(clearRdd)
parseDf = sqlContext.createDataFrame(SizeOrgRdd)
filterDf = parseDf.filter(parseDf.shoulder != 0).filter(parseDf.chest != 0).filter(parseDf.waist == 0)

filterDf.show()

"""
for line in filterDf.take(20) :
    print(line)
"""
# **************************************************************************************************************
# 데이터 전처리 2 ; 데이터 같은 옷끼리 합치기
# **************************************************************************************************************

# //integration
def integration(rdd):
    # // map rdd to keyPairRdd
    pairRdd = rdd.map(lambda row: (row.key, row))

    # //set userData Type
    global userData

    # // create closer to run action 'combineByKey'
    def combiner(row):
        return userData(row.key, row.brand, row.category, row.title, row.gender, [row.size], row.material, row.shoulder,
                        row.chest, row.waist, row.hip, row.thigh, row.group)

    def mergeValue(row1, row2):
        return userData(row1.key, row1.brand, row1.category, row1.title, row1.gender, row1.size + [row2.size],
                        row1.material, row1.shoulder, row1.chest, row1.waist, row1.hip, row1.thigh, row1.group)

    def mergeTwoCombiners(row1, row2):
        return userData(row1.key, row1.brand, row1.category, row1.title, row1.gender, row1.size + row2.size,
                        row1.material, row1.shoulder, row1.chest, row1.waist, row1.hip, row1.thigh, row1.group)

        # //run action 'combineByKey'

    integRdd = pairRdd.combineByKey(combiner, mergeValue, mergeTwoCombiners)

    # //return rdd that maped key pair rdd to rdd
    return integRdd.map(lambda pair: pair[1])

integRdd = integration(SizeOrgRdd).persist()

"""
for line in integRdd.take(100):
    print line
"""

integDf=sqlContext.createDataFrame(integRdd)
#sqlContext.registerDataFrameAsTable(integDf,"integTable")
integTable = integDf.select("shoulder","chest","gender")
filterTable = filterDf.select("shoulder","chest","gender")
#sqlContext.registerDataFrameAsTable(table,"integTable")

#integDf.show()

#resultinteg = integDf.filter(integDf.gender == 'female' ).filter(integDf.shoulder <= 0.33).filter(integDf.shoulder >= 0.2)
#resultinteg.show()
"""
for line in resultinteg.take(100):
    print line
"""

# //material filtering

from collections import OrderedDict

materialCode = {'면/코튼': '00', '폴리우레탄': '01', '폴리에스터': '02', '나일론': '03', '레이온': '04', '모/울': '05', '캐시미어': '06',
                '아크릴': '07', '스판덱스': '08', '실크': '09', '마/린넨': '10', '앙고라': '11'}


def convertMaterialData(row):
    string = row.material
    mjson = json.loads(string)

    divide = 0
    if mjson[u'겉감'] != '':
        divide += 1
    if mjson[u'안감'] != '':
        divide += 1

    x = mjson[u'겉감'].items()
    y = mjson[u'안감'].items()
    z = sorted(x + y, key=lambda tuple: tuple[1], reverse=True)

    top = OrderedDict(z[0:3]).keys()
    codeList = map(lambda x: materialCode[x.encode('utf-8')], top)
    code = str(divide) + codeList[0] + codeList[1] + codeList[2]

    return userData(row.key, row.brand, row.category, row.title, row.gender, row.size, code, row.shoulder, row.chest,
                    row.waist, row.hip, row.thigh, row.group)

materialParsedRdd = integRdd.map(convertMaterialData)
"""
for row in materialParsedRdd.take(20):
    print row
"""


# //getter function

# //get the number of measruement
def getMeasureCount(row):
    rowToList = list(row)

    # //count measurement that is not zero
    count = 0
    print(rowToList.index(row.shoulder))
    for index in range(rowToList.index(row.shoulder), len(rowToList)):
        if rowToList[index] != 0:
            count += 1
    return count


# //get the number of size
def getSizeCount(row):
    return len(row.size)


"""

print getSizeCount(Row(s=1,size = [1,1,1,1,1]))
"""

# **************************************************************************************************************
# 옷 카테고리 예측
# **************************************************************************************************************

from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.linalg import Vectors

pdf = pd.read_csv('file:///home/ec2-user/data/parseData.csv',encoding='utf-8')
df = sqlContext.createDataFrame(pdf)

#df.show()

htf = HashingTF(10000)
categoryModel = NaiveBayesModel.load(sc, "target/tmp/parseModel")

# **************************************************************************************************************
# 분류
# **************************************************************************************************************

# labelDf.show()

def getGenderLabelCode(rdd, label):
    GenderRdd = rdd.map(lambda row: row.gender).distinct()

    def getGenderCode(rdd):
        dic = {'etc': 'e'}
        for feature in rdd.collect():
            uniToStr = str(feature)
            dic[uniToStr] = uniToStr[0]
        return dic

    label['gender'] = getGenderCode(GenderRdd)
    return label


# //get category
def getCategoryLabel(rdd, label):
    labelFile = pd.read_csv('labelCode.csv', encoding='utf-8')
    labelDf = sqlContext.createDataFrame(labelFile)
    labelDic = labelDf.rdd

    labelCode = {}
    for row in labelDic.collect():
        labelCode[row.title] = '{:03d}'.format(row.label)
    label['category'] = labelCode
    return label


# //To do
def getMaterialLabel(rdd, label):
    materialRdd = rdd.map(lambda row: row.material).distinct()

    return label


def getMeasureLabel(rdd, label):
    def selectMeasure(row):
        if row.category in [1, 2]:
            return (row.shoulder, row.chest, row.waist, row.hip, row.thigh)
        elif row.category in [3]:
            return (row.shoulder, row.chest, row.waist, row.hip, row.thigh)
        else:
            return (row.shoulder, row.chest, row.waist, row.hip, row.thigh)

    measureRdd = rdd.map(selectMeasure).distinct()

    def getCode(rdd):
        dic = {}
        index = 0
        for tuple in rdd.collect():
            index += 1
            tupleToStr = str(tuple)
            dic[tupleToStr] = '{:03d}'.format(index)
        return dic

    label['measure'] = getCode(measureRdd)
    return label


def classify(row):
    global label
    global userData
    group = label['gender'][row.gender] + '-'
    splitTitle = row.title.split()
    wtf = htf.transform(splitTitle)
    categoryCode = int(categoryModel.predict(wtf))
    group += str(categoryCode) + '-'
    SizeTuple = (row.shoulder, row.chest, row.waist, row.hip, row.thigh)
    SizeStr = str(SizeTuple)
    group += label['measure'][SizeStr] + '-'
    group += str(getSizeCount(row))

    return userData(row.key, row.brand, row.category, row.title, row.gender, row.size, row.material, row.shoulder,
                    row.chest, row.waist, row.hip, row.thigh, group)


label = {}

getCategoryLabel(materialParsedRdd, label)
getGenderLabelCode(materialParsedRdd, label)
getMeasureLabel(materialParsedRdd, label)

classifiedRdd = materialParsedRdd.map(classify)

classifiedDf = sqlContext.createDataFrame(classifiedRdd)
# classifiedDf.show()
resultTable = classifiedDf.select("key", "brand", "category", "gender", "size", "material", "shoulder", "chest",
                                  "waist", "hip", "thigh", "group")

# **************************************************************************************************************
# **** group label print ****
# **************************************************************************************************************


key = label.keys()
print '{:50}{:30}{:50}{:30}'.format(key[0], key[1], key[2], 'material')
value = label.values()
category = value[0].items()
gender = value[1].items()
measure = value[2].items()
material = materialCode.items()
for index in range(0, len(measure)):
    cValue = category[index] if index < len(category) else ''
    gValue = gender[index] if index < len(gender) else ''
    mValue = measure[index] if index < len(measure) else ''
    maValue = material[index] if index < len(material) else ''
    print '{:50}{:30}{:50}{:30}'.format(cValue, gValue, mValue, maValue)
