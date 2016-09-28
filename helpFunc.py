from pyspark.sql import SQLContext

def print_rdd(rdd, take = 0):
    if take == 0:
        for x in rdd.collect():
            print x

    else:
        for x in rdd.take(take):
            print x


def show_rdd(rdd, sqlContext):
    df = sqlContext.createDataFrame(rdd)
    df.show()

def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False
