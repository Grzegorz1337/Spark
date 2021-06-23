#!/bin/python3
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import explode
from operator import add

from pyspark.sql.functions import lit

buf = 0


def multiply(d):
    res = []
    res.append(int(d[1]) - int(d[0][1]['_2']) * 50)
    m_row_sum = 0
    for i in range(0, len(d[0][1]['_1'])):
        m_row_sum += (float(d[0][0].split(" ,")[i]) * float(d[0][1]['_1'][i]))
    res.append(m_row_sum)
    return res


spark = SparkSession \
    .builder \
    .appName("Join") \
    .getOrCreate()

w = Window().orderBy(lit('A'))
vector = spark.sparkContext.textFile("v*")
matrix = spark.sparkContext.textFile("M*")

vectors = vector.map(lambda x: x.replace(" ", "").split(",")).zipWithIndex()
matrixes = matrix.map(lambda x: x.replace("[[", "").replace("]]", "").split("],["))

rdd_temp = matrixes.zip(vectors).toDF()

mapped_data = rdd_temp.select(explode(rdd_temp._1), rdd_temp._2).rdd.zipWithIndex()
multiplied_data = mapped_data.map(lambda x: multiply(x)).sortBy(lambda x: x[0]).reduceByKey(add).collect()

for i in multiplied_data:
    print(i)
