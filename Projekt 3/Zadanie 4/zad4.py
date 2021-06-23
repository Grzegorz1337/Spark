from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode


def reducer(row):
    key, values = row[0], row[1]
    ret = 0.0
    value_str = {}
    for i in values:
        if i[1] not in value_str:
            value_str[i[1]] = i[2]
        else:
            ret += value_str[i[1]] * i[2]
    if ret != 0.0:
        return [(row[0], ret)]
    return []


spark = SparkSession \
    .builder \
    .appName("Macierz") \
    .getOrCreate()

rows_B = 4
cols_B = 4
rows_A = 4
cols_A = 4

spark.sparkContext.setLogLevel("WARN")

m1 = spark.sparkContext.textFile("M1").map(lambda x: x.split(" ")).zipWithIndex() \
    .toDF()
m1_cells = m1.select(m1._2.alias("row"), explode(m1._1).alias("value")) \
    .rdd.zipWithIndex().map(lambda x: (x[0][0], x[1] % cols_A, x[0][1]))

m2 = spark.sparkContext.textFile("M2").map(lambda x: x.split(" ")).zipWithIndex() \
    .toDF()
m2_cells = m2.select(m2._2.alias("row"), explode(m2._1).alias("value")) \
    .rdd.zipWithIndex().map(lambda x: (x[0][0], x[1] % cols_B, x[0][1]))

matrixA = m1_cells \
    .flatMap(lambda x: [((int(x[0]), i), ('A', int(x[1]), float(x[2]))) for i in range(cols_B)])

matrixB = m2_cells \
    .flatMap(lambda x: [((i, int(x[1])), ('B', int(x[0]), float(x[2]))) for i in range(rows_A)])

result = matrixA.union(matrixB) \
    .groupByKey() \
    .flatMap(reducer)\
    .collect()

for i in result:
    print(i)
