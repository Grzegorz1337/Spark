#!/bin/python3
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, lit
from pyspark.sql.window import Window


spark = SparkSession \
    .builder \
    .appName("Macierz") \
    .getOrCreate()


w = Window.partitionBy(lit(1)).orderBy("value", "row")

m1 = spark.sparkContext.textFile("M1").map(lambda x: x.split(" ")).zipWithIndex() \
    .toDF()
m1_cells = m1.select(m1._2.alias("row"), explode(m1._1).alias("value")) \
    .rdd.zipWithIndex()

m2 = spark.sparkContext.textFile("M2").map(lambda x: x.split(" ")).zipWithIndex() \
    .toDF()
m2_cells = m2.select(m2._2.alias("row"), explode(m2._1).alias("value")) \
    .rdd.zipWithIndex()

first_step_m1 = m1_cells.map(lambda x: (x[1] - (x[0][0] * 4), ('A', x[0][0], int(x[0][1])))).sortBy(lambda y: y[0])
first_step_m2 = m2_cells.map(lambda x: (x[0][0], ('B', x[1] - (x[0][0] * 4), int(x[0][1])))).sortBy(lambda y: y[0])

second_step = first_step_m1.join(first_step_m2).map(lambda x: ((x[1][0][1],x[1][1][1]),x[1][0][2]*x[1][1][2]))\
    .sortByKey().reduceByKey(add).collect()

for i in second_step:
    print(i)
