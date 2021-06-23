#!/bin/python3
import operator
import numpy as np
from pyspark.sql import SparkSession


def multiply(x, v_in):
    res = 0
    for i in range(0, len(v_in)):
        res += float(x.value.split(" ")[i + 1]) * v_in[i]
    return [x.value.split(" ")[0], res]


# Generujemy wektor z losowymi wartościami, tu jest statycznie 14, bo macierz w pliku ma 14 kolumn
v_in = np.random.random(size=14)

spark = SparkSession \
    .builder \
    .appName("Macierze") \
    .getOrCreate()

lines = spark.read.text("matrix.txt").rdd

M_rdd = lines.map(lambda x: multiply(x, v_in)) \
    .reduceByKey(operator.add).collect()

for line in M_rdd:
    print(line)
