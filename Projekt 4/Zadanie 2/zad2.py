import sys
from pyspark.sql import SparkSession

def multiply(r, d, b):
    if int(r[1]) in d:
        return [(int(r[0]), b * (float(r[2]) * d[int(r[1])]))]
    return []

if len(sys.argv) != 2:
    print("Zle argumenty")
    exit(1)

sp = SparkSession.builder.appName("zad1").getOrCreate()
file = sys.argv[1]

v_num = 4
beta = 0.8

dane = sp.read.csv(file, header=False, sep=' ').rdd

res = {}

for i in range(v_num):
    res[i] = float(1.0 / v_num)


for i in range(50):
    res = dane.flatMap(lambda row: multiply(row, res, beta))\
        .reduceByKey(lambda x, y: x + y)\
        .map(lambda x: (x[0], x[1] + (1-beta) * 1/v_num)).collectAsMap()
    print("Result at iteration ", i+1, ": ", res)



