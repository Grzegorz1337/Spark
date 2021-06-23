import sys
from pyspark.sql import SparkSession

def multiply(r,d):
    if int(r[1]) in d:
        return [(int(r[0]), float(r[2]) * d[int(r[1])])]
    return []

if len(sys.argv) != 2:
    print("Zle argumenty")
    exit(1)

sp = SparkSession.builder.appName("zad1").getOrCreate()
file = sys.argv[1]

v_num = 4

dane = sp.read.csv(file, header=False, sep=' ').rdd

res = {}

for i in range(v_num):
    res[i] = float(1.0 / v_num)

print(res)

for i in range(50):
    res = dane.flatMap(lambda row: multiply(row, res))\
        .reduceByKey(lambda x, y: x + y).collectAsMap()
    print("Result at iteration ", i+1, ": ", res)



