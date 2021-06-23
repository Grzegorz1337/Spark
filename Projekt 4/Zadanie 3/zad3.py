import sys
from pyspark.sql import SparkSession

if len(sys.argv) != 3:
    print("Zle argumenty, pierwszym powien byc plik z macierza, a drugim plik ze startowym wektorem rang")
    exit(1)

sp = SparkSession.builder.appName("zad1").getOrCreate()
matrix = sys.argv[1]
vector = sys.argv[2]

v_num = 4
beta = 0.8

dane = sp.read.csv(matrix, header=False, sep=' ').rdd
v = sp.read.csv(vector, sep=' ').rdd.flatMap(lambda r: [(int(r[0]),float(r[1]))])

res = {}

for i in range(v_num):
    res[i] = float(1.0 / v_num)

for i in range(0,50):
    v = dane.flatMap(lambda r: [(int(r[1]), (int(r[0]), float(r[2])))]).join(v)\
        .map(lambda x: (x[1][0][0], beta * float(x[1][0][1]) * float(x[1][1])))\
        .reduceByKey(lambda x, y: x + y)\
        .map(lambda x: (x[0], x[1] + (1-beta) * 1/v_num))

print(v.collectAsMap())

