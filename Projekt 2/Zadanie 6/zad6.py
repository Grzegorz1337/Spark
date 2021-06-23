#!/bin/python3

from __future__ import print_function
import sys
from operator import add
from pyspark.sql import SparkSession
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: zad6 <file> <col_name> [<col_name_2>...<col_name_n.]", file=sys.stderr)
        exit(-1)
    spark = SparkSession\
        .builder\
        .appName("Select columns")\
        .getOrCreate()
    columns = ""
    for i in range(2, len(sys.argv)):
        columns += sys.argv[i]
        columns += " "
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    countsR = lines.pipe('python3 ./mapper_z6.py '+columns)\
        .pipe('python3 ./reducer_z6.py')
    for line in countsR.collect():
        print(line)

    spark.stop()