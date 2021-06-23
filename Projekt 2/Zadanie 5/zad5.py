#!/bin/python3

from __future__ import print_function
import sys
from operator import add
from pyspark.sql import SparkSession
if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: select <file> <column_name> <condition operator> <value>", file=sys.stderr)
        print("Condition operators: = != < <= > >=")
        exit(-1)
    spark = SparkSession\
        .builder\
        .appName("Selection")\
        .getOrCreate()
    task = sys.argv[2] + " " + sys.argv[3] + " " + sys.argv[4]
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    countsR = lines.pipe('python3 ./mapper_z5.py ' + task)\
        .pipe('python3 ./reducer_z5.py')
    for line in countsR.collect():
        print(line)
    #kolumna = < > <= >= wartość
    spark.stop()