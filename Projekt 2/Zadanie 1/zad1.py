#!/bin/python3

from __future__ import print_function
import sys
from operator import add
from pyspark.sql import SparkSession
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 zad1.py <file>", file=sys.stderr)
        exit(-1)
    spark = SparkSession\
        .builder\
        .appName("Union")\
        .getOrCreate()
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    countsR = lines.pipe('python3 ./mapper_z1.py')\
        .sortBy(lambda line: line.split("\t")[0])\
        .pipe('python3 ./reducer_z1.py')
    for line in countsR.collect():
        print(line)

    spark.stop()