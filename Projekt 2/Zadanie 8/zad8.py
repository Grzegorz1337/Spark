#!/bin/python3

from __future__ import print_function
import sys
from operator import add
from pyspark.sql import SparkSession
if __name__ == "__main__":
    print("Program executes sql command: SELECT * FROM TableA A LEFT|RIGHT|INNER|OUTER JOIN TableB B ON A.id = B.id")
    print("Table A is first table in input file")
    print("Table B is second table in input file")
    if len(sys.argv) != 3:
        print("Usage: zad8 <file> <INNER|OUTER|LEFT|RIGHT>", file=sys.stderr)
        exit(-1)
    spark = SparkSession\
        .builder\
        .appName("Join")\
        .getOrCreate()
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    countsR = lines.pipe('python3 ./mapper_z8.py')\
        .sortBy(lambda line: line.split("\t")[0])\
        .pipe('python3 ./reducer_z8.py ' + sys.argv[2])
    for line in countsR.collect():
        print(line)

    spark.stop()