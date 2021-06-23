#!/bin/python3

from __future__ import print_function
import sys
from operator import add
from pyspark.sql import SparkSession
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 zad7.py <file> <group_by_column_name>  <agregation_function>(<agragation_col_name>)", file=sys.stderr)
        print("Aggregation functions supported: COUNT, SUM, MIN, MAX, AVG")
        exit(-1)
    spark = SparkSession\
        .builder\
        .appName("Group and agregate")\
        .getOrCreate()
    columns = sys.argv[2:]
    buf = columns[1].replace(')','').split('(')
    columns.pop(1)
    columns += buf
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    countsR = lines.pipe('python3 ./mapper_z7.py '+columns[0] + " " + columns[2]) \
        .sortBy(lambda line: line.split("\t")[0]) \
        .pipe('python3 ./reducer_z7.py ' + columns[1])
    for line in countsR.collect():
        print(line)

    spark.stop()