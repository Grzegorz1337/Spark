from __future__ import print_function

import operator
import sys
import re
from operator import add
from pyspark.sql import SparkSession
from sys import stdin
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize


def Map(r):
    lem = WordNetLemmatizer()
    sw = set(open("stopwords.txt", "r").readlines())
    p = re.sub(r'[^\w\s]', '', r[0]).lower()
    tokens = word_tokenize(p)
    no_sw_text = [(lem.lemmatize(word), 1) for word in tokens if word not in sw]
    return no_sw_text


def Reduce(r):
    return r[0], sum(r[1])


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    spark = SparkSession \
        .builder \
        .appName("PythonWordCount") \
        .getOrCreate()
    lines = spark.read.text(sys.argv[1]).rdd
    countsR = lines.flatMap(Map)\
        .groupByKey() \
        .map(Reduce)

    for i in countsR.collect():
        print(i)

    spark.stop()
