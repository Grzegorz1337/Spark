#!/bin/python3
import re
import sys
from sys import stdin

colnames = []
reqColumns = sys.argv[1:]

for i in stdin:
    record = i.replace("\n", "").replace("$","").split(',')
    selectedColumns = []
    if record[0] == 'id':
        colnames = record
        continue
    key = record[colnames.index(reqColumns[0])]
    value = record[colnames.index(reqColumns[1])]
    print(key, "\t", value)