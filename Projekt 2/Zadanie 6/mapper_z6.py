#!/bin/python3
import re
import sys
from sys import stdin

colnames = []
reqColumns = sys.argv[1:]

for i in stdin:
    record = i.replace("\n", "").split(',')
    selectedColumns = []
    if record[0] == 'id':
        colnames = record
        continue
    for i in range(0,len(record)):
        if colnames[i] in reqColumns:
            selectedColumns.append(record[i])
    print(selectedColumns)
