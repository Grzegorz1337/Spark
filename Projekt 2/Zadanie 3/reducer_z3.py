#!/bin/python3
from sys import stdin
key = ""
value = []

firstRow = stdin.readline()
key = firstRow.split('\t')[0]
value.append(firstRow.split('\t')[1].replace("\n", ""))

for i in stdin:
    if i.split('\t')[0] == key:
        value.append(i.split('\t')[1].replace("\n", ""))
    else:
        if ' B' not in value and ' A' in value:
            print(key,"\t",value)
        value = []
        key = i.split('\t')[0]
        value.append(i.split('\t')[1].replace("\n", ""))

if ' B' not in value and ' A' in value:
    print(key, "\t", value)
