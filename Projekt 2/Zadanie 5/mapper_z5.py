#!/bin/python3
import sys
from sys import stdin

def transformColumn(v):
    for i in range(0,len(v)):
        try:
            v[i] = float(v[i])
        except:
            v[i] = str(v[i])
    return v


colnames = []
reqCol = sys.argv[1]
operator = sys.argv[2]
conditionValue = sys.argv[3]

try:
    conditionValue = float(conditionValue)
except:
    conditionValue = str(conditionValue)

# TODO Transformacja kolumny której dotyczy warunek + usunięcie znaku $ + zmiana numeru kolumny na jej nazwę
for i in stdin:
    value = i.replace("\n", "").replace("$", "").split(",")
    if value[0] == 'id':
        colnames = value
        continue
    value = transformColumn(value)
    result = {
        '=': lambda x: x == conditionValue,
        '!=': lambda x: x != conditionValue,
        '<': lambda x: x < conditionValue,
        '<=': lambda x: x <= conditionValue,
        '>': lambda x: x > conditionValue,
        '>=': lambda x: x >= conditionValue
    }[operator](value[colnames.index(reqCol)])
    if result:
        print(value)
