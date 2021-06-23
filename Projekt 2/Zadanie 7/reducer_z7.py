#!/bin/python3
import sys
from sys import stdin

def parseVal(r):
    try:
        r = float(r)
    except:
        r = str(r)
    return r


aggrFunc = sys.argv[1]
key = ""
value = []

firstRow = stdin.readline().replace("\n", "").split()
key = firstRow[0]
value.append(parseVal(firstRow[1]))

#TODO Dodać odpowiednią konwersję tablicy value w wypadku gdy elementy są typów numerycznych
for i in stdin:
    if i.split()[0] == key:
        value.append(parseVal(i.split()[1].replace("\n", "")))
    else:
        try:
            result = {
                'COUNT': lambda x: len(x),
                'MIN': lambda x: min(x),
                'MAX': lambda x: max(x),
                'SUM': lambda x: sum(x),
                'AVG': lambda x: sum(x)/len(x)
            }[aggrFunc](value)
            print(key, "\t", result)
        except:
            print("Unable to execute this aggregation function for specified column type")
        value = []
        key = i.split()[0]
        value.append(parseVal(i.split()[1].replace("\n", "")))

try:
    result = {
        'COUNT': lambda x: len(x),
        'MIN': lambda x: min(x),
        'MAX': lambda x: max(x),
        'SUM': lambda x: sum(x),
        'AVG': lambda x: sum(x) / len(x)
    }[aggrFunc](value)
    print(key, "\t", result)
except:
    print("Unable to execute this aggregation function for specified column type")
