#!/bin/python3
from sys import stdin
key = ""
value = 0
pom = 1
for i in stdin:
    if pom == 1:
        key = i.split('\t')[0]
        value = int(i.split('\t')[1])
        pom = 0
    elif i.split('\t')[0] == key:
        value += int(i.split('\t')[1])
    else:
        print(key,"\t",value)
        key = i.split('\t')[0]
        value = int(i.split('\t')[1])

print(key, "\t", value)
