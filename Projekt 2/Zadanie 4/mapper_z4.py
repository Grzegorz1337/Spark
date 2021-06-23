#!/bin/python3
import re

from sys import stdin

for i in stdin:
    record = i.replace("\n", "").split('\t')
    if record[0] == "Orders":
        key = record.pop(2)
        print(key, "\t", record)
    else:
        key = record.pop(1)
        print(key, "\t", record)
