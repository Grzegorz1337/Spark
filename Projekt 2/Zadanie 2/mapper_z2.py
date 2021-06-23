#!/bin/python3
import re

from sys import stdin

for i in stdin:
    key = i.split()[0]
    value = int(i.split()[1])
    print(value, "\t", key)
