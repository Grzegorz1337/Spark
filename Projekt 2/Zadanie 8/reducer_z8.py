#!/bin/python3

import sys
from sys import stdin


def cross_print(key, value, method):
    orders = []
    customers = []
    rightReq = False
    leftReq = False
    for i in value:
        if "Customers" in i:
            rightReq = True
            customers.append(i)
        elif "Orders" in i:
            leftReq = True
            orders.append(i)
    if method == 'LEFT':
        if not leftReq:
            return
    elif method == 'RIGHT':
        if not rightReq:
            return
    elif method == 'INNER':
        if not leftReq or not rightReq:
            return
    if len(orders) == 0:
        for j in customers:
            print(key, "\t", j)
    elif len(customers) == 0:
        for i in orders:
            print(key, "\t", i)
    else:
        for i in orders:
            for j in customers:
                print(key, "\t", i, "\t", j)


method = sys.argv[1]
key = ""
value = []
pom = 1

firstRow = stdin.readline()
key = firstRow.split('\t')[0]
value.append(firstRow.split('\t')[1].replace("\n", ""))

for i in stdin:
    if i.split('\t')[0] == key:
        value.append(i.split('\t')[1].replace("\n", ""))
    else:
        cross_print(key, value, method)
        value = []
        key = i.split('\t')[0]
        value.append(i.split('\t')[1].replace("\n", ""))

cross_print(key,value,method)