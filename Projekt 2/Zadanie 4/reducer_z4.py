#!/bin/python3

def print_cross(key, value):
    orders = []
    customers = []
    for i in value:
        if "Customers" in i:
            customers.append(i)
        elif "Orders" in i:
            orders.append(i)
    for i in orders:
        for j in customers:
            print(key, "\t", i, "\t", j)


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
        print_cross(key, value)
        value = []
        key = i.split('\t')[0]
        value.append(i.split('\t')[1].replace("\n", ""))

print_cross(key, value)
