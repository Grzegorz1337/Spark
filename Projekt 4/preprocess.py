import sys

f = open("macierz1")
matrix = []
index = 0
lines = f.readlines()

for line in lines:
    matrix.append([])
    matrix[index] = [int(x) for x in line.split(' ')]
    index+=1

v = []

for i in range(0,4):
    v.append(0)
    for j in range(0,4):
        v[i] += matrix[j][i]

for i in range(0,4):
    v.append(0)
    for j in range(0,4):
        matrix[i][j] = float(matrix[i][j]/v[j])

res = open("macierz_rzadka.txt",mode='w')

for row in matrix:
    for col in row:
        res.write(str(col)+" ")
    res.write('\n')