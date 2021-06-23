from __future__ import print_function
import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("PythonTransitiveClosure") \
        .getOrCreate()

    if len(sys.argv) != 4:
        print("Bad command, usage: py main.py routes.csv task_number number_of_transfers ")
        exit(-1)

    file = sys.argv[1]
    # 0 means no transfers, so we search graph once to find direct connections
    transfer_number = int(sys.argv[2]) + 1
    task = int(sys.argv[3])

    available_flights = spark.read.csv(file, header=True, sep=',').rdd

    start_cities = {
        1: ['YYZ'],
        2: ['WAW', 'BZG'],
        3: ['YYZ'],
        4: ['WAW', 'BZG']
    }[task]

    visited = set(start_cities)

    for i in range(0, transfer_number):
        new_cities = available_flights.filter(lambda x: x['Source airport'] in visited) \
            .map(lambda x: x['Destination airport']) \
            .groupBy(lambda x: x) \
            .map(lambda x: x[0]).collect()
        for j in new_cities:
            visited.add(j)
        available_flights = available_flights.filter(lambda x: x['Destination airport'] not in visited)

    print(visited)
    spark.stop()
