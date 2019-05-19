# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def separate(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)
    
lines = sc.textFile("file:///course_spark/class_12/fakefriends.csv")
rdd = lines.map(separate)
#lambda x: (x,1) é usado para que possa contar as ocorrências de cada idade depois
totalsByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0]/x[1])

results = averagesByAge.collect()
for result in results:
    print(result)