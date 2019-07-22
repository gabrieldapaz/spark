# -*- coding: utf-8 -*-
#Friends by Age

from pyspark import SparkConf, SparkContext

#Criando o objeto de configuração e o objeto contexto
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

#pegando a string(linha) da base e separando e pegando a idade e o numero de amigos
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("file:///Users/Gabriel/Desktop/Cursos/Curso Spark/Spark Basics/Friends by Age/fakefriends.csv")
rdd = lines.map(parseLine)
#(33, 385) -> (33,(385,1)) e depois soma ex: (33, (734, 2))
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()

for result in results:
    print(result)
