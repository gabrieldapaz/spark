# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
import collections

""" Primeiro eh criado um objeto de configuracao que esse ira
criar um contexto spark, que então ira criar um RDD object"""
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

#aqui voce pode importar da nuvem ou de um hdf5
lines = sc.textFile("file:///Users/Gabriel/Desktop/Cursos/Curso Spark/Ratings Counter/ml-100k/u.data")
#aplicando metodos de transformacao no RDD
ratings = lines.map(lambda x: x.split()[2])
""" Ate o momento nada foi executado, apenas spark criando os caminhos atraves
de grafos e procurando o melhor caminho"""
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
