from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("Ratings")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///Users/Gabriel/Desktop/course_spark/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sortes(result.itens()))
for key, value in sortedResults.itens():
	print("%s %i" % (key,value))