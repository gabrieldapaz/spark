from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("PopularMovies").getOrCreate()

nameDict = loadMovieNames()

lines = spark.sparkContext.textFile("file:///SparkCourse/ml-100k/u.data")

movies = lines.map(lambda x: Row(movieID =int(x.split()[1])))
movieDataset = spark.createDataFrame(movies)

topMovieIDs = movieDataset.groupBy("movieID").count().orderBy("count", ascending=False).cache()

topMovieIDs.show()


top10 = topMovieIDs.take(10)


print("\n")
for result in top10:
   .
    print("%s: %d" % (nameDict[result[0]], result[1]))


spark.stop()
