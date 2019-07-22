from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("Custumer Orders")
sc = SparkContext(conf = conf)

def quebra(texto):
    result = texto.split(',')
    idade = result[0]
    qtd = result[2]
    return (int(idade), float(qtd))

lines = sc.textFile("file:///Users/Gabriel/Desktop/Cursos/Curso Spark/Spark Basics/Customer Orders/customer-orders.csv")
mapedpair = lines.map(quebra)

totalpair = mapedpair.reduceByKey(lambda x,y: x + y)
trocado = totalpair.map(lambda x: (x[1], x[0]))

pairSorted = trocado.sortByKey()
results = pairSorted.collect()

for result in results:
        print(result)