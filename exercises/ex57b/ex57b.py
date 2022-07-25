from graphframes import *
from graphframes.lib import AggregateMessages
from pyspark.sql.functions import sum, count
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


conf = SparkConf().setAppName('ex 53')
sc = SparkContext(conf=conf)
ss = SparkSession.builder.getOrCreate()

ss.udf.register("generate_flag", lambda age: 1 if age < 35 else 0)

vertices = ss.read.load('input/vertexes.csv', format='csv', header=True, inferSchema=True).\
    selectExpr("id", "generate_flag(age) as flag")
edges = ss.read.load('input/edges.csv', format='csv', header=True, inferSchema=True).select("src", "dst")
graph = GraphFrame(vertices, edges)

send_age_to_source = AggregateMessages.dst["flag"]
my_count = graph.aggregateMessages(sum(AggregateMessages.msg), sendToSrc=send_age_to_source)
my_count.filter("sum(MSG) > 0").sort("id").withColumnRenamed("sum(MSG)", "numNeighborsLess35").\
    selectExpr("id", "int(numNeighborsLess35)").write.csv('output/', header=True)

sc.stop()
ss.stop()
