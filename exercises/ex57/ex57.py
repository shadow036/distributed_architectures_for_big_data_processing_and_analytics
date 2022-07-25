from graphframes import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName('ex 53')
sc = SparkContext(conf=conf)
ss = SparkSession.builder.getOrCreate()

ss.udf.register("hops", lambda distance: distance['u1'] if len(distance) > 0 else -1)

vertices = ss.read.load('input/vertexes.csv', format='csv', header=True, inferSchema=True).select("id", "name")
edges = ss.read.load('input/edges.csv', format='csv', header=True, inferSchema=True).select("src", "dst")
graph = GraphFrame(vertices, edges)

graph.shortestPaths(["u1"]).filter("id != 'u1'").selectExpr("name", "hops(distances) as numHops").\
    filter("numHops != -1").write.csv('output/', header=True)

sc.stop()
ss.stop()
