from graphframes import *
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('ex 52')
sc = SparkContext(conf=conf)
ss = SparkSession.builder.getOrCreate()

vertices = ss.read.load('input/vertexes.csv', format='csv', header=True, inferSchema=True)
edges = ss.read.load('input/edges.csv', format='csv', header=True, inferSchema=True)

vertices = vertices.withColumn("id", vertices.id.cast("string"))
edges = edges.withColumn("src", edges.src.cast("string")).withColumn("dst", edges.dst.cast("string"))
graph = GraphFrame(vertices, edges)

output = graph.filterEdges("linktype = 'follow'").dropIsolatedVertices().inDegrees.selectExpr("id", "inDegree as numFollowers")

output.write.csv("output/", header=True)

sc.stop()
ss.stop()
