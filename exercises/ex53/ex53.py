from graphframes import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName('ex 53')
sc = SparkContext(conf=conf)
ss = SparkSession.builder.getOrCreate()

vertices = ss.read.load('input/vertexes.csv', format='csv', header=True, inferSchema=True)
edges = ss.read.load('input/edges.csv', format='csv', header=True, inferSchema=True)
graph = GraphFrame(vertices, edges)

intermediate = graph.filterEdges('linktype == "follow"').inDegrees
max_followers = intermediate.agg({"inDegree": "max"}).withColumnRenamed("max(inDegree)", "numFollowers")
joined = intermediate.join(max_followers, intermediate.inDegree == max_followers.numFollowers)
joined.select("id", "numFollowers").write.csv("output/", header=True)

sc.stop()
ss.stop()
