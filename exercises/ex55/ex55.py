from graphframes import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName('ex 53')
sc = SparkContext(conf=conf)
ss = SparkSession.builder.getOrCreate()

vertices = ss.read.load('input/vertexes.csv', format='csv', header=True, inferSchema=True)
edges = ss.read.load('input/edges.csv', format='csv', header=True, inferSchema=True)
graph = GraphFrame(vertices, edges)

graph.find("(username)-[followed]->(topic)").filter("followed.linktype='follow' and topic.entityName = 'topic'").\
    selectExpr("username.name as username", "topic.name as topic").write.csv('output/', header=True)


sc.stop()
ss.stop()
