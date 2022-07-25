from graphframes import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName('ex 53')
sc = SparkContext(conf=conf)
ss = SparkSession.builder.getOrCreate()

vertices = ss.read.load('input/vertexes.csv', format='csv', header=True, inferSchema=True)
edges = ss.read.load('input/edges.csv', format='csv', header=True, inferSchema=True)
graph = GraphFrame(vertices, edges)

graph.find("(user)-[follow]->(topic); (topic)-[correlated]->(big_data)").\
    filter("user.entityType == 'user' and follow.linkType == 'follow' and topic.entityType == 'topic' and "
           "correlated.linkType == 'correlated' and big_data.entityType == 'topic' and big_data.name == 'Big Data'").\
    selectExpr("user.name as username").write.csv('output/', header=True)

sc.stop()
ss.stop()
