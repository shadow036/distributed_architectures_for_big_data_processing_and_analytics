from graphframes import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName('ex 53')
sc = SparkContext(conf=conf)
ss = SparkSession.builder.getOrCreate()

vertices = ss.read.load('input/vertexes.csv', format='csv', header=True, inferSchema=True)
edges = ss.read.load('input/edges.csv', format='csv', header=True, inferSchema=True)
graph = GraphFrame(vertices, edges)

friend_follower = graph.find("(a)-[friend]->(b);(b)-[not_friend]->(a)").filter("friend.linktype == 'friend' and not_friend.linktype != 'friend'").selectExpr("a.id as IdFriend", "b.id as IdNotFriend")
friend_nothing = graph.find("(a)-[friend]->(b);!(b)-[]->(a)").filter("friend.linktype == 'friend'").selectExpr("a.id as IdFriend", "b.id as IdNotFriend")
friend_follower.union(friend_nothing).write.csv('output/', header=True)

sc.stop()
ss.stop()
