from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from graphframes import *

conf = SparkConf().setAppName('lab 10')
sc = SparkContext(conf=conf)
ss = SparkSession.builder.getOrCreate()

airports = ss.read.load('input_lab10/airports.csv', format='csv', header=True, inferSchema=True). \
    select("id", "name", "city", "country", "iata", "latitude", "longitude")
airlines = ss.read.load('input_lab10/airlines.csv', format='csv', header=True, inferSchema=True). \
    select("airline_id", "name", "country", "icao")
routes = ss.read.load('input_lab10/routes.csv', format='csv', header=True, inferSchema=True). \
    selectExpr("airline_id", "airport_source_id as src", "airport_destination_id as dst"). \
    filter("src is not null and dst is not null")

airports = airports.withColumn("id", airports.id.cast("string"))
routes = routes.withColumn("src", routes.src.cast("string")).withColumn("dst", routes.dst.cast("string"))

flights = GraphFrame(airports, routes)

top10_indegree = flights.inDegrees.sort("inDegree", ascending=False).limit(10)
top10_indegree.show()

reachable_1 = flights.find("(src)-[route]->(dst)").filter("route.src = 1526 and route.dst != 1526"). \
    selectExpr("src.city as source_city", "route", "dst.city as destination_city")

reachable_2 = flights.find("(src)-[route1]->(int);(int)-[route2]->(dst)"). \
    filter("route1.src = 1526 and route2.dst != 1526 and route1.dst != route1.src and route2.dst != route2.src"). \
    selectExpr("src.city as source_city", "route1", "int.city as intermediate_city", "route2",
               "dst.city as destination_city")

reachable_3 = flights.find("(src)-[route1]->(int1);(int1)-[route2]->(int2);(int2)-[route3]->(dst)"). \
    filter("route1.src = 1526 and route3.dst != 1526 and "
           "route1.dst != route1.src and route2.dst != route2.src and "
           "route3.dst != route3.src and route3.dst != route2.src"). \
    selectExpr("src.city as source_city", "route1", "int1.city as intermediate_city1",
               "route2", "int2.city as intermediate_city2", "route3", "dst.city as destination_city")

print('\nREACHABLE WITH 1 FLIGHT: ', reachable_1.count(), '\n')
print('\nREACHABLE WITH 2 FLIGHT: ', reachable_2.count(), '\n')
print('\nREACHABLE WITH 3 FLIGHT: ', reachable_3.count(), '\n')

paths = flights.shortestPaths(['1526']).selectExpr("name", "city", "country", "distances[1526] as distance_from_Turin")\
    .filter("distance_from_Turin is not null").sort("distance_from_Turin", ascending=False).limit(1).show()

sc.setCheckpointDir('.')
connected_components_at_least_two_airports = flights.connectedComponents().groupBy("component").agg({"id": "count"}).withColumnRenamed("count(id)", "count").\
    filter("count > 2").count()
print(connected_components_at_least_two_airports)
