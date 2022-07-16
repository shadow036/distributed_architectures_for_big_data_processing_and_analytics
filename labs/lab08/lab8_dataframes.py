#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 16 16:11:37 2022

@author: shadow36
"""

import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName('lab 8 dataframes')
sc = SparkContext(conf = conf)
ss = SparkSession.builder.getOrCreate()

def check(free_slots):
	if free_slots == 0:
		return 1
	else:
		return 0

ss.udf.register("check", lambda free_slots: check(free_slots))

if __name__ == '__main__':
	par = sys.argv[1]
	stations = ss.read.option("delimiter", "\\t").load('input_lab8/stations.csv', format='csv', header=True, inferSchema=True)
	register = ss.read.option("delimiter", "\\t").load('input_lab8/registerSample.csv', format='csv', header=True, inferSchema=True)
	filtered_register = register.filter("used_slots > 0 or free_slots > 0")
	free_slots_flag = filtered_register.selectExpr("station","date_format(timestamp,'EE') as weekday", "hour(timestamp) as hour","check(free_slots) as critical")
	criticality = free_slots_flag.groupBy("station","weekday","hour").agg({"critical":"sum","station":"count"}).withColumnRenamed("sum(critical)", "sum").withColumnRenamed("count(station)", "count").selectExpr("station","weekday","hour","sum/count as criticality")
	remove_station_name = stations.selectExpr("id","longitude","latitude")
	threshold = criticality.filter("criticality > " + str(par))
	coordinates = threshold.join(remove_station_name, threshold.station == remove_station_name.id).select("station","weekday","hour","criticality","longitude","latitude")
	sorted = coordinates.sort(coordinates.criticality.desc(), coordinates.station.asc(), coordinates.weekday.asc(), coordinates.hour.asc())
	sorted.write.csv('output_lab8/dataframes/', header=True)
	sc.stop()
	ss.stop()

