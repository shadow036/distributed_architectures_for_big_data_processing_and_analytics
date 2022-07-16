import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName('lab 8 sql')
sc = SparkContext(conf = conf)
ss = SparkSession.builder.getOrCreate()

def check(value):
	if value == 0:
		return 1
	else:
		return 0

ss.udf.register("critical", lambda value: check(value))

if __name__ == '__main__':
	par = sys.argv[1]
	stations = ss.read.option("delimiter", "\\t").load('input_lab8/stations.csv', format='csv', header=True, inferSchema=True)
	register = ss.read.option("delimiter", "\\t").load('input_lab8/registerSample.csv', format='csv', header=True, inferSchema=True)
	register.createOrReplaceTempView("register")
	stations.createOrReplaceTempView("stations")
	filtered_register = ss.sql("SELECT * from register WHERE used_slots > 0 OR free_slots > 0")
	filtered_register.createOrReplaceTempView("filtered_register")
	critical_flag = ss.sql("SELECT station, date_format(timestamp,'EE') as weekday, hour(timestamp) as hour, critical(free_slots) as critical FROM filtered_register")
	critical_flag.createOrReplaceTempView("critical_flag")
	criticality = ss.sql("SELECT station,weekday,hour,SUM(critical)/COUNT(station) FROM critical_flag GROUP BY station, weekday, hour").withColumnRenamed("(SUM(critical) / COUNT(station))", "criticality_value")
	criticality.createOrReplaceTempView("criticality")
	threshold = ss.sql("SELECT * FROM criticality WHERE criticality_value > " + par)
	threshold.createOrReplaceTempView("threshold")
	joined = ss.sql("SELECT station, weekday, hour, criticality_value, longitude, latitude FROM threshold,stations WHERE station == id ORDER BY criticality_value DESC, station, weekday, hour")
	joined.createOrReplaceTempView("joined")
	joined.write.csv('output_lab8/sql/', header=True)
	ss.stop()
	sc.stop()
