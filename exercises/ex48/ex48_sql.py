from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('ex 48')
sc = SparkContext(conf=conf)
ss = SparkSession.builder.getOrCreate()

people = ss.read.load('input/persons.csv', format='csv', header=True, inferSchema=True)

people.createOrReplaceTempView("people")
new_people = ss.sql("select name, avg(age) from people group by name having count(name) > 1")

new_people.write.csv('output_sql/')

sc.stop()
ss.stop()