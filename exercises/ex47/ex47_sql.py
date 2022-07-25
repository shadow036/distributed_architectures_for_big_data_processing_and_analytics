from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('ex 47')
sc = SparkContext(conf=conf)
ss = SparkSession.builder.getOrCreate()

people = ss.read.load('input/persons.csv', format='csv', header=True, inferSchema=True)
people.createOrReplaceTempView("people")
new_people = ss.sql("select name, age + 1 as new_age from people where gender = 'male' order by new_age desc, name asc ")
new_people.write.csv('output_sql/', header=True)

ss.stop()