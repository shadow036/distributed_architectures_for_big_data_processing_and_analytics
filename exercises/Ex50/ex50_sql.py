from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('ex 48')
sc = SparkContext(conf=conf)
ss = SparkSession.builder.getOrCreate()

ss.udf.register("name_surname", lambda name, surname: name + " " + surname)
profiles = ss.read.load('input/profiles.csv', format='csv', header=True, inferSchema=True)
profiles.createOrReplaceTempView("profiles")

new_profiles = ss.sql("select name_surname(name, surname) as name_surname from profiles")

new_profiles.write.csv('output_sql/', header=True)

sc.stop()
ss.stop()
