from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('ex 48')
sc = SparkContext(conf=conf)
ss = SparkSession.builder.getOrCreate()

ss.udf.register("rangeage", lambda age: ("[" + str(int(age/10)*10) + "-" + str(int(age/10)*10 + 9) + "]"))
profiles = ss.read.load('input/profiles.csv', format='csv', header=True, inferSchema=True)
profiles.createOrReplaceTempView("profiles")

new_profiles = ss.sql("select name, surname, rangeage(age) as rangeage from profiles")

new_profiles.write.csv('output_sql/', header=True)

sc.stop()
ss.stop()
