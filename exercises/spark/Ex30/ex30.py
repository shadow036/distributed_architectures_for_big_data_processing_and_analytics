from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('ex 30')
sc = SparkContext(conf=conf)

lines = sc.textFile('input/')
target = lines.filter(lambda line: line.find('google') != -1)
target.saveAsTextFile('output/')

sc.stop()