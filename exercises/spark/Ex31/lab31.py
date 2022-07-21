from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('ex 31')
sc = SparkContext(conf=conf)

lines = sc.textFile('input/')
lines.filter(lambda line: line.find('google') != -1).map(lambda line: line.split(' - - ')[0]).distinct().\
    saveAsTextFile('output/')

sc.stop()