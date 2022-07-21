from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('ex 31')
sc = SparkContext(conf=conf)

lines = sc.textFile('input/')

values = lines.map(lambda line: float(line.split(',')[2]))
print(values.sum()/values.count())

sc.stop()