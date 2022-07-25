from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('ex 31')
sc = SparkContext(conf=conf)

lines = sc.textFile('input/')
print(lines.map(lambda line: float(line.split(',')[2])).top(3))

sc.stop()