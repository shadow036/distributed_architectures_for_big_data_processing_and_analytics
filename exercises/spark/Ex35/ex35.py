from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('ex 31')
sc = SparkContext(conf=conf)

lines = sc.textFile('input/')

max_value = lines.map(lambda line: float(line.split(',')[2])).max()
lines.filter(lambda line: float(line.split(',')[2]) == max_value).map(lambda line: line.split(',')[1]).saveAsTextFile('output/')

sc.stop()