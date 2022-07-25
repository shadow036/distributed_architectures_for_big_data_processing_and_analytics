from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('ex 31')
sc = SparkContext(conf=conf)

lines = sc.textFile('input/')

max_value = float(lines.map(lambda line: line.split(',')[2]).max())
target = lines.filter(lambda line: True if (float(line.split(',')[2]) == max_value) else False)

target.saveAsTextFile('output/')

sc.stop()