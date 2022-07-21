from pyspark import SparkConf, SparkContext


def pairs(line):
    words = line.split(',')
    return (words[0], 1)


conf = SparkConf().setAppName('ex 31')
sc = SparkContext(conf=conf)

lines = sc.textFile('input/')
lines.filter(lambda line: float(line.split(',')[2]) > 50.0).map(pairs).reduceByKey(lambda v1, v2: v1 + v2).\
    map(lambda my_tuple: (my_tuple[1], my_tuple[0])).sortByKey(ascending=False).saveAsTextFile('output/')

sc.stop()
