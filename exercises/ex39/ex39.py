from pyspark import SparkConf, SparkContext


def pairs(line):
    words = line.split(',')
    sensor_id = words[0]
    date = words[1]
    return (sensor_id, [date])


conf = SparkConf().setAppName('ex 31')
sc = SparkContext(conf=conf)

lines = sc.textFile('input/')
lines.filter(lambda line: float(line.split(',')[2]) > 50.0).map(pairs).reduceByKey(lambda v1, v2: v1 + v2)\
    .saveAsTextFile('output/')

sc.stop()
