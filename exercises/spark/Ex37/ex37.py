from pyspark import SparkConf, SparkContext


def pairs(line):
    words = line.split(',')
    sensor_id = words[0]
    pm10 = float(words[2])
    return (sensor_id, pm10)


conf = SparkConf().setAppName('ex 31')
sc = SparkContext(conf=conf)

lines = sc.textFile('input/')
lines.map(pairs).reduceByKey(lambda v1, v2: max(v1, v2)).saveAsTextFile('output/')

sc.stop()