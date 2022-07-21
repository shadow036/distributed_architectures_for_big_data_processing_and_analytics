from pyspark import SparkConf, SparkContext


def pairs(line):
    words = line.split(',')
    sensor_id = words[0]
    pm10 = float(words[2])
    if pm10 > 50.0:
        return (sensor_id, 1)
    else:
        return (sensor_id, 0)


conf = SparkConf().setAppName('ex 31')
sc = SparkContext(conf=conf)

lines = sc.textFile('input/')

lines.map(pairs).reduceByKey(lambda v1, v2: v1+v2).filter(lambda sensor: sensor[1] > 1).saveAsTextFile('output/')

sc.stop()