from pyspark import SparkConf, SparkContext


def pairs(line):
    words = line.split(',')
    sensor_id = words[0]
    date = words[1]
    return (sensor_id, (date, float(words[2])))


conf = SparkConf().setAppName('ex 31')
sc = SparkContext(conf=conf)

lines = sc.textFile('input/')
lines.map(pairs).foldByKey([], lambda l1, l2: l1 + [l2[1]])
#v1 + [v2[0]] if v2[1] > 50.0 else v1
sc.stop()