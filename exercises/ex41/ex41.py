import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('ex 41')
sc = SparkContext(conf=conf)


def generate_tuple(record):
    sensor_id = record.split(',')[0]
    pm10 = float(record.split(',')[2])
    if pm10 > 50.0:
        return (sensor_id, 1)
    else:
        return (sensor_id, 0)


k = int(sys.argv[1])
lines = sc.textFile('input/')
topk = lines.map(generate_tuple).reduceByKey(lambda v1, v2: v1+v2).map(lambda val: (val[1], val[0])).\
    sortByKey(ascending=False).top(k)
sc.parallelize(topk).coalesce(1).saveAsTextFile('output/')

sc.stop()
