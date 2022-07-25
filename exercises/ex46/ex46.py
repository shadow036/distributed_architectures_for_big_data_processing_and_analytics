from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('ex 46')
sc = SparkContext(conf=conf)


def generate_windows(reading):
    array = []
    for i in range(max(min_timestamp, reading[0] - 120), min(max_timestamp + 60, reading[0] + 60), 60):
        array.append((i, (reading[0], reading[1])))
    return array


def sort_criterion(value):
    return value[0]


readings = sc.textFile('input/').map(lambda reading: (int(reading.split(',')[0]), float(reading.split(',')[1])))

min_timestamp = int(readings.map(lambda reading: reading[0]).takeOrdered(1)[0])
max_timestamp = int(readings.map(lambda reading: reading[0]).top(3)[2])
compute = readings.flatMap(generate_windows).groupByKey().map(lambda l: sorted(list(l[1]), key=sort_criterion)) \
    .filter(lambda l: l[0][1] < l[1][1] < l[2][1]).map(lambda l: str(l[0][0]) + ',' + str(l[0][1]) + ',' + str(l[1][0])
                                                                 + ',' + str(l[1][1]) + ',' + str(l[2][0]) + ','
                                                                 + str(l[2][1]))
compute.coalesce(1).saveAsTextFile('output/')

sc.stop()
