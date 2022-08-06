from pyspark import SparkConf, SparkContext


def pairs(line):
    words = line.split(',')
    sensor_id = words[0]
    date = words[1]
    return (sensor_id, (date, float(words[2])))


def concatenate(v1, v2):
    if v2 == '':
        return v1
    elif v1 == '' and v2 != '':
        return v2
    else:
        return v1 + ',' + v2


def generate_list(my_string):
    new_list = []
    for date in my_string.split(','):
        new_list.append(date)
    if new_list[0] == '':
        new_list = []
    return new_list


conf = SparkConf().setAppName('ex 31')
sc = SparkContext(conf=conf)

lines = sc.textFile('input/')   # (sensor_id,date,pm10)
result = lines.map(lambda line: (line.split(',')[0], (line.split(',')[1] if float(line.split(',')[2]) > 50 else '')))\
    .reduceByKey(lambda v1, v2: concatenate(v1, v2)).map(lambda my_tuple: (my_tuple[0], generate_list(my_tuple[1])))
result.saveAsTextFile('output/')

sc.stop()