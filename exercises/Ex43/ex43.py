import sys
from pyspark import SparkContext, SparkConf


def critical_flag(reading):
    words = reading.split(',')
    if int(words[5]) < threshold:
        return (words[0], (1, 1))
    else:
        return (words[0], (0, 1))


def critical_flag_timeslot(reading):
    words = reading.split(',')
    timeslot = int(int(words[2]) / 4)
    if int(words[5]) < threshold:
        return ((timeslot, words[0]), (1, 1))
    else:
        return ((timeslot, words[0]), (0, 1))


def filter1(reading):
    words = reading.split(',')
    if int(words[5]) == 0:
        return True
    else:
        return False


def id_timestamp(reading):
    words = reading.split(',')
    return ((words[0], words[1] + ", " + words[2] + ":" + words[3]), None)


def generate_neighbors_tuple(reading):
    my_list = []
    for nei in reading[1]:
        my_list.append((reading[0], nei))
    return my_list


def final_filter(reading):


conf = SparkConf().setAppName('ex 43')
sc = SparkContext(conf=conf)

neighbors = sc.textFile('input/neighbors.txt').map(lambda line: (line.split(',')[0], line.split(',')[1].split(' '))).\
    flatMap(generate_neighbors_tuple).map(lambda tuple: (tuple[1], tuple[0]))
readings = sc.textFile('input/readings.txt')

threshold = int(sys.argv[1])

station_critical = readings.map(critical_flag).reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1])).map(lambda tuple: (tuple[0], tuple[1][0]/tuple[1][1]))
station_critical.saveAsTextFile('output/criticality_user_input/')
station_critical.map(lambda tuple: (tuple[1], tuple[0])).filter(lambda tuple: tuple[0] > 0.8).sortByKey(ascending=False).saveAsTextFile('output/criticality_80/')
timeslot_station__critical = readings.map(critical_flag_timeslot).reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1])).map(lambda tuple: (tuple[0], tuple[1][0]/tuple[1][1]))
timeslot_station__critical.saveAsTextFile('output/criticality_timeslot_user_input/')
timeslot_station__critical.map(lambda tuple: (tuple[1], tuple[0])).filter(lambda tuple: tuple[0] > 0.8).sortByKey(ascending=False).saveAsTextFile('output/criticality_timeslot_80/')
id_timestamp_rdd = readings.filter(filter1).map(id_timestamp)  #((id, timestamp when full), None)
id_timestamp_neighbor = (neighbors.join(id_timestamp_rdd)).map(lambda tuple: ((tuple[1][0], tuple[1][1]), None))    #(id, timestamp_neighbor_when_full)
id_timestamp_rdd.join(id_timestamp_neighbor).map(lambda reading: (reading[0], 1)).filter(final_filter)
sc.stop()