import sys
from pyspark import SparkConf, SparkContext 
from pyspark.sql import SparkSession
from datetime import datetime

conf = SparkConf().setAppName('lab 7')
sc = SparkContext(conf = conf)

def my_filter(line):
    l = line.split('\t')
    if l[0].startswith('station') == True or (l[2] == '0' and l[3] == '0'):
        return False
    else:
        return True

def station_timestamp__flag(station):
    data = station.split('\t')
    station_id = data[0]
    timestamp = data[1]
    datetimeObject = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    day_of_the_week = datetimeObject.strftime("%a")
    hour = datetimeObject.hour
    new_timestamp = day_of_the_week + ' - ' + str(hour)
    free_slots = int(data[3])
    numerator = 0
    if free_slots == 0:
        numerator = 1
    return ((int(station_id), new_timestamp), (numerator,1))

def station__timeslot_criticality(tuple):
    return (tuple[0][0], (tuple[0][1], tuple[1]))

def choose_max_criticality(t1, t2):
    compare = t1[1] - t2[1]
    if compare > 0:
        return t1
    elif compare < 0:
        return t2
    else:
        compare2 = int(t1[0].split(' - ')[1]) - int(t2[0].split(' - ')[1])
        if compare2 < 0:
            return t1
        elif compare2 > 0:
            return t2
        elif t1[0].split(' - ')[0] < t2[0].split(' - ')[0]:
            return t1
        else:
            return t2

def stationsId__coordinates(line):
    data = line.split('\t')
    station_id = data[0]
    longitude = data[1]
    latitude = data[2]
    return (int(station_id), latitude + ', ' + longitude)

def clean_tuple(line):
    return (line[0], [line[1][0][0].split(' - ')[0], int(line[1][0][0].split(' - ')[1]), line[1][0][1], float(line[1][1].split(', ')[1]), float(line[1][1].split(', ')[0])])

def generate_kml_format(result):
    return '<Placemark><name>' + str(result[0]) + '</name><ExtendedData><Data name="DayWeek"><value>' + result[1][0] + '</value></Data><Data name="Hour"><value>' + str(result[1][1]) + '</value></Data><Data name="Criticality"><value>' + str(result[1][2]) + '</value></Data></ExtendedData><Point><coordinates>' + str(result[1][3]) + ',' + str(result[1][4]) + '</coordinates></Point></Placemark>'

if __name__ == '__main__':
    threshold = float(sys.argv[1])
    stations = sc.textFile('input_lab7/stations.csv')
    register = sc.textFile('input_lab7/registerSample.csv')
    stations_criticality = register.filter(my_filter).map(station_timestamp__flag).reduceByKey(lambda t1,t2:(t1[0]+t2[0], t1[1]+t2[1])).mapValues(lambda tuple: float(tuple[0]/tuple[1]))
    stations_minimum_criticality = stations_criticality.filter(lambda l:l[1] > threshold)
    print('\n', stations_minimum_criticality.collectAsMap(), '\n')
    stations_critical_timeslots = stations_minimum_criticality.map(station__timeslot_criticality).reduceByKey(lambda t1, t2: choose_max_criticality(t1, t2))
    print('\n', stations_critical_timeslots.collect(), '\n')
    stations_info = stations.filter(lambda l:l.startswith('id') == False).map(stationsId__coordinates)
    print('\n', stations_info.collect(), '\n')
    kml = stations_critical_timeslots.join(stations_info).map(clean_tuple).map(generate_kml_format).coalesce(1)
    print('\n', kml.collect(), '\n')
    kml.saveAsTextFile('output_lab_7/')
    sc.stop()