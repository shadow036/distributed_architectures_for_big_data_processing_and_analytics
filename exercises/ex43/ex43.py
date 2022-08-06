import sys
from pyspark import SparkContext, SparkConf

sc = SparkContext()


def timeslot_mapper(hour):
    my_list = ['0-3', '4-7', '8-11', '12-15', '16-19', '20-23']
    return my_list[int(int(hour)/4)]


def generate_list(line):
    new_list = []
    station = line.split(',')[0]
    neighbors = line.split(',')[1].split(' ')
    for n in neighbors:
        new_list.append((n, station))
    return new_list


neighbors = sc.textFile('input/neighbors.txt')  # (station id, list of neighbors separated by a space)
readings = sc.textFile('input/readings.txt')    # (station_id,date,hour,minute,number of bikes,number of free slots)

threshold = int(sys.argv[1])

# PART 3
result3 = readings.map(lambda line: (line.split(',')[0], (1 if int(line.split(',')[5]) < threshold else 0, 1))).\
    reduceByKey(lambda t1, t2: (t1[0] + t2[0], t1[1] + t2[1])).map(lambda t: (t[0], 100 * t[1][0]/t[1][1]))
result3.coalesce(1).saveAsTextFile('output_part_3/')

# PART 4
result3.filter(lambda t: t[1] > 80).sortBy(lambda t: t[1], ascending=False).coalesce(1).saveAsTextFile('output_part_4/')

# PART 5
result5 = readings.map(lambda line: ((line.split(',')[0], timeslot_mapper(line.split(',')[2])),
                      (1 if int(line.split(',')[5]) < threshold else 0, 1))).\
    reduceByKey(lambda t1, t2: (t1[0] + t2[0], t1[1] + t2[1])).map(lambda t: (t[0], 100 * t[1][0]/t[1][1])).coalesce(1)
result5.saveAsTextFile('output_part_5/')

# PART 6
result5.filter(lambda t: t[1] > 80).sortBy(lambda t: t[1], ascending=False).saveAsTextFile('output_part_6/')

# PART 7
stationId__timestamp_flag = readings.map(lambda line: (line.split(',')[0],
                           (line.split(',')[1] + ' at ' + line.split(',')[2] + ':' + line.split(',')[3],
                            int(line.split(',')[5])))).map(lambda t: (t[0], (t[1][0], 0 if t[1][1] == 0 else 1)))
neighbor_station = neighbors.flatMap(lambda line: generate_list(line))
stationId__timestamp_flag.join(neighbor_station).map(lambda t: (t[1][1], t[1][0])).join(stationId__timestamp_flag).\
    filter(lambda t: t[1][0][0] == t[1][1][0]).map(lambda t: ((t[0], t[1][0][0]), (t[1][0][1], t[1][1][1]))).\
    reduceByKey(lambda t1, t2: (t1[0] + t2[0], t1[1] + t2[1])).filter(lambda t: t[1][0] + t[1][1] == 0).\
    coalesce(1).saveAsTextFile('output_part_7/')

sc.stop()
