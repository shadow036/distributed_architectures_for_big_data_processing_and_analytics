from pyspark import SparkContext
import sys

sc = SparkContext()


def check_maximum(cpu):
    if cpu > 90:
        return [1, 0]
    elif cpu < 10:
        return [0, 1]
    else:
        return [0, 0]


performance_log = sc.textFile('input/performance_log.txt')  # (date,time,server_id,cpu_usage,ram_usage)
cpu_thr = float(sys.argv[1])
ram_thr = float(sys.argv[2])

# PART 1
may2018 = performance_log.\
    filter(lambda line: line.split(',')[0].split('/')[0] == '2018' and line.split(',')[0].split('/')[1] == '05').cache()
result1 = may2018.\
    map(lambda line: ((line.split(',')[2], line.split(',')[1].split(':')[0]),
                      (float(line.split(',')[3]), float(line.split(',')[4]), 1))).\
    reduceByKey(lambda tuple1, tuple2: (tuple1[0] + tuple2[0], tuple1[1] + tuple2[1], tuple1[2] + tuple2[2])).\
    map(lambda my_tuple: (my_tuple[0], (my_tuple[1][0]/ my_tuple[1][2], my_tuple[1][1]/my_tuple[1][2]))).\
    filter(lambda my_tuple: my_tuple[1][0] > cpu_thr and my_tuple[1][1] > ram_thr).map(lambda my_tuple: my_tuple[0])
result1.coalesce(1).saveAsTextFile('output_spark_part_1/')

# PART 2
result2 = may2018.\
    map(lambda line: ((line.split(',')[2], line.split(',')[0] + ' - ' + line.split(',')[1].split(':')[0]),
                      float(line.split(',')[3]))).reduceByKey(lambda v1, v2: max(v1, v2)).\
    map(lambda my_tuple: (my_tuple[0], check_maximum(my_tuple[1]))).\
    map(lambda my_tuple: ((my_tuple[0][0], my_tuple[0][1].split(' - ')[0]), my_tuple[1])).\
    reduceByKey(lambda v1, v2: [v1[0] + v2[0], v1[1] + v2[1]]).\
    filter(lambda my_tuple: my_tuple[1][0] >= 8 and my_tuple[1][1] >= 8).map(lambda my_tuple: my_tuple[0])
result2.coalesce(1).saveAsTextFile('output_spark_part_2/')

sc.stop()
