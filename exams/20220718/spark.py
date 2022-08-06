from pyspark import SparkContext
from datetime import datetime, timedelta

sc = SparkContext()

production_plants = sc.textFile('input/production_plants.txt')  # (plant_id,city,country)
robots = sc.textFile('input/robots.txt')    # (robot_id,plant_id,ip_address)
out_of_orders = sc.textFile('input/out_of_orders.txt')  # (robot_id,date,cause)


def previous_date(date, delta):
    date = datetime.strptime(date, "%Y/%m/%d")
    return datetime.strftime(date - timedelta(days=delta), "%Y/%m/%d")


def compare(tuple1, tuple2):
    if tuple1[1] > tuple2[1]:
        return tuple1
    elif tuple1[1] < tuple2[1]:
        return tuple2
    else:
        return (tuple1[0] + ',' + tuple2[0], tuple1[1])


def my_split(my_tuple):
    if my_tuple[1][0].find(',') != -1:
        new_list = []
        dates = my_tuple[1][0].split(',')
        for date in dates:
            new_list.append((my_tuple[0], (date, my_tuple[1][1])))
        return new_list
    else:
        return [my_tuple]


# PART 1
robotId__failures2021_failures2020 = out_of_orders.\
    filter(lambda line: 2021 >= int(line.split(',')[1].split('/')[0]) >= 2020).\
    map(lambda line: ((line.split(',')[0], line.split(',')[1]),
                      [1, 0] if line.split(',')[1].split('/')[0] == '2021' else [0, 1])).reduceByKey(lambda v1, v2: v1).\
    map(lambda my_tuple: (my_tuple[0][0], my_tuple[1])).reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1])).\
    filter(lambda my_tuple: my_tuple[1][0] > my_tuple[1][1])
plantId_city = production_plants.map(lambda line: (line.split(',')[0], line.split(',')[1]))
plantId_robotId = robots.map(lambda line: (line.split(',')[1], line.split(',')[0]))
robotId_city = plantId_robotId.join(plantId_city).map(lambda my_tuple: (my_tuple[1][0], my_tuple[1][1]))
result1 = robotId__failures2021_failures2020.join(robotId_city).map(lambda my_tuple: (my_tuple[0], my_tuple[1][1]))
result1.coalesce(1).saveAsTextFile('output_spark_part_1/')

# PART 2
robotId_plantId = robots.map(lambda line: (line.split(',')[0], line.split(',')[1]))
robotId_date = out_of_orders.map(lambda line: (line.split(',')[0], line.split(',')[1]))
result2 = robotId_plantId.join(robotId_date).map(lambda my_tuple: ((my_tuple[1][0], my_tuple[1][1], my_tuple[0]), 1)).\
    flatMap(lambda my_tuple: [my_tuple,
                              ((my_tuple[0][0], previous_date(my_tuple[0][1], 1), my_tuple[0][2]), 1),
                              ((my_tuple[0][0], previous_date(my_tuple[0][1], 2), my_tuple[0][2]), 1)
                              ])\
    .reduceByKey(lambda v1, v2: v1).map(lambda my_tuple: ((my_tuple[0][0], my_tuple[0][1]), 1)).\
    reduceByKey(lambda v1, v2: v1 + v2).map(lambda my_tuple: (my_tuple[0][0], (my_tuple[0][1], my_tuple[1]))).\
    reduceByKey(lambda tuple1, tuple2: compare(tuple1, tuple2)).flatMap(lambda my_tuple: my_split(my_tuple)).\
    map(lambda my_tuple: ((my_tuple[0], my_tuple[1][0]), my_tuple[1][1]))
result2.coalesce(1).saveAsTextFile('output_spark_part_2/')

sc.stop()
