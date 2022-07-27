from pyspark import SparkContext, SparkConf


def reduce_final(my_tuple1, my_tuple2):
    if my_tuple1[1] > my_tuple2[1]:
        return my_tuple1
    elif my_tuple2[1] > my_tuple1[1]:
        return my_tuple2
    else:
        return (my_tuple1[0] + ',' + my_tuple2[0], my_tuple2[1])


def flatMap_final(my_tuple):
    if my_tuple[0].find(',') == -1:
        return [my_tuple[0]]
    else:
        return my_tuple[0].split(',')


conf = SparkConf().setAppName('exam')
sc = SparkContext(conf=conf)

production_plants = sc.textFile('input/production_plants.txt')  # (productionPlantId,city,country)
robots = sc.textFile('input/robots.txt')    # (robotId,productionPlantId,IPAddress)
failures = sc.textFile('input/failures.txt')    # (robotId,failureId,date,time)

# PART 1
productionPlantId_city = production_plants.filter(lambda line: line.split(',')[2] == 'Italy').\
    map(lambda line: (line.split(',')[0], line.split(',')[1]))
productionPlantId_one = robots.map(lambda line: (line.split(',')[1], 1))
city_numberOfRobots = productionPlantId_city.join(productionPlantId_one).\
    map(lambda my_tuple: (my_tuple[1][0], my_tuple[1][1])).\
    reduceByKey(lambda one1, one2: one1 + one2)
max_robots = city_numberOfRobots.map(lambda my_tuple: my_tuple[1]).max()
city_numberOfRobots.filter(lambda my_tuple: my_tuple[1] == max_robots).map(lambda my_tuple: my_tuple[0]).\
    coalesce(1).saveAsTextFile('output_spark_part_1/')

# PART 2
robotId_numberOfFailures = failures.map(lambda line: (line.split(',')[0], 1)).\
    reduceByKey(lambda one1, one2: one1 + one2)
robotId_productionPlantId = robots.map(lambda line: (line.split(',')[0], line.split(',')[1]))
productionPlantId_numberOfFailures = robotId_productionPlantId.join(robotId_numberOfFailures).\
    map(lambda my_tuple: (my_tuple[1][0], my_tuple[1][1])).reduceByKey(lambda v1, v2: max(v1, v2))
missing_production_plants = production_plants.map(lambda line: line.split(',')[0]).\
    subtract(productionPlantId_numberOfFailures.map(lambda my_tuple: my_tuple[0])).map(lambda plant_id: (plant_id, 0))
productionPlantId_numberOfFailures2 = productionPlantId_numberOfFailures.union(missing_production_plants)
productionPlantId_numberOfFailures2.coalesce(1).saveAsTextFile('output_spark_part_2/')

sc.stop()