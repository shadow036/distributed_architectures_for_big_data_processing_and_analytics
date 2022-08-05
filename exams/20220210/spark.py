from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('exam')
sc = SparkContext(conf=conf)

apps = sc.textFile('input/apps.txt')    # (app_id,app_name,price,category,company)
users = sc.textFile('input/users.txt')  # (user_id,name,surname,nationality)
actions = sc.textFile('input/actions.txt')  # (user_id,app_id,date-time,action)


def actual_compare(list1, list2, index, my_tuple1, my_tuple2):
    if list1[index] > list2[index]:
        return my_tuple1
    elif list1[index] < list2[index]:
        return my_tuple2
    elif index < 5:
        return actual_compare(list1, list2, index+1, my_tuple1, my_tuple2)
    else:
        return my_tuple1


def find_last(my_tuple1, my_tuple2):
    year1, month1, day1 = [int(i) for i in my_tuple1[0].split('-')[0].split('/')]
    year2, month2, day2 = [int(i) for i in my_tuple2[0].split('-')[0].split('/')]
    hour1, minutes1, seconds1 = [int(i) for i in my_tuple1[0].split('-')[1].split(':')]
    hour2, minutes2, seconds2 = [int(i) for i in my_tuple2[0].split('-')[1].split(':')]
    list1 = [year1, month1, day1, hour1, minutes1, seconds1]
    list2 = [year2, month2, day2, hour2, minutes2, seconds2]
    return actual_compare(list1, list2, 0, my_tuple1, my_tuple2)


# PART 1
appId_userId = actions.\
    map(lambda line: (line.split(',')[0], (line.split(',')[1], line.split(',')[2], line.split(',')[3]))).\
    filter(lambda my_tuple: my_tuple[1][2] == 'Install' and my_tuple[1][1].split('-')[0].split('/')[0] == '2022').\
    map(lambda my_tuple: (my_tuple[1][0], my_tuple[0]))
appId_priceFlag = apps.map(lambda line: (line.split(',')[0], 1 if float(line.split(',')[2]) > 0 else 0))
result1 = appId_userId.join(appId_priceFlag).\
    map(lambda my_tuple: ((my_tuple[0], my_tuple[1][0]), (my_tuple[1][1], 1 - my_tuple[1][1]))).\
    reduceByKey(lambda tuple1, tuple2: tuple1).map(lambda my_tuple: (my_tuple[0][1], my_tuple[1])).\
    reduceByKey(lambda my_tuple1, my_tuple2: (my_tuple1[0] + my_tuple2[0], my_tuple1[1] + my_tuple2[1])).\
    filter(lambda my_tuple: my_tuple[1][0] > my_tuple[1][1]).map(lambda my_tuple: (my_tuple[0], my_tuple[1][0]))
result1.coalesce(1).saveAsTextFile('output_spark_part_1/')

# PART 2
userId_italian = users.map(lambda line: (line.split(',')[0], line.split(',')[3])).filter(lambda my_tuple: my_tuple[1] == 'Italian')
userId__appId_dateTime_action = actions.map(lambda line: (line.split(',')[0], (line.split(',')[1], line.split(',')[2], line.split(',')[3])))
userId_numberOfCurrentlyInstalledApps = userId_italian.join(userId__appId_dateTime_action).\
    map(lambda my_tuple: ((my_tuple[0], my_tuple[1][1][0]), (my_tuple[1][1][1], my_tuple[1][1][2]))).\
    reduceByKey(lambda my_tuple1, my_tuple2: find_last(my_tuple1, my_tuple2)).filter(lambda my_tuple: my_tuple[1][1] == 'Install').map(lambda my_tuple: (my_tuple[0][0], 1)).reduceByKey(lambda v1, v2: v1+v2)
maxNumberOfCurrentlyInstalledApps = userId_numberOfCurrentlyInstalledApps.map(lambda my_tuple: my_tuple[1]).max()
result2 = userId_numberOfCurrentlyInstalledApps.filter(lambda my_tuple: my_tuple[1] == maxNumberOfCurrentlyInstalledApps).\
    map(lambda my_tuple: my_tuple[0])
result2.coalesce(1).saveAsTextFile('output_spark_part_2/')
sc.stop()
