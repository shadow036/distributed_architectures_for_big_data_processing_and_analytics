from pyspark import SparkConf, SparkContext
import datetime


def previous_date(date):
    values = date.split('/')
    return (datetime.date(int(values[0]), int(values[1]), int(values[2])) - datetime.timedelta(days=1)).\
        strftime("%Y/%m/%d")


def extract_year(date):
    return int(date.split('/')[0])


def failures_filter(line):
    words = line.split(',')
    year = extract_year(words[0])
    if words[3] == 'Engine' and (year == 2017 or year == 2018):
        return True
    else:
        return False


def failures_map(line):
    words = line.split(',')
    year = extract_year(words[0])
    if year == 2017:
        return (words[2], (1, 0))
    else:
        return (words[2], (0, 1))


def failures_map2(line):
    words = line.split(',')
    return [((words[2], words[0]), words[0]), ((words[2], previous_date(words[0])), words[0])]


conf = SparkConf().setAppName('exam')
sc = SparkContext(conf=conf)

cars = sc.textFile('input/cars.txt')    # car_id,model,company,city
car_failures = sc.textFile('input/car_failures.txt')    # date,time,car_id,failure_type

# part 1
car_model_pair = cars.map(lambda car: (car.split(',')[0], car.split(',')[1]))
result = car_failures.filter(failures_filter).map(failures_map).\
    reduceByKey(lambda tuple0, tuple1: (tuple0[0] + tuple1[0], tuple0[1] + tuple1[1])).\
    filter(lambda my_tuple: (my_tuple[1][1] > my_tuple[1][0])).join(car_model_pair)\
    .map(lambda my_tuple: (my_tuple[0], my_tuple[1][1]))

result.coalesce(1).saveAsTextFile('output_spark_1/')

# part 2
failures_window = car_failures.flatMap(failures_map2)
result2 = failures_window.join(failures_window).filter(lambda my_tuple: my_tuple[1][0] != my_tuple[1][1]).groupByKey().\
    map(lambda my_tuple: my_tuple[0])

result2.coalesce(1).saveAsTextFile('output_spark_2/')

sc.stop()
