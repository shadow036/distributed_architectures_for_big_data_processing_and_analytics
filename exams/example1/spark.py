from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('exam')
sc = SparkContext(conf=conf)


def check_subcategory(subcategory):
    if subcategory == 'taxi':
        return [True, False]
    elif subcategory == 'busstop':
        return [False, True]
    else:
        return [False, False]


def check_subcategory2(subcategory):
    if subcategory == 'museum':
        return 1
    else:
        return 0


pois = sc.textFile('input/pois.txt')

# PART 1
city__italy_subcategory = pois.map(lambda line: (line.split(',')[3], (line.split(',')[4], line.split(',')[6]))).\
    filter(lambda my_tuple: my_tuple[1][0] == 'Italy').cache()
result1 = city__italy_subcategory.map(lambda my_tuple: (my_tuple[0], check_subcategory(my_tuple[1][1]))).\
    reduceByKey(lambda tuple1, tuple2: (tuple1[0] or tuple2[0], tuple1[1] or tuple2[1])).\
    filter(lambda my_tuple: my_tuple[1][0] is True and my_tuple[1][1] is False).map(lambda my_tuple: my_tuple[0])
result1.coalesce(1).saveAsTextFile('output_spark_part_1/')

# PART 2
city_numberOfMuseums = city__italy_subcategory.map(lambda my_tuple: (my_tuple[0], check_subcategory2(my_tuple[1][1]))).\
    reduceByKey(lambda v1, v2: v1 + v2).cache()
number_of_italian_cities = city_numberOfMuseums.count()
number_of_italian_museums = city_numberOfMuseums.map(lambda my_tuple: my_tuple[1]).sum()
average_number_of_museums = number_of_italian_museums/number_of_italian_cities
result2 = city_numberOfMuseums.filter(lambda my_tuple: my_tuple[1] > average_number_of_museums).\
    map(lambda my_tuple: my_tuple[0])
result2.coalesce(1).saveAsTextFile('output_spark_part_2/')