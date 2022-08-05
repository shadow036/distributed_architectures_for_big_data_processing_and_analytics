from pyspark import SparkConf, SparkContext
from datetime import datetime


def extract_year(line):
    return int(line.split('-')[0].split('/')[0])


conf = SparkConf().setAppName('exam')
sc = SparkContext(conf=conf)


def compute_difference(date1, date2):
    d1 = datetime.strptime(date1.split('-')[0], "%Y/%m/%d")
    d2 = datetime.strptime(date2.split('-')[0], "%Y/%m/%d")
    return (d1-d2).days


def check_year(date):
    year = int(date.split('-')[0].split('/')[0])
    if year < 1995:
        return True
    else:
        return False


catalog = sc.textFile('input/catalog.txt')  # (item_id,name,category,first_time_in_catalog)
customers = sc.textFile('input/customers.txt')  # (username,name,surname,date_of_birth)
purchases = sc.textFile('input/purchases.txt')  # (date-time,username,item_id,price)

# PART 1
result = purchases.map(lambda line: ((line.split(',')[2], extract_year(line)), 1)).\
    reduceByKey(lambda one1, one2: one1 + one2).\
    filter(lambda my_tuple: 2010 <= my_tuple[0][1] <= 2019 and my_tuple[1] >= 150).\
    map(lambda my_tuple: (my_tuple[0][0], 1)).reduceByKey(lambda one1, one2: one1 + one2).\
    filter(lambda my_tuple: my_tuple[1] == 10).map(lambda my_tuple: my_tuple[0])

result.coalesce(1).saveAsTextFile('output_spark_part_1/')

# PART 2
itemId__category_firstTimeInCatalog = catalog.\
    map(lambda line: (line.split(',')[0], (line.split(',')[2], line.split(',')[3]))).\
    filter(lambda my_tuple: check_year(my_tuple[1][1]))
itemId_purchaseDate = purchases.map(lambda line: (line.split(',')[2], line.split(',')[0]))
result = itemId__category_firstTimeInCatalog.join(itemId_purchaseDate).\
    map(lambda my_tuple:
                 (my_tuple[0],
                  (my_tuple[1][0][0], 1 if compute_difference(my_tuple[1][1], my_tuple[1][0][1]) < 2990 else 0))).\
        reduceByKey(lambda my_tuple1, my_tuple2: (my_tuple1[0], my_tuple1[1] + my_tuple2[1])).\
    filter(lambda my_tuple: my_tuple[1][1] == 0).map(lambda my_tuple: (my_tuple[1][0], 1)).\
    reduceByKey(lambda one1, one2: one1 + one2).filter(lambda my_tuple: my_tuple[1] > 1)

result.coalesce(1).saveAsTextFile('output_spark_part_2/')
sc.stop()
