from pyspark import SparkConf, SparkContext


def filter_year(line):
    year = line.split(',')[2].split('/')[0]
    if int(year) == 2019:
        return True
    else:
        return False


def filter_year2(line):
    year = int(line.split(',')[2].split('/')[0])
    if 2019 >= year >= 2010:
        return True
    else:
        return False


def map_tuple(line):
    words = line.split(',')
    return (words[0], float(words[3]))


def map_tuple2(line):
    words = line.split(',')
    year = int(words[2].split('/')[0])
    return ((words[0], year), 1)


def reduce_maxPurchases(my_tuple1, my_tuple2):
    if my_tuple1[1] > my_tuple2[1]:
        return my_tuple1
    elif my_tuple1[1] == my_tuple2[1]:
        return (my_tuple1[0] + '+' + my_tuple2[0], my_tuple1[1])
    else:
        return my_tuple2


def flatMap_unpack(my_tuple):
    my_list = []
    model_id = my_tuple[1][0]
    if model_id.find('+') != -1:   # multiple max purchases
        for model in model_id.split('+'):
            my_list.append((model, 1))
        return my_list
    else:
        return [(model_id, 1)]


conf = SparkConf().setAppName('exam')
sc = SparkContext(conf=conf)

smartphone_models = sc.textFile('input/smartphone_models.txt')  # (model_id,model_name,brand)
purchases = sc.textFile('input/purchases.txt')  # (model_id,customer_id,date,price)

# PART 1
modelId_totalIncome = purchases.filter(filter_year).map(map_tuple).reduceByKey(lambda price1, price2: price1 + price2)
max_income = modelId_totalIncome.map(lambda my_tuple: my_tuple[1]).reduce(lambda price1, price2: max(price1, price2))
modelId_totalIncome.filter(lambda my_tuple: my_tuple[1] == max_income).map(lambda my_tuple: my_tuple[0]).\
    coalesce(1).saveAsTextFile('output_spark_part_1/')

# PART 2
purchases.filter(filter_year2).map(map_tuple2).reduceByKey(lambda one1, one2: one1 + one2).\
    map(lambda my_tuple: (my_tuple[0][1], (my_tuple[0][0], my_tuple[1]))).reduceByKey(reduce_maxPurchases).\
    flatMap(flatMap_unpack).reduceByKey(lambda one1, one2: one1 + one2).filter(lambda my_tuple: my_tuple[1] >= 2).\
    map(lambda my_tuple: my_tuple[0]).coalesce(1).saveAsTextFile('output_spark_part_2/')

sc.stop()
