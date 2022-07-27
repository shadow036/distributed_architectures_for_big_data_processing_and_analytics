from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('exam')
sc = SparkContext(conf=conf)


def reduceByKey_sum(tuple1, tuple2):
    return (tuple1[0] + tuple2[0], tuple1[1] + tuple2[1])


def extract_year(date):
    return int(date.split('/')[0])


def flatMap_windows(my_tuple):
    if my_tuple[0][1] > 1980:
        tuple1 = (my_tuple[0], (my_tuple[1], 0))
        tuple2 = ((my_tuple[0][0], my_tuple[0][1]-1), (my_tuple[1], 1))
        return [tuple1, tuple2]
    else:
        return [(my_tuple[0], (my_tuple[1], 0))]


car_models = sc.textFile('input/car_models.txt')    # (model_id,model_name,manufacturer)
sales_eu = sc.textFile('input/sales_eu.txt')    # (sale_id,car_id,model_id,date,country,price)
sales_extra_eu = sc.textFile('input/sales_extra_eu.txt')    # (sale_id,car_id,model_id,date,country,price)

# PART 1
modelId_none = car_models.map(lambda line: (line.split(',')[0], line.split(',')[2])).\
    filter(lambda my_tuple: my_tuple[1] == 'FIAT').\
    map(lambda my_tuple: (my_tuple[0], None))
modelId__one_price = sales_eu.map(lambda line: (line.split(',')[2], (line.split(',')[4], line.split(',')[5]))).\
    filter(lambda my_tuple: my_tuple[1][0] == 'Italy').map(lambda my_tuple: (my_tuple[0], (1, int(my_tuple[1][1]))))
modelId__one_price = modelId_none.join(modelId__one_price).map(lambda my_tuple: (my_tuple[0], my_tuple[1][1]))
modelId__numberOfSales_averagePrice = modelId__one_price.reduceByKey(reduceByKey_sum).\
    map(lambda my_tuple: (my_tuple[0], (my_tuple[1][0], my_tuple[1][1]/my_tuple[1][0]))).\
    filter(lambda my_tuple: my_tuple[1][0] > 10 and my_tuple[1][1] > 5000)
result = modelId__numberOfSales_averagePrice.map(lambda my_tuple: my_tuple[0])

result.coalesce(1).saveAsTextFile('output_spark_part_1/')

# PART 2

modelId_year__one1 = sales_eu.map(lambda line: ((line.split(',')[2], extract_year(line.split(',')[3])), 1))
modelId_year__one2 = sales_extra_eu.map(lambda line: ((line.split(',')[2], extract_year(line.split(',')[3])), 1))
modelId_windowYear__maxSales_sortFlag = modelId_year__one1.union(modelId_year__one2).\
    reduceByKey(lambda one1, one2: one1 + one2).\
    flatMap(flatMap_windows)
modelId_windowYear__numberOfSales1_numberOfSales2 = modelId_windowYear__maxSales_sortFlag.\
    join(modelId_windowYear__maxSales_sortFlag).\
    filter(lambda my_tuple: my_tuple[1][0][1] == 0 and my_tuple[1][1][1] == 1).\
    map(lambda my_tuple: (my_tuple[0][0], (1 if my_tuple[1][0][0] - my_tuple[1][1][0] < 0 else 0))).\
    reduceByKey(lambda flag1, flag2: flag1 * flag2).filter(lambda my_tuple: my_tuple[1] == 1).\
    map(lambda my_tuple: my_tuple[0])

modelId_windowYear__numberOfSales1_numberOfSales2.coalesce(1).saveAsTextFile('output_spark_part_2/')

sc.stop()
