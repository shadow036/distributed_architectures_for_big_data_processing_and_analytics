from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('exam')
sc = SparkContext(conf=conf)


def map_higherPriceFlag(my_tuple):
    if float(my_tuple[1][1]) > float(my_tuple[1][0][1]):
        return ((my_tuple[0], my_tuple[1][0][0]), (1, 1))
    else:
        return ((my_tuple[0], my_tuple[1][0][0]), (0, 1))


items = sc.textFile('input/items.txt')  # (item_id,name,recommended_price,category)
customers = sc.textFile('input/customers.txt')  # (username,name,surname,date_of_birth)
advertisements = sc.textFile('input/advertisements.txt')    # (date-time,username,item_id,boolean,price)

# PART 1
itemId__category_recommendedPrice = items.\
    map(lambda line: (line.split(',')[0], (line.split(',')[3], line.split(',')[2])))
itemId_salePrice = advertisements.map(lambda line: (line.split(',')[2], (line.split(',')[3], line.split(',')[4]))).\
    filter(lambda my_tuple: my_tuple[1][0] == 'true').map(lambda my_tuple: (my_tuple[0], my_tuple[1][1]))
itemId__category_recommendedPrice.join(itemId_salePrice).map(map_higherPriceFlag).\
    reduceByKey(lambda tuple1, tuple2: (tuple1[0] + tuple2[0], tuple1[1] + tuple2[1])).\
    map(lambda my_tuple: (my_tuple[0], my_tuple[1][0]/my_tuple[1][1])).filter(lambda my_tuple: my_tuple[1] > 0.9).\
    map(lambda my_tuple: (my_tuple[0][0], my_tuple[0][1])).coalesce(1).saveAsTextFile('output_spark_part_1/')

# PART 2

allItems = items.map(lambda line: (line.split(',')[0]))
itemId_category = items.map(lambda line: (line.split(',')[0], line.split(',')[3]))
advertisedItems = advertisements.map(lambda line: (line.split(',')[2])).distinct()
category_numberOfUnadvertisedItems = allItems.subtract(advertisedItems).map(lambda item: (item, None)).\
    join(itemId_category).\
    map(lambda my_tuple: (my_tuple[1][1], 1)).reduceByKey(lambda one1, one2: one1 + one2).\
    filter(lambda my_tuple: my_tuple[1] >= 2)
category_numberOfLowProfitItems = advertisements.map(lambda line: (line.split(',')[2], float(line.split(',')[4]))).\
    reduceByKey(lambda price1, price2: price1 + price2).join(itemId_category).\
    filter(lambda my_tuple: 0 < my_tuple[1][0] < 100).\
    map(lambda my_tuple: (my_tuple[1][1], 1)).reduceByKey(lambda one1, one2: one1 + one2).\
    filter(lambda my_tuple: my_tuple[1] >= 2)
category_numberOfUnadvertisedItems.join(category_numberOfLowProfitItems).coalesce(1).\
    saveAsTextFile('output_spark_part_2/')
sc.stop()
