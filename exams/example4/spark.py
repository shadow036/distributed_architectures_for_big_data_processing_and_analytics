from pyspark import SparkContext
from datetime import datetime, timedelta

sc = SparkContext()


def previous_date(date):
    date = datetime.strptime(date, "%Y/%m/%d")
    return datetime.strftime(date - timedelta(days=1), "%Y/%m/%d")


stock_prices = sc.textFile('input/stock_prices.txt')    # (stock_id,date,time,price)

# PART 1
stock_date__max_min = stock_prices.filter(lambda line: line.split(',')[1].split('/')[0] == '2017').\
    map(lambda line: ((line.split(',')[0], line.split(',')[1]),
                      [float(line.split(',')[3]), float(line.split(',')[3])])).\
    reduceByKey(lambda v1, v2: [max(v1[0], v2[0]), min(v1[1], v2[1])]).cache()
result1 = stock_date__max_min.map(lambda my_tuple: (my_tuple[0], 1 if my_tuple[1][0] - my_tuple[1][1] > 10 else 0)).\
    map(lambda my_tuple: (my_tuple[0][0], my_tuple[1])).reduceByKey(lambda v1, v2: v1 + v2).\
    filter(lambda my_tuple: my_tuple[1] > 0)
result1.coalesce(1).saveAsTextFile('output_spark_part_1/')

# PART 2
stockId_date__variation_flag = stock_date__max_min.flatMap(lambda my_tuple: [
    (my_tuple[0], (my_tuple[1][0] - my_tuple[1][1], True)),
    ((my_tuple[0][0], previous_date(my_tuple[0][1])), (my_tuple[1][0] - my_tuple[1][1], False))
])
result2 = stockId_date__variation_flag.join(stockId_date__variation_flag).\
    filter(lambda my_tuple: my_tuple[1][0][1] is True and my_tuple[1][1][1] is False).\
    map(lambda my_tuple: (my_tuple[0], (my_tuple[1][0][0], my_tuple[1][1][0]))).\
    filter(lambda my_tuple: abs(my_tuple[1][0] - my_tuple[1][1]) <= 0.1).map(lambda my_tuple: my_tuple[0])
result2.coalesce(1).saveAsTextFile('output_spark_part_2/')

sc.stop()
