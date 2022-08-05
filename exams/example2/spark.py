from pyspark import SparkContext

sc = SparkContext()

temperatures = sc.textFile('Temperatures.txt')

# PART 1
summer2015 = temperatures.\
    filter(lambda line: line.split(',')[0].split('/')[0] == '2015' and '08' >= line.split(',')[0].split('/')[1] >= '06')
result1 = summer2015.\
    map(lambda line: (line.split(',')[1] + ' - ' + line.split(',')[2], (float(line.split(',')[3]), 1))).\
    reduceByKey(lambda tuple1, tuple2: (tuple1[0] + tuple2[0], tuple1[1] + tuple2[1])).\
    map(lambda my_tuple: (my_tuple[0], my_tuple[1][0]/my_tuple[1][1]))
result1.coalesce(1).saveAsTextFile('output_spark_part_1/')

# PART 2
country_averageMaxTemperature = summer2015.map(lambda line: (line.split(',')[2], (float(line.split(',')[3]), 1))).\
    reduceByKey(lambda tuple1, tuple2: (tuple1[0] + tuple2[0], tuple1[1] + tuple2[1])).\
    map(lambda my_tuple: (my_tuple[0], my_tuple[1][0]/my_tuple[1][1]))
result2 = result1.map(lambda my_tuple: (my_tuple[0].split(' - ')[1], (my_tuple[0].split(' - ')[0], my_tuple[1]))).\
    join(country_averageMaxTemperature).map(lambda my_tuple: (my_tuple[1][0][0], (my_tuple[1][0][1], my_tuple[1][1]))).\
    filter(lambda my_tuple: my_tuple[1][0] - my_tuple[1][1] > 5).map(lambda my_tuple: my_tuple[0])
result2.coalesce(1).saveAsTextFile('output_spark_part_2/')
sc.stop()
