from pyspark import SparkContext

sc = SparkContext()

servers = sc.textFile('input/servers.txt')  # (server_id,cpu_version,datacenter_id,city,country)
anomalies = sc.textFile('input/anomalies.txt')  # (server_id,date_time,anomalous_value)

# PART 1
anomalies_2010_2018 = anomalies.\
    filter(lambda line: 2018 >= int(line.split(',')[1].split('_')[0].split('/')[0]) >= 2010).cache()
result1 = anomalies_2010_2018.filter(lambda line: float(line.split(',')[2]) > 100).\
    map(lambda line: ((line.split(',')[0], int(line.split(',')[1].split('_')[0].split('/')[0])), 1)).\
    reduceByKey(lambda v1, v2: v1 + v2).map(lambda my_tuple: (my_tuple[0][0], 1 if my_tuple[1] >= 2 else 0)).\
    reduceByKey(lambda v1, v2: v1 + v2).filter(lambda my_tuple: my_tuple[1] > 0).map(lambda my_tuple: my_tuple[0])
result1.coalesce(1).saveAsTextFile('output_spark_part_1/')

# PART 2
serverId_datacenterId = servers.map(lambda line: (line.split(',')[0], line.split(',')[2]))
serverId_flag = anomalies_2010_2018.map(lambda line: (line.split(',')[0], 1)).reduceByKey(lambda v1, v2: v1 + v2).\
    map(lambda my_tuple: (my_tuple[0], 1 if my_tuple[1] > 2 else 0))
servers_with_no_anomalies = servers.\
    map(lambda line: line.split(',')[0]).distinct().subtract(anomalies_2010_2018.map(lambda line: line.split(',')[0]).
                                                             distinct()).map(lambda server_id: (server_id, 0))
result2 = serverId_flag.union(servers_with_no_anomalies).join(serverId_datacenterId).\
    map(lambda my_tuple: (my_tuple[1][1], my_tuple[1][0])).reduceByKey(lambda v1, v2: v1 + v2).\
    filter(lambda my_tuple: my_tuple[1] == 0).map(lambda my_tuple: my_tuple[0])
print(result2.count())
result2.coalesce(1).saveAsTextFile('output_spark_part_2/')

sc.stop()
