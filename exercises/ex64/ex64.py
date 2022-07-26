from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext


def stream_map(line):
    words = line.split(',')
    return (words[1], int(words[2]))


def reference_map(line):
    words = line.split(',')
    return (words[1], int(words[2]))


conf = SparkConf().setAppName('ex 58')
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 10)

input_stream = ssc.textFileStream('destination/')
reference_rdd = sc.textFile('source/historicalData.txt')

historical_mins = reference_rdd.map(reference_map).reduceByKey(lambda v1, v2: min(v1, v2))
historical_maxs = reference_rdd.map(reference_map).reduceByKey(lambda v1, v2: max(v1, v2))
full_historical = historical_mins.join(historical_maxs)  # (id, (min, max))
stream_mins = input_stream.map(stream_map).reduceByKey(lambda v1, v2: min(v1, v2))
stream_maxs = input_stream.map(stream_map).reduceByKey(lambda v1, v2: max(v1, v2))
full_stream = stream_mins.join(stream_maxs)  # (id, (min, max))
joined = full_stream.transform(lambda batch: batch.join(full_historical))# (id, ((min_stream, max_stream), (min_historical, max_historical)))
result = joined.\
    filter(lambda my_tuple: my_tuple[1][0][0] < my_tuple[1][1][0] or my_tuple[1][0][1] > my_tuple[1][1][1]).\
    map(lambda my_tuple: my_tuple[0])

result.pprint()

ssc.start()
ssc.awaitTermination()
