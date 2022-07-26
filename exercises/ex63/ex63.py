from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext


def stream_map(line):
    words = line.split(',')
    return (words[0], words[3])    # (station_id, timestamp)


def reference_map(line):
    words = line.split('\t')
    return (words[0], words[3]) # (station_id, name)


conf = SparkConf().setAppName('ex 58')
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 2)

input_stream = ssc.textFileStream('destination/').filter(lambda line: int(line.split(',')[1]) == 0).map(stream_map)
reference_rdd = sc.textFile('source/stations.csv').map(reference_map)

result = input_stream.transform(lambda batch: batch.join(reference_rdd)).\
    map(lambda my_tuple: (my_tuple[1][0], my_tuple[1][1]))
result.pprint()

ssc.start()
ssc.awaitTermination()
