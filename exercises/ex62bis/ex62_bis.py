from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('ex 58')
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 30)

input_stream = ssc.textFileStream('destination/')
output_stream1 = input_stream.map(lambda line: (line.split(',')[1], int(line.split(',')[2]))).\
    reduceByKeyAndWindow(lambda v1, v2: min(v1, v2), None, 60)
output_stream2 = input_stream.map(lambda line: (line.split(',')[1], int(line.split(',')[2]))).\
    reduceByKeyAndWindow(lambda v1, v2: max(v1, v2), None, 60)
result = output_stream1.join(output_stream2).\
    map(lambda my_tuple: (my_tuple[0], (100 * (my_tuple[1][1] - my_tuple[1][0])/my_tuple[1][1]))).\
    filter(lambda my_tuple: my_tuple[1] > 0.5)
result.pprint()

ssc.start()
ssc.awaitTermination()
