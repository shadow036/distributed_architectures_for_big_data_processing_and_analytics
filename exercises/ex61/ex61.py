from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('ex 58')
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 2)

input_stream = ssc.textFileStream('destination/')
output_stream = input_stream.map(lambda line: int(line.split(',')[1])).reduce(lambda v1, v2: max(v1, v2))
output_stream.pprint()

ssc.start()
ssc.awaitTermination()
