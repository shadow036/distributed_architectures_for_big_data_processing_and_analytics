from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('ex 58')
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 2)

input_stream = ssc.textFileStream('destination/')
output_stream = input_stream.filter(lambda line: int(line.split(',')[1]) == 0).count()
output_stream.pprint()

ssc.start()
ssc.awaitTermination()
