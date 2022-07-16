import sys
from pyspark import SparkConf, SparkContext #NOQ

def filter_prefix(line):
	return (line[:2] == "ho")

conf = SparkConf().setAppName("lab 5")
sc = SparkContext(conf = conf)

input_path = 'input_lab5/'
output_path = 'output_lab5/'
output_path2 = 'output_lab5_2/'
input_RDD = sc.textFile(input_path)
#PART 1
output_RDD = input_RDD.filter(filter_prefix)
max_frequency = output_RDD.map(lambda l:int(l.split('\t')[1])).top(1)[0]
print("\n'ho' count: ", output_RDD.count(), "\n")
print("\n'ho' max frequency: ", max_frequency, "\n")
output_RDD.saveAsTextFile(output_path)
#PART 2
most_frequent_RDD = output_RDD.filter(lambda l: int(l.split('\t')[1]) > 0.8*max_frequency)
print("\n'ho' words with frequency > 80'%' of the maximum one: ", most_frequent_RDD.count(), "\n")
output2_RDD = most_frequent_RDD.map(lambda l:l.split('\t')[0])
output2_RDD.saveAsTextFile(output_path2)
sc.stop()
