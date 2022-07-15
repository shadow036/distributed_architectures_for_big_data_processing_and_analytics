import sys
from pyspark import SparkConf, SparkContext

def frequencies(array):
    frequencies = []
    for product1 in array:
        for product2 in array:
            if(product1[1] < product2[1]):
                frequencies.append(((product1, product2), 1))
    return frequencies
    
if __name__ == "__main__":
    conf = SparkConf().setAppName("lab 6")
    sc = SparkContext(conf = conf)
    input_path = 'input_lab6/'
    output_path = 'output_lab6/'
    user_product_RDD = sc.textFile(input_path).filter(lambda line:line.startswith("Id") == False).map(lambda line:(line.split(',')[2], line.split(',')[1])).distinct()
    products_bought_together_RDD = user_product_RDD.reduceByKey(lambda a1, a2: a1+','+a2).map(lambda tuple:(tuple[0], tuple[1].split(','))).values().flatMap(frequencies).reduceByKey(lambda c1, c2: c1+c2).filter(lambda tuple:tuple[1] > 1).sortBy(lambda tuple:tuple[1], False)
    products_bought_together_RDD.saveAsTextFile(output_path)
    print(products_bought_together_RDD.top(10, lambda tuple:tuple[1]))
    sc.stop()
