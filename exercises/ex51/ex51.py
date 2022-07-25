from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, BooleanType


def number_of_words(text):
    return len(text.split(' '))


def spark_presence(text):
    return (text.find("Spark") != -1)


def restore_original_features(features, index):
    return features[index]


number_of_words_udf = udf(lambda text: number_of_words(text), IntegerType())
spark_presence_udf = udf(lambda text: spark_presence(text), BooleanType())
restore_original_features_udf = udf(lambda features, index: restore_original_features(features, index))

conf = SparkConf().setAppName('ex 51')
sc = SparkContext(conf=conf)
ss = SparkSession.builder.getOrCreate()

assembler = VectorAssembler(inputCols=["number_of_words", "spark_presence"], outputCol="features")
unlabeled = ss.read.load('input/unlabeled.csv', format='csv', header=True, inferSchema=True). \
    withColumn("number_of_words", number_of_words_udf(col("text"))). \
    withColumn("spark_presence", spark_presence_udf(col("text")))
training = ss.read.load('input/training.csv', format='csv', header=True, inferSchema=True). \
    withColumn("number_of_words", number_of_words_udf(col("text"))).\
    withColumn("spark_presence", spark_presence_udf(col("text")))
new_unlabeled = assembler.transform(unlabeled)  # adding features column
new_training = assembler.transform(training)    # adding features column

lr = LogisticRegression()
model = lr.fit(new_training)
output = model.transform(new_unlabeled).\
    withColumn("number_of_words", number_of_words_udf(col("text"))).\
    withColumn("spark_presence", spark_presence_udf(col("text"))).\
    select("text", "prediction")

output.write.csv('output/', header=True)

sc.stop()
ss.stop()