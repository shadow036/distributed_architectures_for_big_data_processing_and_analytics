import numpy
from pyspark.sql import SparkSession
from pyspark.ml.feature import SQLTransformer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.linalg import Vectors
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel

ss = SparkSession.builder.getOrCreate()
ss.udf.register("discriminate", lambda num, den: 1.0 if num/den > 0.9 else 0.0)
ss.udf.register("my_len", lambda text: len(text))

# this is weird...

if __name__ == '__main__':
	training = ss.read.load('input_lab9/ReviewsSample.csv', format='csv', header=True, inferSchema=True)

	assembler = VectorAssembler(inputCols=["feature"],outputCol="features")
	transformer = SQLTransformer(statement="SELECT Text, my_len(Text) AS feature, discriminate(HelpfulnessNumerator, HelpfulnessDenominator) AS label FROM __THIS__ WHERE HelpfulnessNumerator > 0 OR HelpfulnessDenominator > 0")
	lr = LogisticRegression()

	data1 = transformer.transform(training)
	#data1 = training.filter("HelpfulnessNumerator > 0 and HelpfulnessDenominator > 0").selectExpr("my_len(Text) AS length", "discriminate(HelpfulnessNumerator, HelpfulnessDenominator) AS label")
	data2 = data1.withColumn("feature",data1.feature.cast("float"))
	data3 = data2.withColumn("label",data2.label.cast("float"))
	pipeline1 = Pipeline().setStages([assembler, lr])
	model = pipeline1.fit(data3)
	predictions = model.transform(data3)
	predictions.show(14)
	predictions.write.csv('output_lab9/', header=True)
	
