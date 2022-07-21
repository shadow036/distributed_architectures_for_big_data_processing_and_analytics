import sys
from pyspark import SparkConf, SparkContext


def remove_features_questions(record):
    words = record.split(',')
    return (words[0], words[2])


def remove_features_answers(record):
    words = record.split(',')
    return (words[1], words[3])


conf = SparkConf().setAppName('ex 42')
sc = SparkContext(conf=conf)

questions = sc.textFile('input/questions.txt').map(remove_features_questions)
answers = sc.textFile('input/answers.txt').map(remove_features_answers)

join = (questions.cogroup(answers)).coalesce(1).saveAsTextFile('output/')

sc.stop()
