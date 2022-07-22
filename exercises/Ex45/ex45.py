import sys
from pyspark import SparkConf, SparkContext


conf = SparkConf().setAppName('ex 44')
sc = SparkContext(conf=conf)

watched_movies = sc.textFile('input/watchedmovies.txt').map(lambda line: (line.split(',')[0], line.split(',')[1]))
preferences = sc.textFile('input/preferences.txt').map(lambda line: ((line.split(',')[0], line.split(',')[1]), None))
movies_info = sc.textFile('input/movies.txt').map(lambda line: (line.split(',')[0], line.split(',')[2]))

threshold = float(sys.argv[1])

movies_count = watched_movies.map(lambda tuple: (tuple[0], 1)).reduceByKey(lambda v1, v2: v1 + v2)
watched_genres = watched_movies.map(lambda line: (line[1], line[0])).join(movies_info).map(lambda tuple: ((tuple[1][0], tuple[1][1]), 1))
matching_movies = watched_genres.join(preferences)
matching_movies_count = matching_movies.map(lambda line: (line[0][0], line[1][0])).reduceByKey(lambda count, one: count + one)
misleading_profiles = movies_count.join(matching_movies_count).\
    map(lambda line: (line[0], (line[1][0] - line[1][1])/line[1][0])).\
    filter(lambda line: line[1] > threshold)
misleading_profiles.coalesce(1).saveAsTextFile('output/misleading_profiles/')
(watched_genres.reduceByKey(lambda v1, v2: v1 + v2)).subtractByKey(preferences).filter(lambda tuple: tuple[1] >= 5)\
    .map(lambda tuple: (tuple[0][0], tuple[0][1])).join(misleading_profiles).map(lambda tuple: (tuple[0], tuple[1][0]))\
    .coalesce(1).saveAsTextFile('output/misleading_genres/')

sc.stop()
