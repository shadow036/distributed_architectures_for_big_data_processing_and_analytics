from pyspark import SparkConf, SparkContext


def filter_period(line):
    year, month, daytime = line.split(',')[2].split('/')
    year = int(year)
    month = int(month)
    day = int(daytime.split('_')[0])
    if year < 2018 or (year == 2018 and month < 9) or (year == 2018 and month == 9 and day < 14):
        return False
    if year > 2020 or (year == 2020 and month > 9) or (year == 2020 and month == 9 and day > 13):
        return False
    return True


def map_young(line):
    words = line.split(',')
    if int(words[4]) > 1999:
        return (words[0], 1)
    else:
        return (words[0], 0)


def generate_tuple(line):
    words = line.split(',')
    year = words[2].split('/')[0]
    return (words[0] + ',' + year + ',' + words[1], None)


def generate_tuple2(my_tuple):
    words = my_tuple[0].split(',')
    return ((words[0], int(words[1])), 1)


def reduceByKey_final(my_tuple1, my_tuple2):
    if my_tuple1[1] > my_tuple2[1]:
        return my_tuple1
    elif my_tuple1[1] == my_tuple2[1]:
        if my_tuple1[0] < my_tuple2[0]:
            return my_tuple1
        else:
            return my_tuple2
    else:
        return my_tuple2


conf = SparkConf().setAppName('exam')
sc = SparkContext(conf=conf)

users = sc.textFile('input/users.txt')  # (user_id,name,surname,gender,year_of_birth)
songs = sc.textFile('input/songs.txt')  # (song_id,title,music_genre)
listen_to_songs = sc.textFile('input/listen_to_songs.txt')  # (song_id,user_id,timestamp_start,timestamp_end)

# PART 1
userId_youngFlag = users.map(lambda line: map_young(line))
userId_songId = listen_to_songs.filter(lambda line: filter_period(line)).\
    map(lambda line: (line.split(',')[1], line.split(',')[0]))
excludedSongs = listen_to_songs.map(lambda line: line.split(',')[0]).subtract(userId_songId.map(lambda my_tuple: my_tuple[1])).distinct()
userId_songId.join(userId_youngFlag).map(lambda my_tuple: (my_tuple[1][0], my_tuple[1][1])).\
    reduceByKey(lambda flag1, flag2: flag1 + flag2).filter(lambda my_tuple: my_tuple[1] == 0).\
    map(lambda my_tuple: my_tuple[0]).union(excludedSongs).coalesce(1).saveAsTextFile('output_spark_part_1/')

# PART 2
result = listen_to_songs.map(lambda line: generate_tuple(line)).reduceByKey(lambda v1, v2: v1).\
    map(lambda my_tuple: generate_tuple2(my_tuple)).reduceByKey(lambda one1, one2: one1 + one2).\
    map(lambda my_tuple: (my_tuple[0][0], (my_tuple[0][1], my_tuple[1]))).reduceByKey(reduceByKey_final).\
    map(lambda my_tuple: (my_tuple[0], my_tuple[1][0]))

result.coalesce(1).saveAsTextFile('output_spark_part_2/')

sc.stop()
