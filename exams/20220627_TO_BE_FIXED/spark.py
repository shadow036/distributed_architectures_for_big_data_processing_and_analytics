from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('exam')
sc = SparkContext(conf=conf)


def check_year(date):
    if date.split('/')[0] == '2022':
        return 1
    else:
        return 0


servers = sc.textFile('input/servers.txt')    # (server_id,operating_system,model)
patches = sc.textFile('input/patches.txt')  # (patch_id,release_date,operating_system)
applied_patches = sc.textFile('input/applied_patches.txt')  # (patch_id,server_id,date)

# PART 1
serverId_operatingSystem = servers.map(lambda line: (line.split(',')[0], line.split(',')[1]))
serverId_numberOfAppliedPatches = applied_patches.\
    map(lambda line: (line.split(',')[1], check_year(line.split(',')[2]))).\
    reduceByKey(lambda v1, v2: v1 + v2).filter(lambda my_tuple: my_tuple[1] > 0)
maxNumberOfAppliedPatches = serverId_numberOfAppliedPatches.map(lambda my_tuple: my_tuple[1]).max()
result = serverId_numberOfAppliedPatches.filter(lambda my_tuple: my_tuple[1] == maxNumberOfAppliedPatches).\
    join(serverId_operatingSystem).map(lambda my_tuple: (my_tuple[0], my_tuple[1][1]))
result.coalesce(1).saveAsTextFile('output_spark_part_1/')

# PART 2
operatingSystem_numberOfPatches = patches.map(lambda line: (line.split(',')[2], 1)).reduceByKey(lambda v1, v2: v1 + v2)
serverId_numberOfAppliedPatches = applied_patches.map(lambda line: (line.split(',')[1], 1)).\
    reduceByKey(lambda v1, v2: v1 + v2)
serverId__numberOfAppliedPatches_numberOfPatches = serverId_numberOfAppliedPatches.join(serverId_operatingSystem).\
    map(lambda my_tuple: (my_tuple[1][1], (my_tuple[0], my_tuple[1][0]))).join(operatingSystem_numberOfPatches).\
    map(lambda my_tuple: (my_tuple[1][0][0], (my_tuple[1][0][1], my_tuple[1][1]))).\
    filter(lambda my_tuple: my_tuple[1][0] == my_tuple[1][1]).map(lambda my_tuple: my_tuple[0])
servers_with_no_patches = serverId_operatingSystem.map(lambda my_tuple: (my_tuple[1], None)).\
    subtract(operatingSystem_numberOfPatches.map(lambda my_tuple: (my_tuple[0], None))).\
    join(serverId_operatingSystem.map(lambda my_tuple: (my_tuple[1], my_tuple[0]))).map(lambda my_tuple: my_tuple[1][1])
result2 = serverId__numberOfAppliedPatches_numberOfPatches.union(servers_with_no_patches)
result2.coalesce(1).saveAsTextFile('output_spark_part_2/')

sc.stop()
