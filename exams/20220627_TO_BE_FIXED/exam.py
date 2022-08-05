import pyspark as ps

from pyspark import SparkContext
from pyspark.sql import Row, DataFrame, SparkSession

from typing import List, Tuple, Dict, Optional, Union

sc = SparkContext()

servers_path = 'input/servers.txt'
patches_path = 'input/patches.txt'
applied_patches_path = 'input/applied_patches.txt'

outpath1 = 'out1'
outpath2 = 'out2'


# In[ ]:


servers_rdd = sc.textFile(servers_path)
patches_rdd = sc.textFile(patches_path)
applied_patches_rdd = sc.textFile(applied_patches_path)


# In[ ]:


# Ex. 1
# count the number of applied patches per server in 2022
# --> filter only applied patches for which date is in 2022
# --> mapToPair with key = SID, value = +1
# --> reduceByKey to count per each server the number of applied patches
count_appl_patches_per_server_rdd = applied_patches_rdd \
                                        .filter(lambda line: line.split(',')[2].startswith('2022')) \
                                        .map(lambda it: (it.split(',')[0], 1)) \
                                        .reduceByKey(lambda i1, i2: i1 + i2)

# store in a local variable the max number of applied patches in 2022
max_patches = count_appl_patches_per_server_rdd.values().max()

# filter all the servers for which count == max_patches
servers_res1 = count_appl_patches_per_server_rdd \
                    .filter(lambda it: it[1] == max_patches)
    
# retrieve for each server its OS
def get_sid_os(line: str) -> Tuple[str, str]:
    fields = line.split(',')
    return (fields[0], fields[1])

server_os_rdd = servers_rdd.map(get_sid_os)

# join servers_res1 with server_os_rdd
# keep only
# key = SID
# value = OS
res1 = server_os_rdd \
            .join(servers_res1) \
            .map(lambda it: (it[0], it[1][0]))

res1.saveAsTextFile(outpath1)


# In[ ]:


# Ex. 2
# count the number of patches available for each OS
# key = OS
# value = count of number of available patches
patches_per_os = patches_rdd \
                    .map(lambda it: (it.split(',')[2], 1)) \
                    .reduceByKey(lambda i1, i2: i1 + i2)

# compute the number of applied patches for each server
# key = SID
# value = count of number of applied patches
patch_per_server = applied_patches_rdd \
                        .map(lambda it: (it.split(',')[0], 1)) \
                        .reduceByKey(lambda i1, i2: i1 + i2)

# retrieve for each server its OS
# prepare a pairRDD where
# key = SID
# value = OS
# I can re-use server_os_rdd previously defined in Ex. 1

# join server_os_rdd with patch_per_server
# using a rightOuterJoin to keep also servers with no patches
# map missing count values to 0
patch_per_server_with_os = patch_per_server \
                                .rightOuterJoin(server_os_rdd) \
                                .map(lambda it: (it[0], (it[1][0] if not it[1][0] is None else 0, it[1][1])))

# simply swap key and values from patch_per_server_with_os
# before:
# key = SID
# value = (countPatches, OS)
# after:
# key = OS
# value = (SID, countPatches)
os_server_patches = patch_per_server_with_os \
                        .map(lambda it: (it[1][1], (it[0], it[1][0])))

# join os_server_patches with patches_per_os
# to verify whether each server with its own OS was properly updated
# and all the patches were applied
# A leftOuterJoin is used to keep OS for which no patches are available
# key = OS
# value = ((SID, number_of_applied_patches), number_of_available_patches)
joined_rdd = os_server_patches.leftOuterJoin(patches_per_os)

# remap number_of_available_patches == None to 0 (-> no patches available for OS)
# and filter by keeping all the elements for which
# number of applied patches == number of available patches
# and keep only SID
def filter_servers_with_all_patches_applied(it: Tuple[str, Tuple[Tuple[str, int], int]]) -> bool:
    value = it[1]
    sid, applied_patches = value[0]
    available_patches = value[1] if not value[1] is None else 0

    return applied_patches == available_patches
res2 = joined_rdd \
            .filter(filter_servers_with_all_patches_applied) \
            .map(lambda it: it[1][0][0])

res2.saveAsTextFile(outpath2)

