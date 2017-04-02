

'''
Notes:

Managing Spark Partitions with Coalesce and Repartition

Spark splits data into partitions and executes computations on the partitions in parallel. We should understand how data is partitioned and when you need to manually adjust the partitioning to keep your Spark computations running efficiently.
'''



# -*- coding: utf-8 -*-

#Python Imports
from operator import add

import sys

from datetime import date, datetime

from ast import literal_eval

#Spark Imports
from pyspark import SparkConf, SparkContext, sql

from pyspark.sql import SQLContext

from pyspark.sql import DataFrame

from pyspark.sql import Column

from pyspark.sql import Row

from pyspark.sql import functions

from pyspark.sql import types

from pyspark.sql.types import *  

from pyspark.sql import SparkSession

import pyspark.sql.functions as func

conf = SparkConf().setMaster("local").setAppName("Task 1")

sc = SparkContext(conf = conf)                                          

sqlContext = SQLContext(sc)

################################################               D1_AutoInsurance.csv             ###################################################
print("\n\n*************************************                   D1_AutoInsurance.csv          ********************************************\n\n")

'''
Customer	State	Customer Lifetime Value	Response	Coverage	Education	Effective To Date	EmploymentStatus	Gender	Income	Location Code	Marital Status	Monthly Premium Auto	Months Since Last Claim	Months Since Policy Inception	Number of Open Complaints	Number of Policies	Policy Type	Policy	Renew Offer Type	Sales Channel	Total Claim Amount	Vehicle Class	Vehicle Size

'''

filey = "/root/Desktop/D1_AutoInsurance.csv"


spark = SparkSession.builder.master("local").appName("Task 3 App").config("spark.some.config.option", "some-value").getOrCreate()

df = spark.read.csv(filey,header=True,inferSchema=True) 

#(schema=None, sep=None, encoding=None, quote=None, escape=None, comment=None, header=None, inferSchema=None, ignoreLeadingWhiteSpace=None, ignoreTrailingWhiteSpace=None, nullValue=None, nanValue=None, positiveInf=None, negativeInf=None, dateFormat=None, maxColumns=None, maxCharsPerColumn=None, maxMalformedLogPerPartition=None, mode=None)

df.show()

#print(df.schema)

print df.printSchema()

df.rdd.repartition(3)

print df.rdd.getNumPartitions()
print "\n\n"

#df.write.csv("/root/Desktop/spark_output")

#Depending on the number of Partitions, The data is separated on the different partitions.

#coalesce

#The coalesce method reduces the number of partitions in a DataFrame.

df.coalesce(2)

print df.rdd.getNumPartitions()

# As I am working on standalone local System,partition will always be 1.

'''
The coalesce algorithm moved the data from Partition B to Partition A and moved the data from Partition D to Partition C. The data in Partition A and Partition C does not move with the coalesce algorithm. This algorithm is fast in certain situations because it minimizes data movement.
'''

#The coalesce algorithm changes the number of nodes by moving data from some partitions to existing partitions. This algorithm obviously cannot increate the number of partitions.

#Repartition

#The repartition method can be used to either increase or decrease the number of partitions in a DataFrame.

df.repartition(2)

print df.rdd.getNumPartitions()

'''

Repartition ==> 2 For,

Partition ABC: 1, 3, 5, 6, 8, 10
Partition XYZ: 2, 4, 7, 9

Partition ABC contains data from Partition A, Partition B, Partition C, and Partition D. Partition XYZ also contains data from each original partition. The repartition algorithm does a full data shuffle and equally distributes the data among the partitions. It does not attempt to minimize data movement like the coalesce algorithm.
'''

df.repartition(6)

print df.rdd.getNumPartitions()

'''
Partition 00000: 5, 7
Partition 00001: 1
Partition 00002: 2
Partition 00003: 8
Partition 00004: 3, 9
Partition 00005: 4, 6, 10

The repartition method does a full shuffle of the data, so the number of partitions can be increased.

'''


'''

Differences between coalesce and repartition

The repartition algorithm does a full shuffle of the data and creates equal sized partitions of data. coalesce combines existing partitions to avoid a full shuffle.

'''


#repartition by column:

Statedf = df.repartition(df.State)


Statedf.show() # PARTITION Occured by STATE Name. 

''''
When partitioning by a column, Spark will create a minimum of 200 partitions by default. This example will have two partitions with data and 198 empty partitions.

The statef contains different partitions for each state and is optimized for extracts by state. Partitioning by a column is similar to indexing a column in a relational database.

'''

'''
In general, you can determine the number of partitions by multiplying the number of CPUs in the cluster by 4 (source).

number_of_partitions = number_of_cpus * 4

'''

#If you're writing the data out to a file system, you can choose a partition size that will create reasonable sized files (100MB). Spark will optimize the number of partitions based on the number of clusters when the data is read.

#Why did we use the repartition method instead of coalesce?

#A full data shuffle is an expensive operation for large data sets, but our data puddle is only 2,000 rows. The repartition method returns equal sized text files, which are more efficient for downstream consumers.


#Actual performance improvement

#It took 241 seconds to count the rows in the data puddle when the data wasn't repartitioned on a 5 node cluster. But 2 sec after Partition.

'''

You probably need to think about partitions

The partitioning of DataFrames seems like a low level implementation detail that should be managed by the framework, but it's not. When filtering large DataFrames into smaller ones, you should almost always repartition the data.

You'll probably be filtering large DataFrames into smaller ones frequently, so get used to repartitioning.

'''

#######################################
#One additional point to note here is that, as the basic principle of Spark RDD is immutability. The repartition or coalesce will create new RDD. The base RDD will continue to have existence with its original number of partitions. In case the use case demands to persist RDD in cache, then the same has to be done for the newly created RDD.
######################################


KeysVal = df.rdd.map(lambda x: (x[1],[x[0],x[2]])).toDF(["Key","Value"])

print KeysVal.show()
print "\n\n"

A = df.rdd.countByKey()

#B = df.rdd.collectAsMap()

C = df.rdd.lookup("Arizona")

print A
print "\n\n"

#print B
print "\n\n"

print C
print "\n\n"





'''
#joins and cogroup in Spark :	

There are indications that joins in Spark are implemented with / based on the cogroup function/primitive/transform. So let me focus first on cogroup - it returns a result which is RDD consisting of essentially ALL elements of the cogrouped RDDs. Said in another way - for every key in each of the cogrouped RDDs there is at least one element from at least one of the cogrouped RDDs.

That would mean that when smaller, moreover streaming e.g. JavaPairDstreamRDDs keep getting joined with much larger, batch RDD that would result in RAM allocated for multiple instances of the result (cogrouped) RDD a.k.a essentially the large batch RDD and some more ... Obviously the RAM will get returned when the DStream RDDs get discard and they do on a regular basis, but still that seems as unnecessary spike in the RAM consumption

We have two questions:

    Is there anyway to control the cogroup process more "precisely" e.g. tell it to include I the cogrouped RDD only elements where there are at least one element from EACH of the cogrouped RDDs per given key. Based on the current cogroup API this is not possible

    If the cogroup is really such a sledgehammer and secondly the joins are based on cogroup then even though they can present a prettier picture in terms of the end result visible to the end user does that mean that under the hood there is still the same atrocious RAM consumption going on

Solution:

It is not that bad. It largelly depends on the granularity of your partitioning. Cogroup will first shuffle by key, in disk, to distinct executor nodes. For each key, yes, the entire set of all elements with that key, for both RDDs, will be loaded in RAM and given to you. But not all keys need to be in RAM at any given time, so unless your data is really skewed you will not suffer for it much. 

'''
x = df.rdd.map(lambda l:(str(l[0]), 1))
y = df.rdd.map(lambda l:(str(l[0]), 2))

CGXY = x.cogroup(y).collect()[:10]

print SXY

JXY = x.join(y).collect()[:10]

print JXY

#groupByKey and reduceByKey

#groupByKey([numTasks]) 

KeysGroupBy = KeysVal.groupByKey()

#print KeysGroupBy.collect()[:10]
print "\n\n"

KeysMap = KeysGroupBy.map(lambda x : {x[0]: list(x[1])}).collect()
 
#print KeysMap[:10]
print "\n\n"

#print rdd.collect()[:10]
ReduceKeyRDD = df.rdd.map(lambda n:  (str(n[1]), float(n[2]))).reduceByKey(lambda v1,v2: v1 + v2).collect()

#print ReduceKeyRDD
print "\n\n"


