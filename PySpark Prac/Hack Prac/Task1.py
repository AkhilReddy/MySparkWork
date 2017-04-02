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


filey = sc.textFile("/root/Desktop/D1_AutoInsurance.csv")

rdd = filey.map(lambda line: line.split(","))

print rdd.count()

for row in rdd.take(rdd.count()): print(row[1])

#Map(func)

Map_rdd = rdd.map(lambda x: [x[0],x[1],x[2]])

#print Map_rdd.collect()[:10]
print "\n\n"

#Filter(func)
print "Filter :"
print "\n\n"

Filter_rdd = rdd.filter(lambda x: [x[12] > 85])

#print Filter_rdd.collect()[:10]
print "\n\n"

#flatMap(func)

flatMap_rdd = rdd.flatMap(lambda x: [x[0],x[1],x[2]])

#print flatMap_rdd.collect()[:10] 
print "\n\n"


#sample(withReplacement, fraction, seed) 

sampleRdd = rdd.sample(True,0.5,3)

#print sampleRdd.collect()[:10]
print "\n\n"


#union(otherDataset) 

A = rdd.map(lambda x: [x[0],x[1],x[2]])
B = rdd.map(lambda x: [x[3],x[4],x[5]])

AUB = A.union(B)

#print AUB.take(10)
#print AUB.collect()
print "\n\n"

#Intersection(otherDataset)
C = rdd.map(lambda x: [x[0],x[1],x[2]])
D = rdd.map(lambda x: [x[0],x[1],x[2]])


CVD = C.intersection(D)

print CVD
print "\n\n"

#Distinct([numTasks]))

CountRDD = rdd.count()

#print CountRDD
print "\n\n"

DistinctRDD = rdd.distinct()

#print DistinctRDD
print "\n\n"

Keys = rdd.map(lambda x: (x[0],[x[1],x[2]]))

#print Keys.collect()
print "\n\n"

KeysVal = rdd.map(lambda x: (x[1],[x[0],x[2]]))

#print KeysVal.collect()
print "\n\n"


#groupByKey([numTasks]) 

KeysGroupBy = KeysVal.groupByKey()

#print KeysGroupBy.collect()[:10]
print "\n\n"


KeysMap = KeysGroupBy.map(lambda x : {x[0]: list(x[1])}).collect()
 
#print KeysMap[:10]
print "\n\n"

header = rdd.first() #extract header
rddN = rdd.filter(lambda row : row != header)   #filter out header

#print rdd.collect()[:10]
ReduceKeyRDD = rddN.map(lambda n:  (str(n[1]), float(n[2]))).reduceByKey(lambda v1,v2: v1 + v2).collect()

#print ReduceKeyRDD
print "\n\n"

sortByKeyRDD = rddN.map(lambda n:  (str(n[1]), float(n[2]))).sortByKey(False).collect()

#print sortByKeyRDD
print "\n\n"


names1 = rddN.map(lambda a: (a[0], 1))
names2 = rddN.map(lambda a: (a[1], 1))
JoinRDD = names1.join(names2).collect()

#print JoinRDD
print "\n\n"

LeftJoin = names1.leftOuterJoin(names2).collect()

#print LeftJoin
print "\n\n"

#pipeRDD = rdd.pipe(rddN)

#print pipeRDD.collect()
print "\n\n"

print "#########################################################################"
print "\n\n"

x = rddN.map(lambda l:(str(l[0]), 1))
y = rddN.map(lambda l:(str(l[0]), 2))

SXY = sorted(x.cogroup(y).collect())

#print SXY


CountNames = names1.countByKey() 

#print CountNames
print "\n\n"

Ex = names1.repartition(3)

#print Ex.getNumPartitions()


####################################################################################
#aggregateByKey(zeroValue)(seqOp, combOp, [numTasks]) 

#AggregateRDD = rddN.aggregateByKey([],\
    (lambda x, y: (x[0]+y[0],x[1]+1)),\
    (lambda rdd1, rdd2: (rdd1[0]+rdd2[0], rdd1[1]+rdd2[1])))

#print AggregateRDD.collect()
#print "\n\n"

###################################################################################


