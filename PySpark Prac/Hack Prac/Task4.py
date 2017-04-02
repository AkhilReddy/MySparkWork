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

#RDD
'''
filey = sc.textFile("/root/Desktop/D1_AutoInsurance.csv")

rdd = filey.map(lambda line: line.split(","))

print rdd.count()

header = rdd.first() #extract header
rddN = rdd.filter(lambda row : row != header)   #filter out header

Map_rdd = rddN.map(lambda x: [x[0],x[1],x[2]])

print Map_rdd.collect()[:10]
print "\n\n"

KeyValueRDD = rddN.map(lambda n:  (str(n[1]), float(n[2])))

'''

#DF

filey = "/root/Desktop/D1_AutoInsurance.csv"


spark = SparkSession.builder.master("local").appName("Task 4 App").config("spark.some.config.option", "some-value").getOrCreate()

df = spark.read.csv(filey,header=True,inferSchema=True) 

df.show()

print df.printSchema()


c = 0
flag1 = 0
def abc(l): # BroadCast c
	#global c
	if broadcastedflag.value[0] == 0:
		c = broadcasted.value[0]
		c+=1
		broadcastedflag.value[0]+=1
	else:
		broadcasted.value[0]+=1
		c = broadcasted.value[0]
	#broadcasted = sc.broadcast([c,d]) # WE CANNOT DO BROADCAST ON DRIVERS
	return(c,l)

flag2 = 0
d = 0
def abc2(l): # BroadCast d
	#global d
	if broadcastedflag.value[1] == 0:
		d = broadcasted.value[1]
		d+=1
		broadcastedflag.value[1]+=1
	else:
		broadcasted.value[1]+=1
		d=broadcasted.value[1]
	return(l,d)


broadcasted = sc.broadcast([c,d])

broadcastedflag = sc.broadcast([flag1,flag2])

CusDF = df.rdd.map(lambda l: abc(l[0])).toDF(["Key","Value"])

PrmDF = df.rdd.map(lambda l: abc2(l[12])).toDF(["Key","Value"])

#CusDF.limit(100).show()

#PrmDF.limit(100).show()


#JDF = CusDF.join(PrmDF,CusDF.Key == PrmDF.Key)


#JDF.show()
#print "OOOOOOOOOOOOOOOOMMMMMMMMMMMMMMMMMMMMMGGGGGGGGGGGGGGGGGGGGG"

'''
broadcastedPrmDF = sc.broadcast(PrmDF.rdd.collectAsMap())

rowFunc = lambda x: (x[0], x[1], broadcastedPrmDF.value.get(x[1], -1))
def mapFunc(partition):
    for row in partition:
        yield rowFunc(row)

JoinCusDFPrmDF = CusDF.rdd.mapPartitions(mapFunc,preservesPartitioning=True)

print JoinCusDFPrmDF.take(5)

'''

'''

foreach(func) 	Run a function func on each element of the dataset. This is usually done for side effects such as updating an Accumulator or interacting with external storage systems.
Note: modifying variables other than Accumulators outside of the foreach() may result in undefined behavior. See Understanding closures for more details.

'''

a = sc.accumulator(1)

def f(x):
	global a
	a += float(x["Customer Lifetime Value"])
	print(x["Customer Lifetime Value"])	

df.rdd.foreach(f)

print a.value


b = sc.accumulator(0)
def g(x):
	b.add(x["Customer Lifetime Value"])

df.rdd.foreach(g)

print b.value

'''

>>> from pyspark.accumulators import AccumulatorParam
>>> class VectorAccumulatorParam(AccumulatorParam):
...     def zero(self, value):
...         return [0.0] * len(value)
...     def addInPlace(self, val1, val2):
...         for i in xrange(len(val1)):
...              val1[i] += val2[i]
...         return val1
>>> va = sc.accumulator([1.0, 2.0, 3.0], VectorAccumulatorParam())
>>> va.value
[1.0, 2.0, 3.0]
>>> def g(x):
...     global va
...     va += [x] * 3
>>> rdd.foreach(g)
>>> va.value
[7.0, 8.0, 9.0]

>>> rdd.map(lambda x: a.value).collect() # doctest: +IGNORE_EXCEPTION_DETAIL
Traceback (most recent call last):
    ...
Py4JJavaError:...

>>> def h(x):
...     global a
...     a.value = 7
>>> rdd.foreach(h) # doctest: +IGNORE_EXCEPTION_DETAIL
Traceback (most recent call last):
    ...
Py4JJavaError:...

>>> sc.accumulator([1.0, 2.0, 3.0]) # doctest: +IGNORE_EXCEPTION_DETAIL
Traceback (most recent call last):
    ...
Exception:...

'''


