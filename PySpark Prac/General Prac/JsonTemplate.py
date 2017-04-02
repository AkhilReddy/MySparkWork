from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)


from operator import add
import sys

## Python Imports
import csv, io

import re

import cStringIO

#Python Imports
import csv, io

import re

import cStringIO

#Just for Testing
print "Test Code Running"

#Creating RDD
jsonRDD = sc.wholeTextFiles("/home/akhil/Desktop/generated.json").map(lambda x: x[1])

print(jsonRDD.first())

print("\n\n")

js = jsonRDD.map(lambda x: re.sub(r"\s+", "", x, flags=re.UNICODE))

print(js.first())


print("\n\n       GGGGGGGGGGGGGGGGGGOOOOOOOOOOOOOOOOOOOOOOOOOOOOOODDDDDDDDDDDDDDDDDDDDDDDDDDDDDD    \n\n")

for x in js.collect():
	print(x)
	print("\n\n")


#df_tranf_filter = js.filter(lambda l: l['age'] > 30).map(lambda l: (l['company'],l['latitude'])) 

print("\n\n            OOOOOOOOOOOMMMMMMMMMMMMMMMMGGGGGGGGGGGGGGGGGGGG    \n\n")

#print("       QQWRGFDHBVCNBVCNBVCNBVHBNBVCNBVCBV :       ",df_tranf_filter.first())
#sum_latitude = df_tranf_filter.reduceByKey(add).collect()



