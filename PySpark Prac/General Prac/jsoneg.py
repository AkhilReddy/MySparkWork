#SELECT SUM(latitude) FROM generated WHERE age > 30 GROUP by company
# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext
from operator import add
import sys

## Python Imports
import csv, io

import re

import cStringIO

## Constants

APP_NAME = "HelloWorld of working on Json"

##OTHER FUNCTIONS/CLASSES

## Main functionality

def main(sc,filename):
	print "Test Code Started"
	print("\n\n")
	#df = spark.read.json(filename)
	#df.show()
	
	#Creating RDD
	#jsonRDD = sc.wholeTextFiles(filename).map(lambda x: x[1])
	jsonRDD = sc.textFile(filename).map(lambda x: x[0])
	print(jsonRDD.first())
	print("\n\n")

	#Creating Dataframe
	df = jsonRDD.map(lambda x: re.sub(r"\s+", "", x, flags=re.UNICODE))
	print(df.first())

	print("\n\n")
 	'''
	#Just for Testing
	print "Test Code Running"
	'''
        df_tranf_filter = df.filter(lambda l: l['age'] > 30).map(lambda l: (l['company'],l['latitude'])) 
	#rdd_tranf_filter = rdd.filter(lambda l: l.age > 30).map(lambda l: (l.company,l.latitude)) 
	sum_latitude = df_tranf_filter.reduceByKey(add).collect()
	'''
if __name__ == "__main__":
   # Configure Spark
   conf = SparkConf().setAppName(APP_NAME)
   conf = conf.setMaster("local[*]")
   sc   = SparkContext(conf=conf)
   filename = sys.argv[1]
   # Execute Main functionality
   main(sc, filename)







