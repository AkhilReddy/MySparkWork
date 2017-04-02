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
#combineByKey()

filey = sc.textFile("/root/Desktop/D1_AutoInsurance.csv")

rdd = filey.map(lambda line: line.split(","))

print rdd.count()

header = rdd.first() #extract header
rddN = rdd.filter(lambda row : row != header)   #filter out header

Map_rdd = rddN.map(lambda x: [x[0],x[1],x[2]])

print Map_rdd.collect()[:10]
print "\n\n"

KeyValueRDD = rddN.map(lambda n:  (str(n[1]), float(n[2])))

def add(a, b): 
	return a + int(b)


print sorted(KeyValueRDD.combineByKey(int, add, add).collect())


#DF
'''
filey = "/root/Desktop/D1_AutoInsurance.csv"


spark = SparkSession.builder.master("local").appName("Task 4 App").config("spark.some.config.option", "some-value").getOrCreate()

df = spark.read.csv(filey,header=True,inferSchema=True) 

df.show()

#print df.printSchema()

'''
