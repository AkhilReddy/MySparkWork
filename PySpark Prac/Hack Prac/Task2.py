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

header = rdd.first() #extract header
rddN = rdd.filter(lambda row : row != header)   #filter out header

Map_rdd = rdd.map(lambda x: [x[0],x[1],x[2]])

print Map_rdd.collect()[:10]
print "\n\n"

c = 0

def abc(l): # BroadCast c
	global c
	c+=1
	return(c,l)


DfMap = Map_rdd.map(lambda l: abc(l))

print DfMap.collect()[:10]
print "\n\n"

DfEnuMap = Map_rdd.map(lambda l: list(enumerate(l)))

print DfEnuMap.collect()

print "\n\n"


Map_rddN = rdd.map(lambda x: [[x[0],x[1],x[2]]])

#DfEnuMapL = Map_rddN.map(lambda l: enumerate(l))

#print DfEnuMapL.collect()

#print "\n\n"

#PrintDfEnuMap = DfEnuMap.map(lambda l : l[0]).collect()

#print PrintDfEnuMap 

'''
DfEnuMapP = Map_rdd.map(lambda l: print(l))

print DfEnuMapP
print "\n\n"
'''

