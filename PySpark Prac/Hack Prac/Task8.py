# -*- coding: utf-8 -*-
import numpy

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

dfN = df.select(df[6])

dfN.show()



from datetime import date, datetime

def convert(date_string):
	x = date_string.split("/")
	print x
	print "\n\n"
	date_object = date(*map(int,([x[2],x[0],x[1]])))
	return date_object

DF = df.rdd.map(lambda p:convert(p[6]))

print DF.collect()[:10]

#add_months(start, months)

#Returns the date that is months months after start

#dfn = DF.select(func.add_months(DF[6], 1).alias('AddedMonth'))

#print dfn.collect()[:10]

################################

OrdersFile = sc.textFile("/root/Desktop/example.csv")

header = OrdersFile.first()

schemaString = header.replace('"','')  # get rid of the double-quotes

#By Default We Set as StringType()
fields = [StructField(field_name, StringType(), False) for field_name in schemaString.split(',')]

print(fields)
print("\n\n")

print(len(fields) ) # how many elements in the header?
print "\n\n"


fields[2].dataType = FloatType()
fields[6].dataType = DateType()


print("\n\n")
print(fields)
print("\n\n")

schema = StructType(fields)

print(schema)
print("\n\n")

OrdersHeader = OrdersFile.filter(lambda l: "Customer" in l)
print(OrdersHeader.collect())
print("\n\n")

OrdersNoHeader = OrdersFile.subtract(OrdersHeader)
print(OrdersNoHeader.count())
print("\n\n")

print(OrdersNoHeader.collect())
print("\n\n")

from datetime import date, datetime

def convert(date_string):
	x = date_string.split("/")
	print x
	print "\n\n"
	date_object = date(*map(int,([x[2],x[0],x[1]])))
	return date_object

#(int(p[0]),str(p[1].encode("utf-8")),int(p[2]),convert(p[3]),convert(p[4]),convert(p[5]),int(p[6]),float(p[7]),str(p[8].encode("utf-8"))

Orders_temp = OrdersNoHeader.map(lambda k: k.split(",")).map(lambda p:(str(p[0].encode("utf-8")),str(p[1].encode("utf-8")),float(p[2]),str(p[3].encode("utf-8")),str(p[4].encode("utf-8")),str(p[5].encode("utf-8")),convert(p[6])))

print(Orders_temp.top(2)) 
print("\n\n")

Orders_df = sqlContext.createDataFrame(Orders_temp, schema)

print("Dtypes : ",Orders_df.dtypes)
print("\n\n")

print("Schema:  ",Orders_df.printSchema())
print("\n\n")

#add_months(start, months)

#Returns the date that is months months after start

dfn = Orders_df.select(func.add_months(Orders_df[6], 1).alias('AddedMonth'))

print "OOOOOOOOOOOOOOOOOOOOOOOOOMMMMMMMMMMMMMMMMMMMMMMMMMMMMGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG"
print "\n\n"

print dfn.collect()[:10]

dfn.show()

dfn2 = Orders_df.select(func.date_add(Orders_df[6], 1).alias('AddedDate'))

#dfn2.show()

#date_format(date, format)

dfn3 = Orders_df.select(func.date_format(Orders_df[6], 'MM/dd/yyy').alias('date'))

#dfn3.show()

#date_sub(start, days)

dfn4 = Orders_df.select(func.date_sub(Orders_df[6], 3).alias('date_sub'))

#dfn4.show()

#datediff(end, start)

dfn5 = Orders_df.select(func.datediff(Orders_df[6],Orders_df[6]).alias('datediff'))

#dfn5.show()

#dayofmonth(col)

dfn6 = Orders_df.select(func.dayofmonth(Orders_df[6]).alias('dayofmonth'))

#dfn6.show()

#dayofyear(col)

dfn7 = Orders_df.select(func.dayofyear(Orders_df[6]).alias('dayofyear'))

#dfn7.show()

#functions.last_day(date)

dfn8 = Orders_df.select(func.last_day(Orders_df[6]).alias('last_day'))

#dfn8.show()

#functions.month

dfn9 = Orders_df.select(func.month(Orders_df[6]).alias('month'))

dfn9.show()

#functions.months_between(date1, date2)

dfn10 = Orders_df.select(func.months_between(Orders_df[6],Orders_df[6]).alias('months_between'))

#dfn10.show()

#functions.next_day(date, dayOfWeek)

dfn11 = Orders_df.select(func.next_day(Orders_df[6],'Sun').alias('next_day'))

#dfn11.show()

#functions.to_date(col)

dfn12 = Orders_df.select(func.to_date(Orders_df[6]).alias('to_date'))

#dfn12.show()

#functions.weekofyear(col)

dfn13 = Orders_df.select(func.weekofyear(Orders_df[6]).alias('weekofyear'))

#dfn13.show()

#functions.year(col)

dfn14 = Orders_df.select(func.year(Orders_df[6]).alias('year'))

#dfn14.show()

#Quarter(col)


def Quarter(x):
	#d = { '01' : 'Q1' , '02' : 'Q1' , '03' : 'Q1' , '04' : 'Q2' , '05' : 'Q2' , '06' : 'Q2' , '07' : 'Q3', '08' : 'Q3' , '09' : 'Q3' , '10' : 'Q4' , '11' : 'Q4' , '12' : 'Q4' }

	d = { '1' : 'Q1' , '2' : 'Q1' , '3' : 'Q1' , '4' : 'Q2' , '5' : 'Q2' , '6' : 'Q2' , '7' : 'Q3', '8' : 'Q3' , '9' : 'Q3' , '10' : 'Q4' , '11' : 'Q4' , '12' : 'Q4' ,' ' : ' '} 

	print "OOOOOOOK"
	print x
	
	return(d[str(x.month)])

dfn15  = Orders_df.rdd.map(lambda l: Row(Quarter = Quarter(l[6]))).toDF()

#dfn15 = dfi.map(lambda line: Row(Quarter = line[0])).toDF()

#dfn15 = Orders_df.select(Quarter(Orders_df[6]))

#print dfn15.collect()

dfn15.show()

#functions.quarter(col)

dfn16 = Orders_df.select(func.quarter(Orders_df[6]).alias('quarter'))

dfn16.show()

#FiscalWeek  || FiscalYear


#Month Start Date

#date_list = [my_dt_ob.year, my_dt_ob.month, my_dt_ob.day, my_dt_ob.hour, my_dt_ob.minute, my_dt_ob.second]

def Month_Start_Date(x):
	
	x = x.replace(day=1) 
	
        print x.day
	return x

dfn17  = Orders_df.rdd.map(lambda l: Row(Month_Start_Date = Month_Start_Date(l[6]))).toDF()

dfn17.show()




