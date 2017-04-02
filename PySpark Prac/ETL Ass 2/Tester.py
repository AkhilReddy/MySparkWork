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

from pyspark.sql.types import *  # Imports all data Types

from pyspark.sql import SparkSession

import pyspark.sql.functions as func

from pyspark.sql.functions import udf

from pyspark.sql import Window

conf = SparkConf().setMaster("local").setAppName("Aggregate Module")

sc = SparkContext(conf = conf)                                          # Initalising or Configuring "Spark Context"

sqlContext = SQLContext(sc)

#############################################################################################################################################

# Columns :- OrderID   CustomerID  EmployeeID  OrderDate  RequiredDate   ShippedDate  ShipVia   Freight	  ShipName

#############################################################################################################################################



################################################################################################################################################
	
def convert(date_string):
	date_new = date_string.split(" ")[0]
	date_object = date(*map(int,(date_string.split(" ")[0].split("-"))))
	return date_object

def dataFrame_Maker(*args):
	File = args[0]
	OrdersFile = sc.textFile(File)

	header = OrdersFile.first()

	schemaString = header.replace('"','')  # get rid of the double-quotes

	fields = [StructField(field_name, StringType(), False) for field_name in schemaString.split(',')]

	fields[0].dataType = LongType()
	fields[2].dataType = IntegerType()
	fields[3].dataType = DateType()
	fields[4].dataType = DateType()
	fields[5].dataType = DateType()
	fields[6].dataType = IntegerType()
	fields[7].dataType = FloatType()

	schema = StructType(fields)

	OrdersHeader = OrdersFile.filter(lambda l: "OrderID" in l)

	OrdersNoHeader = OrdersFile.subtract(OrdersHeader)

	Orders_temp = OrdersNoHeader.map(lambda k: k.split(",")).map(lambda p:(int(p[0]),str(p[1].encode("utf-8")),int(p[2]),convert(p[3]),convert(p[4]),convert(p[5]),int(p[6]),float(p[7]),str(p[8].encode("utf-8"))))

	Orders_df = sqlContext.createDataFrame(Orders_temp, schema)

	return Orders_df
	

if __name__ == "__main__":
	inputer = sys.argv[1]

	spark = SparkSession.builder.master("local").appName("Aki-Karthi App").config("spark.some.config.option", "some-value").getOrCreate()

	df = spark.read.csv(inputer,header=True,inferSchema=True) 

	#(schema=None, sep=None, encoding=None, quote=None, escape=None, comment=None, header=None, inferSchema=None, ignoreLeadingWhiteSpace=None, ignoreTrailingWhiteSpace=None, nullValue=None, nanValue=None, positiveInf=None, negativeInf=None, dateFormat=None, maxColumns=None, maxCharsPerColumn=None, maxMalformedLogPerPartition=None, mode=None)

	df.show()

        #print(df.schema)

	print df.printSchema()
	
	#df0 = dataFrame_Maker(inputer)
	
	'''
	print "\n\n"

	customers = sc.parallelize([("Alice", "2016-05-01", 50.00),("Alice", "2016-05-03", 45.00),("Alice", "2016-05-04", 55.00),("Bob", "2016-05-01", 25.00),("Bob", "2016-05-04", 29.00),("Bob", "2016-05-06", 27.00)]).toDF(["Name", "Date", "AmountSpent"])	
	print customers.show()	
	print "\n\n"

        #wSpec1 = Window.partitionBy("Name").orderBy("Date").rowsBetween(-1, 1)

	# customers.withColumn("MovingAvg",func.avg(customers("AmountSpent")).over(wSpec1)).show()

	#customers.withColumn("MovingAvg",customers.filter(customers("AmountSpent") > 30)).show()

	#customers.filter("AmountSpent > 30").show()
	
	a = "AmountSpent"
	b = '>'
	c = str(30)
	x = a+b+c
	#customers.filter("AmountSpent > 30").show()
	print a

	customers.filter(x).show()


	'''


		



	







































