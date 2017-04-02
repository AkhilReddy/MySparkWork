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

conf = SparkConf().setMaster("local").setAppName("Aggregate Module")

sc = SparkContext(conf = conf)                                          # Initalising or Configuring "Spark Context"

sqlContext = SQLContext(sc)

#############################################################################################################################################


# Columns :- OrderID   CustomerID  EmployeeID  OrderDate  RequiredDate   ShippedDate  ShipVia   Freight	  ShipName

#############################################################################################################################################

#1. Rank

'''
pyspark.sql.functions.dense_rank()
Window function: returns the rank of rows within a window partition, without any gaps.

The difference between rank and denseRank is that denseRank leaves no gaps in ranking sequence when there are ties. That is, if you were ranking a competition using denseRank and had three people tie for second place, you would say that all three were in second place and that the next person came in third.

pyspark.sql.functions.percent_rank()
Window function: returns the relative rank (i.e. percentile) of rows within a window partition.

pyspark.sql.functions.rank()
Window function: returns the rank of rows within a window partition.

The difference between rank and denseRank is that denseRank leaves no gaps in ranking sequence when there are ties. That is, if you were ranking a competition using denseRank and had three people tie for second place, you would say that all three were in second place and that the next person came in third.

This is equivalent to the RANK function in SQL.
'''
#def Rank(df,col):
	#return(df.select(df[col].func.rank()).alias('Rank').show())
#	return df.select(df[col]).alias("Rank").withColumn("Rank", func.rank()).collect()

def Rank(df,colnme):
	lookup = df.select(colnme).distinct().orderBy(colnme).rdd.zipWithIndex().map(lambda x: x[0] + (x[1], )).toDF([colnme, "Rank"])
        return df.join(lookup, [colnme]).withColumn("Rank", Column("Rank"))
#2. Percentile

'''
pyspark.sql.functions.percent_rank()
Window function: returns the relative rank (i.e. percentile) of rows within a window partition.
'''


#3. Quartile

'''
def quartiles(dataPoints):
   # check the input is not empty
  if not dataPoints:
    raise StatsError('no data points passed')
   # 1. order the data set
  sortedPoints = sorted(dataPoints)
   # 2. divide the data set in two halves
  mid = len(sortedPoints) / 2

if (len(sortedPoints) % 2 == 0):
   # even
   lowerQ = median(sortedPoints[:mid])
   upperQ = median(sortedPoints[mid:])
 else:
   # odd
   lowerQ = median(sortedPoints[:mid])  # same as even
   upperQ = median(sortedPoints[mid+1:])

return (lowerQ, upperQ)
'''

#4. Decile

'''


'''


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
	
	df0 = dataFrame_Maker(inputer)
	
	print "\n\n"

	print Rank(df0,'EmployeeID')
	print "\n\n"



	
