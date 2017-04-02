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


#Other :- Currency	Temperature	Timezone	Weight	Length	GeoIP


# Columns :- OrderID   CustomerID  EmployeeID  OrderDate  RequiredDate   ShippedDate  ShipVia   Freight	  ShipName

#############################################################################################################################################

#1. Currency
def Currency(df,col,conv1,conv2):
	r = conv1/conv2
	cur = udf(lambda s: (s*r), FloatType())
	return df.select(cur(df[col]).alias('CurrencyConversion')).show()
	
#2. Temperature
def TemperatureDeg(df,col):
	deg = udf(lambda s: ((s-32)*0.556), FloatType())
	return df.select(deg(df[col]).alias('CelsiusTemperature')).show()

def TemperatureFrh(df,col):
	frh = udf(lambda s: ((s*1.5)+32), FloatType())
	return df.select(frh(df[col]).alias('FahrenheitTemperature')).show()

#3. Timezone

def TimeConverterGMT(df,col,val):
	Conv = {'GMT':0 , 'EDT':-4 , 'EST' : -5 ,'CDT' : -5 , 'CST' : -6 ,'MDT' : -6 ,'MST' : -7,'PDT' : -7 , 'PST' : -8 ,'IST' : 5.5 ,'Sydney': 10 ,'UTC' : 0 }
	conv = udf(lambda s: (s+Conv[val]), FloatType())
	return df.select(conv(df[col]).alias('GMTConverter')).show()

#4. Weight
def WeightConverter_Gr_Kg(df,col):
	wtconv = udf(lambda s: (s/1000), FloatType())
	return df.select(wtconv(df[col]).alias('GramToKgConverter')).show()

def WeightConverter_Kg_Gr(df,col):
	wtconv = udf(lambda s: (s*1000), IntegerType())
	return df.select(wtconv(df[col]).alias('KgToGramConverter')).show()

#5. Length	
def LengthConverter_Mtr_Km(df,col):
	Lnconv = udf(lambda s: (s/1000), IntegerType())
	return df.select(Lnconv(df[col]).alias('MtrToKmConverter')).show()

def LengthConverter_Km_Mtr(df,col):
	Lnconv = udf(lambda s: (s*1000), IntegerType())
	#return df.select(Lnconv(df[col]).alias('KmToMtrConverter')).show()
	dfn = df.select(df[0],Lnconv(df[col]).alias('KmToMtrConverter'))
	#return dfn.show()
	#return df.join(dfn,dfn[0]==df[0]).show()
	return df.join(dfn,["OrderID"]).show()
	
#6. GeoIP
'''
Not A Good Transform 

But Can be solved by using Dictionary
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

	#print TemperatureFrh(df0,2)
	print "\n\n"

	print ("Please Select TimeZones for Conversion:",'GMT', 'EDT', 'EST','CDT', 'CST','MDT','MST','PDT', 'PST','IST','Sydney','UTC')
	#print TimeConverterGMT(df0,2,'IST')
	print "\n\n"

	print LengthConverter_Km_Mtr(df0,2)
	print "\n\n"



	












