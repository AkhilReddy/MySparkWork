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

#Mathematical :-	Abs	Log	Exponential	Inverse	Factorial	Ceiling	Floor	Mod	Power	Radians	Round	Square	Cube	Square Root	Cube Root	Sine	Cosine

# Columns :- OrderID   CustomerID  EmployeeID  OrderDate  RequiredDate   ShippedDate  ShipVia   Freight	  ShipName	

################################################################################################################################################

#1. Abs	
# Data Type Exceptiion
def Absolute(df,col):
	#return(df.groupby(df[col]).agg(func.abs(df[col])).show())
	#return(df.select(func.abs(df[col]).alias('Abs')).collect())
	#return(df.select(func.abs(df[col]).alias('Abs')).map(lambda l: str(l.Abs)).collect())
	return(df.select(func.abs(df[col]).alias('Abs')).show())

#2. Log	
# Data Type Exceptiion
def Log(df,logval,col): # Returns the first argument-based logarithm of the second argument.If there is only one argument, then this takes the natural logarithm of the argument.
	#return(df.groupby(df[col]).agg(func.log(logval,df[col])).show())
	#df.select(log(logval, df[col]).alias('ten')).map(lambda l: str(l.ten)).collect()
	#return(df.select(func.log(logval,df[col]).alias('Log')).show())
	#df.select(func.log(10.0, df.EmployeeID).alias('Ten')).map(lambda l: str(l.ten)[:7]).collect()
	#return(df.select(func.log(logval, df[col]).alias('ten')).collect())
	return(df.select(func.log(logval, df[col]).alias('Log'+str(logval))).show())

def Log10(df,col): # Computes the logarithm of the given value in Base 10.
	return(df.select(func.log10(df[col]).alias('Log10')).show())

def LogNat(df,col): # Computes the natural logarithm of the given value plus one.
	return(df.select(func.log1p(df[col]).alias('LogNat+1')).show())

def Log2(df,col): # Returns the base-2 logarithm of the argument.
	return(df.select(func.log2(df[col]).alias('Log2')).show())

#3. Exponential	
# Data Type Exceptiion
def Exponential(df,col): # Computes the exponential of the given value.
	return(df.select(func.exp(df[col]).alias('Exponential')).show())

def ExponentialNeg1(df,col): # Computes the exponential of the given value minus one.
	#return(df.select(func.expm1(df[col]).alias('Exponential-1'))) # DataFrame[Exponential-1: double]
	return(df.select(func.expm1(df[col]).alias('Exponential-1')).show())

#4. Inverse	
# Data Type Exceptiion
'''

'''
#5. Factorial
# Data Type Exceptiion
def Factorial(df,col): # Computes the factorial of the given value.
	#return(df.select(factorial(df[col]).alias('Factorial')).collect())	
	return(df.select(func.factorial(df[col]).alias('Factorial')).show())	
	
#6. Ceiling
# Data Type Exceptiion
def Ceiling(df,col): # Computes the ceiling of the given value.
	#return(df.select(func.ceil(df[col]).alias('Ceiling')).collect())
	return(df.select(func.ceil(df[col]).alias('Ceiling')).show())
#7. Floor	
# Data Type Exceptiion
def Floor(df,col): # Computes the floor of the given value.
	#return(df.select(func.floor(df[col]).alias('Floor')).collect())
	return(df.select(func.floor(df[col]).alias('Floor')).show())
#8. Mod	
# Data Type Exceptiion
'''

'''
#9. Power	
# Data Type Exceptiion
def PowerCols(df,col1,col2):
	#return(df.select(func.pow(df[col1],df[col2]).alias('Power')).collect())
	return(df.select(func.pow(df[col1],df[col2]).alias('Power')).show())

def PowerVal(df,col,val):
	#return(df.select(func.pow(df[col],val).alias('Power'+str(val))).collect())
	return(df.select(func.pow(df[col],val).alias('Power'+str(val))).show())

#10. Radians	
# Data Type Exceptiion
def Radians(df,col): # Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
	return(df.select(func.toRadians(df[col]).alias('Radians')).show())

def Degrees(df,col): # Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
	return(df.select(func.toDegrees(df[col]).alias('Degrees')).show())

#11. Round	
# Data Type Exceptiion
def RoundUP(df,col,scale=0): # Round the given value to scale decimal places using HALF_UP rounding mode if scale >= 0 or at integral part when scale < 0.
	return(df.select(func.round(df[col],scale).alias('RoundUP')).show())

def RoundEVEN(df,col,scale=0): # Round the given value to scale decimal places using HALF_UP rounding mode if scale >= 0 or at integral part when scale < 0.
	return(df.select(func.bround(df[col],scale).alias('RoundEVEN')).show())

#12. Square	
# Data Type Exceptiion
def Square(df,col):
	sqr = udf(lambda s: (s*s), IntegerType()) 
	#return(df.rdd.map(lambda line: (line[col]*line[col])))#.toDF(['SquaredCol']))
	return df.select(sqr(df[col]).alias('SquaredCol')).show()
#13. Cube	
# Data Type Exceptiion
def Cube(df,col):
	cub = udf(lambda s: (s*s*s), IntegerType()) 
	#return(df.rdd.map(lambda line: (line[col]*line[col])))#.toDF(['SquaredCol']))
	return df.select(cub(df[col]).alias('CubedCol')).show()
#14. Square Root	
# Data Type Exceptiion
def SquareRoot(df,col): # Computes the square root of the specified float value.
	return(df.select(func.sqrt(df[col]).alias('SquareRoot')).show())
#15. Cube Root	
# Data Type Exceptiion
def CubeRoot(df,col): # Computes the cube-root of the given value.
	return(df.select(func.cbrt(df[col]).alias('CubeRoot')).show())

#16. Sine	
# Data Type Exceptiion
def Sin(df,col): # Computes the sine of the given value.
	return(df.select(func.sin(df[col]).alias('Sin')).show())

def Sinh(df,col): # Computes the hyperbolic sine of the given value.
	return(df.select(func.sinh(df[col]).alias('Hyperbolic Sine')).show())

def aSin(df,col): # Computes the sine inverse of the given value; the returned angle is in the range-pi/2 through pi/2.
	return(df.select(func.asin(df[col]).alias('Sine Inverse')).show())

#17. Cosine
# Data Type Exceptiion
def Cos(df,col): # Computes the cosine of the given value.
	return(df.select(func.cos(df[col]).alias('Cos')).show())

def Cosh(df,col): # Computes the hyperbolic cosine of the given value.
	return(df.select(func.cosh(df[col]).alias('Hyperbolic Cosine')).show())

def aCos(df,col): # Computes the cosine inverse of the given value; the returned angle is in the range0.0 through pi.
	return(df.select(func.acos(df[col]).alias('Cosine Inverse')).show())


################################################################################################################################################
	
def test(*args):
	x = args[0]
	#y = args[1]
	print "\n\n"
	print x
	print "\n\n"
	#print y
	#print "\n\n"

def convert(date_string):
	date_new = date_string.split(" ")[0]
	date_object = date(*map(int,(date_string.split(" ")[0].split("-"))))
	return date_object

def dataFrame_Maker(*args):
	File = args[0]
	OrdersFile = sc.textFile(File)

	header = OrdersFile.first()

	schemaString = header.replace('"','')  # get rid of the double-quotes

	#By Default We Set as StringType()
	fields = [StructField(field_name, StringType(), False) for field_name in schemaString.split(',')]

	#print(fields)
	#print("\n\n")

	#print(len(fields) ) # how many elements in the header?
	#print "\n\n"


	fields[0].dataType = LongType()
	fields[2].dataType = IntegerType()
	fields[3].dataType = DateType()
	fields[4].dataType = DateType()
	fields[5].dataType = DateType()
	fields[6].dataType = IntegerType()
	fields[7].dataType = FloatType()

	#print("\n\n")
	#print(fields)
	#print("\n\n")

	schema = StructType(fields)

	#print(schema)
	#print("\n\n")

	OrdersHeader = OrdersFile.filter(lambda l: "OrderID" in l)
	#print(OrdersHeader.collect())
	#print("\n\n")

	OrdersNoHeader = OrdersFile.subtract(OrdersHeader)
	#print(OrdersNoHeader.count())
	#print("\n\n")

	#print(OrdersNoHeader.collect())
	#print("\n\n")

	Orders_temp = OrdersNoHeader.map(lambda k: k.split(",")).map(lambda p:(int(p[0]),str(p[1].encode("utf-8")),int(p[2]),convert(p[3]),convert(p[4]),convert(p[5]),int(p[6]),float(p[7]),str(p[8].encode("utf-8"))))

	#print(Orders_temp.top(2)) 
	#print("\n\n")

	Orders_df = sqlContext.createDataFrame(Orders_temp, schema)

	#print("Dtypes : ",Orders_df.dtypes)
	#print("\n\n")
	
	#print("Schema:  ",Orders_df.printSchema())
	#print("\n\n")

	return Orders_df
	

if __name__ == "__main__":
	inputer = sys.argv[1]
	#test(inputer)
	
	df0 = dataFrame_Maker(inputer)
	
	#print Absolute(df0,2)
	print "\n\n"

	#print Log(df0,10.0,2)
	print "\n\n"

	#print Log2(df0,2)
	print "\n\n"

	#print ExponentialNeg1(df0,2)
	print "\n\n"
	
	#print Factorial(df0,2)
	print "\n\n"
	
	#print PowerCols(df0,2,2)
        print "\n\n"

	#print PowerVal(df0,2,0)
	print "\n\n"

	#print Degrees(df0,2)
	print "\n\n"

	#print RoundEVEN(df0,2,1)
	print "\n\n"

	#print SquareRoot(df0,2)
	print "\n\n"

	#print aSin(df0,2)
	print "\n\n"

	#print Cosh(df0,2)
	print "\n\n"
	
	print Square(df0,2)
	print "\n\n"
       





