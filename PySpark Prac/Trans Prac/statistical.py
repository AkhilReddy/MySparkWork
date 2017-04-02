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

from pyspark.sql import DataFrameStatFunctions

from pyspark.sql import types

from pyspark.sql.types import *  # Imports all data Types

from pyspark.sql import SparkSession

import pyspark.sql.functions as func

#import pyspark.sql.DataFrameStatFunctions as statfunc

conf = SparkConf().setMaster("local").setAppName("Aggregate Module")

sc = SparkContext(conf = conf)  # Initalising or Configuring "Spark Context"

sqlContext = SQLContext(sc)

# Columns :- OrderID   CustomerID  EmployeeID  OrderDate  RequiredDate   ShippedDate  ShipVia   Freight	  ShipName	

# Statistical :- Mean  ; Median ; Mode ; Standard Deviation ; Variance ; Correlation ; Covariance ; Standardize ; Normalize

################################################################################################################################################

#1. Mean
# Data Type Exceptiion
def Mean_col(df,col): 
	#return(df.agg(func.mean(df[col]))) # DataFrame[avg(EmployeeID): double]
	return(df.agg(func.mean(df[col])).collect()[0][0])

#2. Median
# Data Type Exceptiion
def Median(df,col):
	df_sorted = df.sort(df[col].desc()).collect()
	#return df_sorted
	length = df.count()
	
	if not length % 2:
		return((df_sorted[length / 2][col] + df_sorted[(length / 2) - 1][col]) / 2.0)
	else:
		return(df_sorted[length / 2][col])
	'''   
length = len(sorts)
    if not length % 2:
        return (sorts[length / 2] + sorts[length / 2 - 1]) / 2.0
    return sorts[length / 2]
	'''

#3. Mode 
# Data Type Exceptiion
def Mode_Col(df,col):
	#return(df.groupby(df[col]).count().distinct().groupby(df[col]).max('count').collect())
	#return(df.groupby(df[col]).max(str(df[col])).collect())
	dfi = df.groupby(df[col]).count().distinct()
	#return(dfi.show())	
	#return(dfi.agg(func.max(dfi[1])).show())
	#return dfi.orderBy(func.desc("count")).first()
	return dfi.orderBy(func.desc("count")).collect()[0][0]
	#return dfi.sort(dfi.count.desc()).show()
	
#4. Standard Deviation 
# Data Type Exceptiion
def StandardDev(df,col): # returns the unbiased sample standard deviation of the expression in a group.
	return(df.agg(func.stddev(df[col])).collect()[0][0]) 	

def StandardDev_pop(df,col): # returns population standard deviation of the expression in a group.
	return(df.agg(func.stddev_pop(df[col])).collect()[0][0])

def StandardDev_sample(df,col): # returns the unbiased sample standard deviation of the expression in a group.Almost same as StandardDev.
	return(df.agg(func.stddev_samp(df[col])).collect()[0][0])

#5. Variance 
# Data Type Exceptiion
def Variance(df,col):
	return(df.agg(func.variance(df[col])).collect()[0][0])

def VariancePop(df,col):
	return((df.agg(func.var_pop(df[col])).collect()[0][0]))

def VarianceSamp(df,col):
	return((df.agg(func.var_samp(df[col])).collect()[0][0]))

#6. Correlation 
# Data Type Exceptiion
def Correlation(df,col1,col2,method=None): # Calculates the correlation of two columns of a DataFrame as a double value. Currently only supports the Pearson Correlation Coefficient.So method is specified as "None".
	return((df.agg(func.corr(df[col1],df[col2])).collect()[0][0])) 

#7. Covariance 
# Data Type Exceptiion
#Explore Later
def Covariance(df,col1,col2): # Calculate the sample covariance for the given columns, specified by their names, as a double value.
	#return((df.stat.cov(str(df[col1]),str(df[col2])).collect()[0][0])) 
	#return((df.agg(DataFrameStatFunctions.cov(df[col1],df[col2])).collect()[0][0]))
	return(df.cov(col1,col2))
 
#8. Standardize     
# Data Type Exceptiion
#Broad Cast mean,stddev in sc
'''
broadcast = sc.broadcast([mean,stddev])

mean= broadcast.value[0]
stddev = broadcast.value[1]
'''
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf

def Standardize(df,col,colnum):
	mean = Mean_col(df,colnum)
	stddev = StandardDev(df,colnum)
	print mean,stddev
	std = udf(lambda s: ((s-mean)/stddev), FloatType())
	#return df.select(std(df[colnum]).alias(col+'Standardize')).collect()
	return df.select(std(df[colnum]).alias(col+'Standardize')).show()
	#newDF = df.rdd.map(lambda x: ((x[colnum]-mean)/stddev) ).toDF([col+' Standardize'])
	#newDF = df.rdd.map(lambda line: (line[colnum])).toDF()
	#x = df.rdd.map(lambda line: Row(col =((x[colnum]-mean)/stddev))).toDF()
	#return (x.show())
	
	
# To be decided correct DataType or Use map to select [:7] places
#9. Normalize
#Data Type Exceptiion
#Broad Cast mini,maxi in sc
'''
broadcast = sc.broadcast([mini,maxi])

mini = broadcast.value[0]
maxi = broadcast.value[1]
'''
'''
Xnew = (X - Xmin)/(Xmax - Xmin)
'''
def Normalize(df,col,colnum):
	maxi = df.agg(func.max(df[col])).collect()[0][0]
	mini = df.agg(func.min(df[col])).collect()[0][0]
	print maxi,mini
	rg = maxi - mini
	nor = udf(lambda s: ((s-mini)/rg), IntegerType()) # DoubleType FloatType IntegerType
	return df.select(nor(df[colnum]).alias(col+'Normalize')).show()


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
	
	print "\n\n"

	print Mode_Col(df0,2)
	print "\n\n"

	#print Mean_col(df0,2)
	print "\n\n"

	#print StandardDev(df0,2)
	print "\n\n"

	#print StandardDev_pop(df0,2)
	print "\n\n"

	#print StandardDev_sample(df0,2)
	print "\n\n"

	#print Variance(df0,2)
	print "\n\n"

	#print VariancePop(df0,2)
	print "\n\n"

	#print VarianceSamp(df0,2)
	print "\n\n"

	#print Correlation(df0,0,2)
	print "\n\n"

	#print Correlation(df0,3,4)
	#print "\n\n"

	"""
Exception:
	pyspark.sql.utils.AnalysisException: u"cannot resolve 'corr(`OrderDate`, `RequiredDate`)' due to data type mismatch: argument 1  requires double type, however, '`OrderDate`' is of date type. argument 2 requires double type, however, '`RequiredDate`' is of date type.;"
	"""

	#print Covariance(df0,'OrderID','EmployeeID')
	print "\n\n"
	
	#print Standardize(df0,'EmployeeID',2)
	print "\n\n"

	#print Median(df0,2)
	print "\n\n"

	print Normalize(df0,'EmployeeID',2)
	print "\n\n"
	

	

	
        

       





