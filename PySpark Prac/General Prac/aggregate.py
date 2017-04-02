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

conf = SparkConf().setMaster("local").setAppName("Aggregate Module")

sc = SparkContext(conf = conf)                                          # Initalising or Configuring "Spark Context"

sqlContext = SQLContext(sc)

# Aggregate :-  Count ; Sum ; Maximum ; Minimum ; Atmost ; Atleast ; Greatest ; Least

# Columns :- OrderID   CustomerID  EmployeeID  OrderDate  RequiredDate   ShippedDate  ShipVia   Freight	  ShipName	

#1 . Count
def Count(df): #Aggregate function: returns the number of items in a group or Column.
	return (df.count())

'''
toDF(*cols)
Returns a new class:DataFrame that with new specified column names

Parameters:	cols – list of new column names (string)
>>> df.toDF('f1', 'f2').collect()
[Row(f1=2, f2=u'Alice'), Row(f1=5, f2=u'Bob')]
'''

def Distinct_Col_Count(df,col1,col2): # Returns a distinct count based on columns of Data Frames.
	types = [f for f in df.schema.names]
	x1 = str(types[col1])
	x2 = str(types[col2])
        #return x1,x2
	x = df.rdd.map(lambda line: (line[col1], line[col2])).toDF([x1,x2]) #.distinct().count()
	#x = df.rdd.map(lambda line: Row(line.__fields__[col1]=(line[col1]),line.__fields__[col2]=(line[col2]))).toDF()
	return x.show()
	#df2 = sqlContext.createDataFrame(x)
'''
df.rdd.map(lambda row:
    Row(row.__fields__ + ["day"])(row + (row.date_time.day, ))
)
'''

def Distinct_Count(df): # distinct count on rows
	return df.distinct().count()	

def Distinct_Cols(df,col1,col2): # Returns a new Column for distinct count of col or cols.
	return (df.agg(func.countDistinct(df[col1], df[col2]).alias('c')).collect())

def Distinct_grouby_Count(df,col): # group by a column in a Spark dataframe and for each group count the unique values of column
	#df.groupby(df[col]).count().distinct().show()
	return (df.groupby(df[col]).count().distinct().show())

def DistinctCount_groupby(df,col1,col2): # count of distinct elements of each by group
	df.groupby(df[col1]).agg(func.countDistinct(df[col2])).show()
	return (df.groupby(df[col1]).agg(func.countDistinct(df[col2])).count())

def DistictDf_byDropDuplicates(df): # Return a new DataFrame with duplicate rows removed, optionally only considering certain columns.
	df.dropDuplicates().show()
	return (df.dropDuplicates())	

def DistictDf_byDropDuplicates(df,colname1,colname2): # Return a new DataFrame with duplicate rows removed only considering certain columns.
	df.dropDuplicates([colname1, colname2]).show()
	return (df.dropDuplicates([colname1, colname2]))

#2 . Sum
def Sum_col(df,colname): # Compute the sum for each numeric columns for each group.
	#print "ooooooooooommmmmmmmmmmmmmmmmggggggggggggggggggggg"
	#print "\n\n"
	#df.groupBy().sum(colname).collect()
	#print "\n\n"
	try:
		out = df.groupBy().sum(colname).show()
		if out:
			return out
		else:
			raise "AnalysisException"
	except: # pyspark.sql.utils.AnalysisException
		print "Given Column Name is not Numeric"
		print "\n\n" 
	#return(df.groupBy().sum(colname).show())
	#df.groupBy().sum('age').collect()
	
#To be Solved
def Sum(df,*col): # Compute the sum for each numeric columns for each group of column names (string). Non-numeric columns are ignored.
	#print col
	ls = [i for i in col]
	print ls
	return (df.groupBy().sum(ls).show())
	#print "\n\n"
	#df.groupBy().sum('age').collect()
	

#3 . Maximum
def Maximum_Col(df,col): #  returns the maximum value of the expression in a group or Column which are numeric type.
	#max(col)
	#df.agg(func.max(df[col])).count()
	types = [f.dataType for f in df.schema.fields]
	#print types
	#try:
	#print "aaaaaaaaaaaaaaaa"
	x = str(types[col])
	try: 
		if x == "IntegerType" or x == "LongType" :
			return (df.agg(func.max(df[col]).alias('Maximum['+str(col)+']')).show())
		else:
			raise TypeError
	except TypeError:
		print "Given Input Column Type is not Integer"
				
	#print x
	#print type(x)
	#if x == "StringType":
	#	print "OOOOOOKKKKKKKK"	
	#	print "\n\n"
			#return (df.agg(func.max(df[col]).alias('Maximum['+str(col)+']')).show())	
	#except:
	#	print "OOOOOOOMMMMGGGGGGGGG"


#4 . Minimum
def Minimum_Col(df,col): # returns the minimum value of the expression in a group or Column which are numeric type.
	types = [f.dataType for f in df.schema.fields]
	try: 
		if str(types[col]) == "IntegerType" or str(types[col]) == "LongType" :
			return (df.agg(func.min(df[col]).alias('Minimum['+str(col)+']')).show())
		else:
			raise TypeError
	except TypeError:
		print "Given Input Column Type is not Integer"
	

#5. Greatest
def Greatest_Col(df,col):
	types = [f.dataType for f in df.schema.fields]
	try: 
		if str(types[col]) != "IntegerType" or str(types[col]) != "LongType" :
			return (df.agg(func.max(df[col]).alias('Maximum['+str(col)+']')).show())
		else:
			raise TypeError
	except TypeError:
		print "Given Input Column Type is Numeric Type"

#6. Least
def Least_Col(df,col):
	types = [f.dataType for f in df.schema.fields]
	try: 
		if str(types[col]) != "IntegerType" or str(types[col]) != "LongType" :
			return (df.agg(func.min(df[col]).alias('Minimum['+str(col)+']')).show())
		else:
			raise TypeError
	except TypeError:
		print "Given Input Column Type is Numeric Type"


#7. Atmost	
def AtmostVal_Col(df,col,val):
	types = [f.dataType for f in df.schema.fields]
	try: 
		if str(types[col]) == "IntegerType" or str(types[col]) == "LongType" :
			return (df.filter(df[col] <= val).show())
		else:
			raise TypeError
	except TypeError:
		print "Given Input Column Type is not Numeric Type"

#8. Atleast	
def AtleastVal_Col(df,col,val):
	types = [f.dataType for f in df.schema.fields]
	try: 
		if str(types[col]) == "IntegerType" or str(types[col]) == "LongType" :
			return (df.filter(df[col] >= val).show())
		else:
			raise TypeError
	except TypeError:
		print "Given Input Column Type is not Numeric Type"

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
	coount = Count(df0)
	print coount
	print "\n\n"
	
	#print Distinct_Count(df0)
	print "\n\n"

	#x = input("Please give column Numbers of Tables : ")
 	#print x
	
	#print Distinct_Cols(df0,2,3)
	print "\n\n"

	print Distinct_Col_Count(df0,2,3)
	print "\n\n"

	#print Distinct_grouby_Count(df0,2)
	print "\n\n"

	#print DistinctCount_groupby(df0,2,3)
	print "\n\n"

	#df1 = DistictDf_byDropDuplicates(df0,'CustomerID','EmployeeID')		
		
	#print Sum_col(df0,'OrderID')	
	#print Sum_col(df0,'CustomerID')
	print "\n\n"

	#print Sum(df0,'OrderID','EmployeeID')	

	#print Maximum_Col(df0,0)
	print "\n\n"

	#print Minimum_Col(df0,'OrderID')
	print "\n\n"

	#print Greatest_Col(df0,'OrderDate')
	#print Greatest_Col(df0,3)
	print "\n\n"

	#print Least_Col(df0,'OrderDate')
	#print Least_Col(df0,0)
	print "\n\n"

	#print AtmostVal_Col(df0,1,5)
	print "\n\n"

	#print AtleastVal_Col(df0,2,5)
	print "\n\n"
	

	

	
        

       





