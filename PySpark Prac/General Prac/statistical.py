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
'''
mean(*args)
Computes average values for each numeric columns for each group.

mean() is an alias for avg().

Parameters:	cols – list of column names (string). Non-numeric columns are ignored.
>>> df.groupBy().mean('age').collect()
[Row(avg(age)=3.5)]
>>> df3.groupBy().mean('age', 'height').collect()
[Row(avg(age)=3.5, avg(height)=82.5)]

pyspark.sql.functions.mean(col)
Aggregate function: returns the average of the values in a group.

pyspark.sql.functions.mean(col)
Aggregate function: returns the average of the values in a group.

agg(func.max(df[col]).alias('Maximum['+str(col)+']')).show()
'''
def Mean_col(df,col):
	#return(df.agg(func.mean(df[col]))) # DataFrame[avg(EmployeeID): double]
	return(df.agg(func.mean(df[col])).collect()[0][0])

#2. Median
'''
def median(mylist):
    sorts = sorted(mylist)
    length = len(sorts)
    if not length % 2:
        return (sorts[length / 2] + sorts[length / 2 - 1]) / 2.0
    return sorts[length / 2]

df.sort(col("ShipSum").desc())
sortWithinPartitions(*cols, **kwargs)
Returns a new DataFrame with each partition sorted by the specified column(s).

Parameters:	
cols – list of Column or column names to sort by.
ascending – boolean or list of boolean (default True). Sort ascending vs. descending. Specify list for multiple sort orders. If a list is specified, length of the list must equal length of the cols.
>>> df.sortWithinPartitions("age", ascending=False).show()

---------------------------------

sort(*cols, **kwargs)
Returns a new DataFrame sorted by the specified column(s).

Parameters:	
cols – list of Column or column names to sort by.
ascending – boolean or list of boolean (default True). Sort ascending vs. descending. Specify list for multiple sort orders. If a list is specified, length of the list must equal length of the cols.
>>> df.sort(df.age.desc()).collect()
[Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
>>> df.sort("age", ascending=False).collect()
[Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
>>> df.orderBy(df.age.desc()).collect()
[Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
>>> from pyspark.sql.functions import *
>>> df.sort(asc("age")).collect()
[Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
>>> df.orderBy(desc("age"), "name").collect()
[Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
>>> df.orderBy(["age", "name"], ascending=[0, 1]).collect()
[Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]


---------------------------

orderBy(*cols, **kwargs)
Returns a new DataFrame sorted by the specified column(s).

Parameters:	
cols – list of Column or column names to sort by.
ascending – boolean or list of boolean (default True). Sort ascending vs. descending. Specify list for multiple sort orders. If a list is specified, length of the list must equal length of the cols.
>>> df.sort(df.age.desc()).collect()
[Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
>>> df.sort("age", ascending=False).collect()
[Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
>>> df.orderBy(df.age.desc()).collect()
[Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
>>> from pyspark.sql.functions import *
>>> df.sort(asc("age")).collect()
[Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
>>> df.orderBy(desc("age"), "name").collect()
[Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
>>> df.orderBy(["age", "name"], ascending=[0, 1]).collect()
[Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]

'''
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
'''
pyspark.sql.functions.stddev(col)¶
Aggregate function: returns the unbiased sample standard deviation of the expression in a group.

New in version 1.6.

pyspark.sql.functions.stddev_pop(col)
Aggregate function: returns population standard deviation of the expression in a group.

New in version 1.6.

pyspark.sql.functions.stddev_samp(col)
Aggregate function: returns the unbiased sample standard deviation of the expression in a group.
'''
def StandardDev(df,col): # returns the unbiased sample standard deviation of the expression in a group.
	return(df.agg(func.stddev(df[col])).collect()[0][0]) 	

def StandardDev_pop(df,col): # returns population standard deviation of the expression in a group.
	return(df.agg(func.stddev_pop(df[col])).collect()[0][0])

def StandardDev_sample(df,col): # returns the unbiased sample standard deviation of the expression in a group.Almost same as StandardDev.
	return(df.agg(func.stddev_samp(df[col])).collect()[0][0])

#5. Variance 
'''
pyspark.sql.functions.var_pop(col)
Aggregate function: returns the population variance of the values in a group.

New in version 1.6.

pyspark.sql.functions.var_samp(col)
Aggregate function: returns the unbiased variance of the values in a group.

New in version 1.6.

pyspark.sql.functions.variance(col)
Aggregate function: returns the population variance of the values in a group.

'''
def Variance(df,col):
	return(df.agg(func.variance(df[col])).collect()[0][0])

def VariancePop(df,col):
	return((df.agg(func.var_pop(df[col])).collect()[0][0]))

def VarianceSamp(df,col):
	return((df.agg(func.var_samp(df[col])).collect()[0][0]))

#6. Correlation 
'''
pyspark.sql.functions.corr(col1, col2)[source]
Returns a new Column for the Pearson Correlation Coefficient for col1 and col2.

>>> a = [x * x - 2 * x + 3.5 for x in range(20)]
>>> b = range(20)
>>> corrDf = sqlContext.createDataFrame(zip(a, b))
>>> corrDf = corrDf.agg(corr(corrDf._1, corrDf._2).alias('c'))
>>> corrDf.selectExpr('abs(c - 0.9572339139475857) < 1e-16 as t').collect()
[Row(t=True)]


class pyspark.sql.DataFrameStatFunctions(df)
Functionality for statistic functions with DataFrame.

New in version 1.4.

corr(col1, col2, method=None)
Calculates the correlation of two columns of a DataFrame as a double value. Currently only supports the Pearson Correlation Coefficient. DataFrame.corr() and DataFrameStatFunctions.corr() are aliases of each other.

Parameters:	
col1 – The name of the first column
col2 – The name of the second column
method – The correlation method. Currently only supports “pearson”
'''
def Correlation(df,col1,col2,method=None): # Calculates the correlation of two columns of a DataFrame as a double value. Currently only supports the Pearson Correlation Coefficient.So method is specified as "None".
	return((df.agg(func.corr(df[col1],df[col2])).collect()[0][0])) 

#7. Covariance 
'''
class pyspark.sql.DataFrameStatFunctions(df)
Functionality for statistic functions with DataFrame.
cov(col1, col2)
Calculate the sample covariance for the given columns, specified by their names, as a double value. DataFrame.cov() and DataFrameStatFunctions.cov() are aliases.

Parameters:	
col1 – The name of the first column
col2 – The name of the second column

cov(col1, col2)
Calculate the sample covariance for the given columns, specified by their names, as a double value. DataFrame.cov() and DataFrameStatFunctions.cov() are aliases.

Parameters:	
col1 – The name of the first column
col2 – The name of the second column
'''
#Explore Later
def Covariance(df,col1,col2): # Calculate the sample covariance for the given columns, specified by their names, as a double value.
	#return((df.stat.cov(str(df[col1]),str(df[col2])).collect()[0][0])) 
	#return((df.agg(DataFrameStatFunctions.cov(df[col1],df[col2])).collect()[0][0]))
	return(df.cov(col1,col2))
 
#8. Standardize     
'''
Xnew = (X - Mean)/(Std.Deviation)
'''
'''
registerFunction(name, f, returnType=StringType)
Registers a python function (including lambda function) as a UDF so it can be used in SQL statements.

In addition to a name and the function itself, the return type can be optionally specified. When the return type is not given it default to a string and conversion will automatically be done. For any other return type, the produced object must match the specified type.

Parameters:	
name – name of the UDF
f – python function
returnType – a DataType object
>>> sqlContext.registerFunction("stringLengthString", lambda x: len(x))
>>> sqlContext.sql("SELECT stringLengthString('test')").collect()
[Row(_c0=u'4')]
>>> from pyspark.sql.types import IntegerType
>>> sqlContext.registerFunction("stringLengthInt", lambda x: len(x), IntegerType())
>>> sqlContext.sql("SELECT stringLengthInt('test')").collect()
[Row(_c0=4)]
>>> from pyspark.sql.types import IntegerType
>>> sqlContext.udf.register("stringLengthInt", lambda x: len(x), IntegerType())
>>> sqlContext.sql("SELECT stringLengthInt('test')").collect()
[Row(_c0=4)]

pyspark.sql.functions.udf(f, returnType=StringType)[source]
Creates a Column expression representing a user defined function (UDF).

>>> from pyspark.sql.types import IntegerType
>>> slen = udf(lambda s: len(s), IntegerType())
>>> df.select(slen(df.name).alias('slen')).collect()
[Row(slen=5), Row(slen=3)]

# We use UDF() from functions.udf() And .withColumn()
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
 
def uppercase(string):
    return string.upper()

udf_uppercase = udf(uppercase, StringType())
 
# Convert a whole column to uppercase with a UDF.Adding new Column with Name "ShipName_upper" using ".withColumn"
newDF = Orders_df.withColumn("ShipName_upper", udf_uppercase("ShipName"))

-----------------
registerFunction(name, f, returnType=StringType)
Registers a python function (including lambda function) as a UDF so it can be used in SQL statements.

In addition to a name and the function itself, the return type can be optionally specified. When the return type is not given it default to a string and conversion will automatically be done. For any other return type, the produced object must match the specified type.

Parameters:	
name – name of the UDF
f – python function
returnType – a pyspark.sql.types.DataType object


sqlContext.registerFunction("stringLengthInt", lambda x: len(x), IntegerType())
x = sqlContext.sql("SELECT stringLengthInt(Region) AS RegionLength  From Employees").show()
print(x)
print("\n\n")

'''
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf

'''
def convert(x,Mean,StdDev):
	return ((x-Mean)/(StdDev))

def Standardize(df,col,colnum):
	udf_convert = udf(convert, IntegerType())
	mean = Mean_col(df,colnum)
	stddev = StandardDev(df,colnum)
	newDF = df.withColumn(col+" Standardised", udf_convert(col,mean,stddev))
	return (newDF.show())
'''
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
	

	

	
        

       





