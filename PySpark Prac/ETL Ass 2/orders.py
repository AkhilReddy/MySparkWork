# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext, sql

from pyspark.sql import SQLContext

from pyspark.sql import DataFrame

from pyspark.sql import Column

from pyspark.sql import Row

from pyspark.sql import HiveContext

from pyspark.sql import GroupedData

from pyspark.sql import DataFrameNaFunctions

from pyspark.sql import DataFrameStatFunctions

from pyspark.sql import functions

from pyspark.sql import types

from pyspark.sql import Window

from pyspark.sql import SparkSession


conf = SparkConf().setMaster("local").setAppName("My App")

sc = SparkContext(conf = conf) #Initalising or Configuring "Spark Context"

sqlContext = SQLContext(sc)


######################################################## RDD Transformations on Data Sets ######################################################

###################################################         Working with orders.csv          ###################################################

print("\n\n******************************************     Working with orders.csv          ***********************************************\n\n")

'''
#OrderID   CustomerID	EmployeeID  OrderDate	RequiredDate   ShippedDate  ShipVia   Freight	ShipName	
'''

# Imports all data Types
from pyspark.sql.types import *

OrdersFile = sc.textFile("/home/akhil/Desktop/3 Day Task/orders.csv")

header = OrdersFile.first()

schemaString = header.replace('"','')  # get rid of the double-quotes

#By Default We Set as StringType()
fields = [StructField(field_name, StringType(), False) for field_name in schemaString.split(',')]

print(fields)
print("\n\n")

print(len(fields) ) # how many elements in the header?
print "\n\n"


fields[0].dataType = LongType()
fields[2].dataType = IntegerType()
fields[3].dataType = DateType()
fields[4].dataType = DateType()
fields[5].dataType = DateType()
fields[6].dataType = IntegerType()
fields[7].dataType = FloatType()

print("\n\n")
print(fields)
print("\n\n")

schema = StructType(fields)

print(schema)
print("\n\n")

OrdersHeader = OrdersFile.filter(lambda l: "OrderID" in l)
print(OrdersHeader.collect())
print("\n\n")

OrdersNoHeader = OrdersFile.subtract(OrdersHeader)
print(OrdersNoHeader.count())
print("\n\n")

print(OrdersNoHeader.collect())
print("\n\n")

from datetime import date, datetime

def convert(date_string):
	date_new = date_string.split(" ")[0]
	date_object = date(*map(int,(date_string.split(" ")[0].split("-"))))
	return date_object

Orders_temp = OrdersNoHeader.map(lambda k: k.split(",")).map(lambda p:(int(p[0]),str(p[1].encode("utf-8")),int(p[2]),convert(p[3]),convert(p[4]),convert(p[5]),int(p[6]),float(p[7]),str(p[8].encode("utf-8"))))

print(Orders_temp.top(2)) 
print("\n\n")

Orders_df = sqlContext.createDataFrame(Orders_temp, schema)

print("Dtypes : ",Orders_df.dtypes)
print("\n\n")

print("Schema:  ",Orders_df.printSchema())
print("\n\n")

'''
from pyspark.sql.types import *
from pyspark.sql.functions import *

#Doing Cache for DF
cached = Orders_df.persist()
col = cached.columns

print col
print "\n\n"

print cached.printSchema()
print "\n\n"

#Registering Table 
Orders_df.registerTempTable("Orders")

#Apply SQL on Table
sqlContext.sql("SELECT * FROM Orders").show()
print("\n\n")

sqlContext.sql("SELECT ShippedDate FROM Orders").show()
print("\n\n")

#select (only one column)
xyz = Orders_df.select('OrderID').take(2)
print xyz

#first
frst = Orders_df.first()
print frst

#selecting multiple columns
selected = Orders_df.select('OrderID','CustomerID','OrderDate','ShipName')
selected.show()
 
#filter - WHERE Equavelent
shipnames = Orders_df.filter(functions.col('OrderID') == 3) # Returns None 
shipnames.show()
Orders_df.filter(Orders_df.EmployeeID > 3).count()
print "\n\n"

shipnames = Orders_df.filter(functions.col('EmployeeID') == '4')
shipnames.show()
print "\n\n"
 
#Alias
Orders_df.select(functions.col('OrderDate').alias('Date')).show()

#grouping/counting
val = Orders_df.filter(functions.col('OrderDate') == convert('1996-07-08 00:00:00.000')).groupBy('CustomerID').count().show() 
print val
print "\n\n"

#where
ex = Orders_df.where(functions.col('EmployeeID') > 3 ).show()
print ex

# We use UDF() from functions.udf() And .withColumn()
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
 
def uppercase(string):
    return string.upper()

udf_uppercase = udf(uppercase, StringType())
 
# Convert a whole column to uppercase with a UDF.Adding new Column with Name "ShipName_upper" using ".withColumn"
newDF = Orders_df.withColumn("ShipName_upper", udf_uppercase("ShipName"))
newDF.show()
print "\n\n"

'''
#OrderID   CustomerID	EmployeeID  OrderDate	RequiredDate   ShippedDate  ShipVia   Freight	ShipName	
'''

#Distinct
out = Orders_df.select("EmployeeID").distinct().count()
print out
print "\n\n"

#Multiple GroupBy() + Count() ==> How many Orders per EmployeeID
Orders_df.groupBy("ShipVia","EmployeeID").count().show()
Orders_df.groupBy("EmployeeID","ShipVia").count().show()

#OrderBy()
x = Orders_df.groupBy("EmployeeID").sum("OrderID").orderBy("sum(OrderID)").count()
print x
print "\n\n"

Orders_df.groupBy("EmployeeID").sum("OrderID").orderBy("sum(OrderID)").alias("OrderSum").show() # alias() don't work Here

#What's the min number of bids per item? what's the average? what's the max? 
#Orders_df.groupBy("OrderID", "EmployeeID").agg(min("ShipVia"), avg("ShipVia"),max("ShipVia")).show
#Orders_df.groupBy("OrderID", "EmployeeID").count.agg(min("ShipVia"), avg("ShipVia"),max("ShipVia")).show
#Orders_df.groupBy("OrderID", "EmployeeID").agg(min("ShipVia"), max("ShipVia")).show
Orders_df.groupBy("OrderID", "EmployeeID").agg({"ShipVia":"min","ShipVia":"max","ShipVia":"avg"}).show()

#Sum() ==> Do sum of all aggregateable values
Orders_df.groupby('EmployeeID').sum().show()
#Orders_df.groupby('EmployeeID').apply(lambda x: x.sum())

#Substract ==> Remove a Row() OR Set of Rows from Other DF
#New_DF = Orders_df_Test.select('EmployeeID').subtract(Orders_df_Train.select('EmployeeID'))

#crosstab ==> DataFrame to calculate the pair wise frequency of columns.
Orders_df.crosstab('EmployeeID', 'ShipVia').show()

# You can do pretty much anything within the function
# df.groupby('word').apply(lambda x: x['count'].sum())


#sort()
#groupBy and sort in the descending order (pyspark)
from pyspark.sql.functions import col
Orders_df.filter(functions.col('EmployeeID') > 3).sort(col("EmployeeID").desc()).show()


from pyspark.sql.functions import *
#order by sum
Orders_df.groupBy("EmployeeID").sum("ShipVia").orderBy(desc("sum(ShipVia)")).show()

from pyspark.sql.functions import *
#dropDuplicates()
Orders_df.select('EmployeeID','ShipVia').dropDuplicates().show()
Orders_df.select('EmployeeID').dropDuplicates().show()
Orders_df.select('EmployeeID').dropDuplicates().sort(col("EmployeeID").desc()).show()


#dropna() ==> drop the all rows with null value
x = Orders_df.dropna().count()
print x
print "\n\n"

Orders_df.dropna().show()

#fillna  ==> fill the null values in DataFrame with constant number
Orders_df.fillna(-1).show(5)

#mean 
Orders_df.groupby('EmployeeID').agg({'ShipVia': 'mean'}).show()

#sample ==> sample DataFrame from the base DataFrame
#Arg: 
#withReplacement = True or False to select a observation with or without replacement.
#fraction = x, where x = .5 shows that we want to have 50% data in sample DataFrame.
#seed for reproduce the result

t1 = Orders_df.sample(False, 0.6, 42)
t2 = Orders_df.sample(False, 0.4, 43)
print (t1.count(),t2.count())
print "\n\n"


#apply map operation on DataFrame columns
#We can apply a function on each row of DataFrame using map operation. After applying this function, we get the result in the form of RDD. 
x = Orders_df.select('EmployeeID').rdd.map(lambda x:(x,1)).take(5)
print x
print "\n\n"

#sort the DataFrame based on column(s)
#orderBy operation on DataFrame to get sorted output based on some column. The orderBy operation take two arguments.
#>List of columns.
#>ascending = True or False for getting the results in ascending or descending order(list in case of more than two columns )

Orders_df.orderBy(Orders_df.EmployeeID.desc()).show(5)

#add the new column in DataFrame
#The withColumn operation will take 2 parameters.
#>Column name which we want add /replace.
#>Expression on column.

Orders_df.withColumn('OrderID_new', Orders_df.OrderID /2.0).select('OrderID','OrderID_new').show(5)

#drop a column in DataFrame
Orders_df.drop('ShipName').columns()

# remove some categories of Product_ID column in test that are not present in Product_ID column in train

#Refer link: https://www.analyticsvidhya.com/blog/2016/10/spark-dataframe-and-operations/ 


from pyspark.sql.functions import *

from pyspark.sql import functions as F

Orders_df.groupBy('EmployeeID').agg(F.count(Orders_df.ShipVia).alias("Via_Count"), F.avg(Orders_df.ShipVia).alias("Via_Average")).show()

#movie_names_df = info.join(movies_df, info.movieId == movies_df.ID, "inner").select(movies_df.title, info.average, info.movieId, info.count).show()

#groupby + agg + orderBy
import pyspark.sql.functions as func
Orders_df.groupBy("EmployeeID").agg(func.sum("ShipVia").alias("ShipSum")).orderBy("EmployeeID").show()


#Using agg with exprs
#Orders_df.groupBy("EmployeeID").select('EmployeeID','ShipVia').agg(exprs).show()
New_DF = Orders_df.select('EmployeeID','ShipVia')
print New_DF
exprs = {x: "sum" for x in New_DF.columns} # It's way of creating Dictionary
#We Cannot Use show with groupBy()
#New_DF.groupBy("EmployeeID").count().show()
New_DF.groupBy("EmployeeID").agg(exprs).show()
from pyspark.sql.functions import min
exprs2 = [min(x) for x in New_DF.columns]
New_DF.groupBy("EmployeeID").agg(*exprs2).show()

'''
#Joins
#############################################         Working with orderdetails.csv          ###################################################

print("\n\n**************************************         Working with orderdetails.csv          *****************************************\n\n")

'''
OrderID	   ProductID	UnitPrice    Quantity	Discount
'''


OrderDetailsFile = sc.textFile("/home/akhil/Desktop/3 Day Task/orderdetails.csv")

header = OrderDetailsFile.first()

schemaString = header.replace('"','')  # get rid of the double-quotes

#By Default We Set as StringType()
field = [StructField(field_name, StringType(), False) for field_name in schemaString.split(',')]

print(field)
print("\n\n")

print(len(field)) # how many elements in the header?
print "\n\n"

field[0].dataType = LongType()
field[1].dataType = IntegerType()
field[2].dataType = FloatType()
field[3].dataType = IntegerType()
field[4].dataType = FloatType()

print("\n\n")
print(field)
print("\n\n")

schema = StructType(field)

print(schema)
print("\n\n")

OrderDetailsHeader = OrderDetailsFile.filter(lambda l: "OrderID" in l)
print(OrderDetailsHeader.collect())
print("\n\n")

OrderDetailsNoHeader = OrderDetailsFile.subtract(OrderDetailsHeader)
print(OrderDetailsNoHeader.count())
print("\n\n")

print(OrderDetailsNoHeader.collect())
print("\n\n")

OrderDetails_temp = OrderDetailsNoHeader.map(lambda k: k.split(",")).map(lambda p:(int(p[0]),int(p[1]),float(p[2]),int(p[3]),float(p[4])))

print(OrderDetails_temp.top(2)) 
print("\n\n")

OrderDetails_df = sqlContext.createDataFrame(OrderDetails_temp, schema)

print("Dtypes : ",OrderDetails_df.dtypes)
print("\n\n")

print("Schema:  ",OrderDetails_df.printSchema())
print("\n\n")

'''
#Join 
Orders_df.filter(Orders_df.EmployeeID > 3).join(OrderDetails_df, Orders_df.OrderID == OrderDetails_df.OrderID).show()

#pyspark.sql.utils.AnalysisException: u'Cartesian joins could be prohibitively expensive and are disabled by default. To explicitly enable them, please set spark.sql.crossJoin.enabled = true;' while doing "OrderDetails_df.OrderID == OrderDetails_df.OrderID"
'''
'''
#Join and groupBy
Orders_df.filter(Orders_df.EmployeeID > 3).join(OrderDetails_df, Orders_df.OrderID == OrderDetails_df.OrderID).groupBy(Orders_df.EmployeeID,"Discount").agg({"UnitPrice": "avg", "Quantity": "max"}).show()

import pyspark.sql.functions as func

# Select distict('OrderID'),Sum('ShipVia') As ShipSum From Orders_df groupby 'EmployeeID' orderby 'EmployeeID' Desc  

a1 = Orders_df.select("OrderID","EmployeeID","ShipVia")
print a1 # It's a DataFrame => DataFrame[OrderID: bigint, EmployeeID: int, ShipVia: int]
print "\n\n"
a1.show()

a2 = a1.groupBy("EmployeeID")
print a2 # <pyspark.sql.group.GroupedData object at 0x7ffb40787cd0> (GroupedData Object)
print "\n\n"
#a2.show() # AttributeError: 'GroupedData' object has no attribute 'show'

a3 = a2.agg(func.sum("ShipVia").alias("ShipSum"))
print a3 # It's a DataFrame => DataFrame[EmployeeID: int, ShipSum: bigint]
print "\n\n"
a3.show()
'''
# It won't work as orderBy() will be on Resulted DataFrame
'''
a4 = a3.orderBy("OrderID")
print a4
print "\n\n"
a4.show()
'''
'''
a4 = a3.orderBy("EmployeeID")
print a4 # It's a DataFrame => DataFrame[EmployeeID: int, ShipSum: bigint]
print "\n\n"
a4.show()

from pyspark.sql.functions import * #for col()
a5 = a4.sort(col("ShipSum").desc())
print a5 # It's a DataFrame => DataFrame[EmployeeID: int, ShipSum: bigint]
print "\n\n"
a5.show()

#TypeError: distinct() takes exactly 1 argument (2 given)
#a6 = a5.distinct("ShipSum") 
a6 = a5.distinct() 
print a6 # It's a DataFrame => DataFrame[EmployeeID: int, ShipSum: bigint]
print "\n\n"
a6.show()
'''
#Complex Join + select
Orders_df.join(OrderDetails_df, Orders_df.OrderID == OrderDetails_df.OrderID).select(Orders_df.OrderID,'CustomerID','EmployeeID','OrderDate','ShipName','ProductID').show()


