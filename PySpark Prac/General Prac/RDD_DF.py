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

##################################################### RDD Transformations on Data Sets #########################################################

'''
Data Set looks like This:

Year	First Name	County	Sex	Count
2012	DOMINIC	CAYUGA	M	6
2012	ADDISON	ONONDAGA	F	14
2012	ADDISON	ONONDAGA	F	14
2012	JULIA	ONONDAGA	F	15

'''
# 1.MAP()
baby_names = sc.textFile("/home/akhil/Desktop/baby_names.csv")

rows = baby_names.map(lambda line: line.split(","))

#Prints Names
for row in rows.take(rows.count()): 
	print(row[1])
 
# 2.flatMap(func)
'''

#“Similar to map, but each input item can be mapped to 0 or more output items (so func should return a Seq rather than a single item).”

#flatMap is helpful with nested datasets.  It may be beneficial to think of the RDD source as hierarchical JSON (which may have been converted to #case classes or nested collections).  This is unlike CSV which has no hierarchical structural.

Eg:
Spark flatMap Example Using Python

# Bazic map example in python
x = sc.parallelize(["spark rdd example", "sample example"], 2)

# map operation will return Array of Arrays in following case (check the result)
y = x.map(lambda x: x.split(' '))
y.collect()
[['spark', 'rdd', 'example'], ['sample', 'example']]

# flatMap operation will return Array of words in following case (check the result)
y = x.flatMap(lambda x: x.split(' '))
y.collect()
['spark', 'rdd', 'example', 'sample', 'example']
sc.parallelize([2, 3, 4]).flatMap(lambda x: [x,x,x]).collect()
[2, 2, 2, 3, 3, 3, 4, 4, 4]
 
sc.parallelize([1,2,3]).map(lambda x: [x,x,x]).collect()
[[1, 1, 1], [2, 2, 2], [3, 3, 3]]
'''

# 3.filter() ==> SELECT Names from Table where Table['Names'] == "MICHAEL" 

a = rows.filter(lambda line: "MICHAEL" in line).collect()
print a

# 4.mapPartitions(func, preservesPartitioning=False)

'''
Consider mapPartitions a tool for performance optimization if you have the resources available.  It won’t do much when running examples on your laptop.  It’s the same as “map”, but works with Spark RDD partitions which are distributed.  Remember the first D in RDD – Resilient Distributed Datasets.

In examples below that when using parallelize, elements of the collection are copied to form a distributed dataset that can be operated on in parallel.

A distributed dataset can be operated on in parallel.

One important parameter for parallel collections is the number of partitions to cut the dataset into. Spark will run one task for each partition of the cluster.
'''

'''

one_through_9 = range(1,10)
parallel = sc.parallelize(one_through_9, 3)
def f(iterator): yield sum(iterator)
parallel.mapPartitions(f).collect()
[6, 15, 24]
 
parallel = sc.parallelize(one_through_9)
parallel.mapPartitions(f).collect()
[1, 2, 3, 4, 5, 6, 7, 17]


Results [6,15,24] are created because mapPartitions loops through 3 partitions which is the second argument to the sc.parallelize call.

Partion 1: 1+2+3 = 6

Partition 2: 4+5+6 = 15

Partition 3: 7+8+9 = 24

The second example produces [1,2,3,4,5,6,7,17] which I’m guessing means the default number of partitions on my laptop is 8.

Partion 1 = 1

Partition 2= 2

Partion 3 = 3

Partition 4 = 4

Partion 5 = 5

Partition 6 = 6

Partion 7 = 7

Partition 8: 8+9 = 17

Typically you want 2-4 partitions for each CPU core in your cluster. Normally, Spark tries to set the number of partitions automatically based on your cluster or hardware based on standalone environment.

To find the default number of partitions and confirm the guess of 8 above:

print sc.defaultParallelism

'''
# 5.mapPartitionsWithIndex(func)
parallel = sc.parallelize(one_through_nine,3)
def show(index, iterator): 
	yield 'index: '+str(index)+" values: "+ str(list(iterator))
parallel.mapPartitionsWithIndex(show).collect()
'''
['index: 0 values: [1, 2, 3]',
 'index: 1 values: [4, 5, 6]',
 'index: 2 values: [7, 8, 9]']
'''
'''
When learning these APIs on an individual laptop or desktop, it might be helpful to show differences in capabilities and outputs.  For example, if we change the above example to use a parallelize’d list with 3 slices, our output changes significantly As Above.
'''

# 6.distinct([numTasks])

#Another simple one.  Return a new RDD with distinct elements within a source RDD
 
parallel = sc.parallelize(range(1,9))
par2 = sc.parallelize(range(5,15))
 
parallel.union(par2).distinct().collect()
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]

######################################################## The Keys ##############################################################################


#The group of transformation functions (groupByKey, reduceByKey, aggregateByKey, sortByKey, join) all act on key,value pair RDDs.

baby_names = sc.textFile("baby_names.csv")
rows = baby_names.map(lambda line: line.split(","))

# 7.groupByKey([numTasks])   ===> GROUPBY in SQL

#“When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs. ”

#The following groups all names to counties in which they appear over the years.

rows = baby_names.map(lambda line: line.split(","))
namesToCounties = rows.map(lambda n: (str(n[1]),str(n[2]) )).groupByKey()
namesToCounties.map(lambda x : {x[0]: list(x[1])}).collect()

'''
[{'GRIFFIN': ['ERIE',
   'ONONDAGA',
   'NEW YORK',
   'ERIE',
   'SUFFOLK',
   'MONROE',
   'NEW YORK',
...
'''

#8. reduceByKey(func, [numTasks]) ==> GroupBY AND SUM()

Operates on key, value pairs again, but the func must be of type (V,V) => V

Let’s sum the yearly name counts over the years in the CSV.  Notice we need to filter out the header row.  Also notice we are going to use the “Count” column value (n[4])

 
filtered_rows = baby_names.filter(lambda line: "Count" not in line).map(lambda line: line.split(","))
filtered_rows.map(lambda n:  (str(n[1]), int(n[4]) ) ).reduceByKey(lambda v1,v2: v1 + v2).collect()
 
'''
[('GRIFFIN', 268),
 ('KALEB', 172),
 ('JOHNNY', 219),
 ('SAGE', 5),
 ('MIKE', 40),
 ('NAYELI', 44),
....

'''
reduceByKey(func: (V, V) ⇒ V): RDD[(K, V)]

#9. aggregateByKey(zeroValue)(seqOp, combOp, [numTasks])

Ok, I admit, this one drives me a bit nuts.  Why wouldn’t we just use reduceByKey?  I don’t feel smart enough to know when to use aggregateByKey over reduceByKey.  For example, the same results may be produced as reduceByKey:

filtered_rows = baby_names.filter(lambda line: "Count" not in line).map(lambda line: line.split(","))
filtered_rows.map(lambda n:  (str(n[1]), int(n[4]) ) ).aggregateByKey(0, lambda k,v: int(v)+k, lambda v,k: k+v).collect()
 
'''
[('GRIFFIN', 268),
 ('KALEB', 172),
 ('JOHNNY', 219),
 ('SAGE', 5),
...
'''

#10. sortByKey(ascending=True, numPartitions=None, keyfunc=<function <lambda>>) ==> GROUPBY AND ORDERBY  INC/DEC

This simply sorts the (K,V) pair by K. 

filtered_rows.map (lambda n:  (str(n[1]), int(n[4]) ) ).sortByKey().collect()
'''
[('AADEN', 18),
 ('AADEN', 11),
 ('AADEN', 10),
 ('AALIYAH', 50),
 ('AALIYAH', 44),
...
'''
#opposite
filtered_rows.map (lambda n:  (str(n[1]), int(n[4]) ) ).sortByKey(False).collect()

'''
[('ZOIE', 5),
 ('ZOEY', 37),
 ('ZOEY', 32),
 ('ZOEY', 30),

'''

############################################################### JOIN ###########################################################################

join(otherDataset, [numTasks])
If you have relational database experience, this will be easy.  It’s joining of two datasets.  Other joins are available as well such as leftOuterJoin and rightOuterJoin.

names1 = sc.parallelize(("abe", "abby", "apple")).map(lambda a: (a, 1))
names2 = sc.parallelize(("apple", "beatty", "beatrice")).map(lambda a: (a, 1))
names1.join(names2).collect()
 
[('apple', (1, 1))]

names1.leftOuterJoin(names2).collect()
[('abe', (1, None)), ('apple', (1, 1)), ('abby', (1, None))]
 
names1.rightOuterJoin(names2).collect()
[('apple', (1, 1)), ('beatrice', (None, 1)), ('beatty', (None, 1))]
 
#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
#Source link:  https://www.supergloo.com/fieldnotes/apache-spark-transformations-python-examples/

#https://github.com/tmcgrath/spark-with-python-course/blob/master/Spark-Transformers-With-Spark.ipynb

#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#


#####################################################     Testing TimeStamp DataType()     #####################################################
#####################################################     Working with orders.csv          #####################################################

print("\n\n******************************************     Working with orders.csv          ***********************************************\n\n")

'''

#OrderID	CustomerID	EmployeeID	OrderDate	RequiredDate	ShippedDate	ShipVia	Freight	ShipName	ShipAddress	#ShipCity	ShipRegion	ShipPostalCode	ShipCountry

'''

# Imports all data Types
from pyspark.sql.types import *

OrdersFile = sc.textFile("/home/akhil/Desktop/3 Day Task/data/orders.csv")

header = OrdersFile.first()

schemaString = header.replace('"','')  # get rid of the double-quotes

#By Default We Set as StringType()
fields = [StructField(field_name, StringType(), False) for field_name in schemaString.split(',')]

print(fields)

print(len(fields) ) # how many elements in the header?


fields[0].dataType = LongType()
fields[2].dataType = IntegerType()
fields[3].dataType = DateType()
fields[4].dataType = DateType()
fields[6].dataType = IntegerType()
fields[7].dataType = FloatType()

print("\n\n")
print(fields)
print("\n\n")

# We can get rid of any annoying leading underscores or change name of field by 
# Eg: fields[0].name = 'id'

schema = StructType(fields)

print(schema)
print("\n\n")

OrdersHeader = OrdersFile.filter(lambda l: "OrderID" in l)
print(OrdersHeader.collect())
print("\n\n")

OrdersNoHeader = OrdersFile.subtract(OrdersHeader)
print(OrdersNoHeader.count())
print("\n\n")

from datetime import date, datetime

def convert(date_string):
	date_new = date_string.split(" ")[0]
	date_object = date(*map(int,(date_string.split(" ")[0].split("-"))))
	#assert date_object == datetime.strptime(date_new, "%Y/%m/%d").date()
	return date_object

#Remove Nullable = "False" Here
Orders_temp = OrdersNoHeader.map(lambda k: k.split(",")).map(lambda p:(int(p[0]),str(p[1].encode("utf-8")),int(p[2]),convert(p[3]),convert(p[4]),p[5],int(p[6]),float(p[7]), p[8],p[9],p[10],p[11],p[12],p[13]))

print(Orders_temp.top(2)) 
print("\n\n")

Orders_df = sqlContext.createDataFrame(Orders_temp, schema)

print("\n Dtypes : ",Orders_df.dtypes)
print("\n\n")

print("\n Schema:  ",Orders_df.printSchema())
print("\n\n")

print("\n\n ******************************************** OK WELL ************************************* \n\n")

from pyspark.sql.types import *
from pyspark.sql.functions import *

cached = Orders_df.persist()
# OR USE cacheTable() from SparkContext


#.............   Issue or Error Here. .....!!

#x = cached.count()
print "\n\n"

col = cached.columns

print("\n\n ******************************************** OK WELL NOW ************************************* \n\n")

print col
print "\n\n"

print cached.printSchema()

#Registering Table 
Orders_df.registerTempTable("Orders")

####################################################### Manually create DataFrames (Fact Table) ################################################

print("\n\n******************************************     Working with Fact Table          ***********************************************\n\n")


FactSchema = StructType([
                   StructField('ProductID',IntegerType(),nullable=False),StructField('EmployeeID',LongType(),nullable=False),StructField('CustomerID',StringType(),nullable=False),StructField('OrderDate',StringType(),nullable=False),StructField('ShipDate',StringType(),nullable=False),StructField('UnitPrice',FloatType(),nullable=False),StructField('Quantity',StringType(),nullable=False),StructField('Discount',FloatType(),nullable=False),StructField('ExtendedPrice',DoubleType(),nullable=False)])

FactData = sc.parallelize([
                           Row(1,1080,'ASDEF','1996-07-04 00:00:00.000','1996-07-04 00:00:00.000',34.56,'13 Books',3.56,89.56) ])

FactDF = sqlContext.createDataFrame(FactData,FactSchema)

print FactDF.printSchema()

################################################################# withColumn() #################################################################
'''
DataFrame provides a convenient method of form DataFrame.withColumn([string] columnName, [udf] userDefinedFunction) to append column to an existing DataFrame. Here the userDefinedFunction is of type pyspark.sql.functions.udf which is of the form udf(userMethod, returnType). The userMethod is the actual python method the user application implements and the returnType has to be one of the types defined in pyspark.sql.types, the user method can return. Here is an example python notebook that creates a DataFrame of rectangles.
'''
import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark.sql.functions import udf

sparkContext = SparkContext(conf = SparkConf())
hiveContext = HiveContext(sparkContext)

rectangleList = [('RectangleA',10,20,30),('RectangleB',40,50,60),('RectangleC',70,80,90)]

rectangleDataFrame = hiveContext.createDataFrame(rectangleList,['RectangleName','Height','Width','DegreeofRotation'])

'''
Let us suppose that the application needs to add the length of the diagonals of the rectangle as a new column in the DataFrame. Since the length of the diagonal can be represented as a float DataFrame.withColumn can be used with returnType as FloatType.
'''

#User defined function for calculating Diagnaol

from math import *

def calculateDiag(ht,wdt):
	
	return sqrt(ht*ht + wdt*wdt)

#Define UDF for calculating the length of the diagonal

diagonalCalculator = udf(calculateDiag,FloatType())

#Append th length of the diagonal as a new column to the original data frame

rectangleDataFrame = rectangleDataFrame.withColumn("Diagonal",diagonalCalculator(rectangleDataFrame['Height'],rectangleDataFrame['Width']))

print rectangleDataFrame.take(1)

#Advanced Way : https://blogs.msdn.microsoft.com/azuredatalake/2016/02/10/pyspark-appending-columns-to-dataframe-when-dataframe-withcolumn-cannot-be-used/


#################################################################################################################################################
##############################################     SQL To  Equavalent Transforms on DF     #####################################################

#####################################################          Quaring DataFrames     ##########################################################

'''

#OrderID	CustomerID	EmployeeID	OrderDate	RequiredDate	ShippedDate	ShipVia	Freight	ShipName	ShipAddress	#ShipCity	ShipRegion	ShipPostalCode	ShipCountry

# Use Orders_df DataFrame
'''

print("\n\n ******************************************************* SQL - SELECT ******************************************************** \n\n")
#1 . SQL - SELECT 

#select (only one column)
xyz = Orders_df.select('OrderID').take(2)
print xyz

#first
frst = Orders_df.first()
print frst

#selecting multiple columns
selected = Orders_df.select('OrderID','CustomerID','OrderDate')
selected.show()
 
#2 . SQL - SELECT BY NAME AND GROUPBY


#############################
#filter - WHERE Equavelent
#############################

#shipnames = Orders_df.filter(functions.col('OrderID') == 3) # Returns None 
#shipnames = Orders_df.filter(functions.col('EmployeeID') == '5')
#UnicodeEncodeError: 'ascii' codec can't encode character u'\xfa' in position 906: ordinal not in range(128)
#shipnames.show()
 

#Alias

Orders_df.select(functions.col('OrderDate').alias('Date')).show()

#######################
#grouping/counting
#######################

#val = Orders_df.filter(functions.col('OrderDate') == 1997-07-07).groupBy('CustomerID').count() # 

#val = Orders_df.filter(functions.col('OrderDate') == datetime.date(1996, 7, 9)).groupBy('CustomerID').count() 

#TypeError: descriptor 'date' requires a 'datetime.datetime' object but received a 'int'

#print val

######################
#where
######################

#ex_df = parquet.where(functions.col('CustomerID') > 4 ).toPandas()

#print ex_df



##############  User Defined Functions with DataFrames ###########

# We use UDF() from functions.udf()

#Use link : http://blog.brakmic.com/data-science-for-losers-part-5-spark-dataframes/

#UDFs are not unique to SparkSQL. You can also define them as named functions and insert them into a chain of operators without using SQL. The contrived example below shows how we would define and use a UDF directly in the code.

#Eg:
'''
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
 
# Define the UDF
def uppercase(string):
    return string.upper()
udf_uppercase = udf(uppercase, StringType())
 
# Convert a whole column to uppercase with a UDF.
newDF = oldDF.withColumn("name_upper", udf_uppercase("name"))
'''



################################################################################################################################################


The eBay online auction dataset has the following data fields:

@ auctionid - unique identifier of an auction
@ bid - the proxy bid placed by a bidder
@ bidtime - the time (in days) that the bid was placed, from the start of the auction
@ bidder - eBay username of the bidder
@ bidderrate - eBay feedback rating of the bidder
@ openbid - the opening bid set by the seller
@ price - the closing price that the item sold for (equivalent to the second highest bid + an increment)

Using Spark DataFrames we will explore the data with questions like:

> How many auctions were held?
> How many bids were made per item?
> What's the minimum, maximum, and average number of bids per item?
> Show the bids with price > 100


Using Spark DataFrames, we will explore the SFPD data with questions like:

> What are the top 10 Resolutions?
> How many Categories are there?
> What are the top 10 incident Categories?


#How many auctions were held?
auction.select("auctionid").distinct.count
#Long = 627

#How many bids per item?
auction.groupBy("auctionid", "item").count.show

#**********************************************************************************************************************************************#
auctionid  item    count
3016429446 palm    10
8211851222 xbox    28
3014480955 palm    12
8214279576 xbox    4
3014844871 palm    18
3014012355 palm    35
1641457876 cartier 2
#**********************************************************************************************************************************************#

#What's the min number of bids per item? what's the average? what's the max? 
auction.groupBy("item", "auctionid").count.agg(min("count"), avg("count"),max("count")).show

# MIN(count)          AVG(count)           MAX(count)
#     1          16.992025518341308            75

# Get the auctions with closing price > 100
highprice= auction.filter("price > 100")

highprice: org.apache.spark.sql.DataFrame = [auctionid: string, bid: float, bidtime: float, bidder: // string, bidderrate: int, openbid: float, price: float, item: string, daystolive: int]


#display dataframe in a tabular format
highprice.show()

# auctionid  bid   bidtime  bidder         bidderrate openbid price item daystolive
# 8213034705 95.0  2.927373 jake7870       0          95.0    117.5 xbox 3        
# 8213034705 115.0 2.943484 davidbresler2  1          95.0    117.5 xbox 3








#************************************************************************************************************************************************

#DataFrame DF :

'''
     word  tag count
0    a     S    30
1    the   S    20
2    a     T    60
3    an    T    5
4    the   T    10 
'''

#We want to find, for every "word", the "tag" that has the most "count". So the return would be something like

'''
Out:
     word  tag count
1    the   S    20
2    a     T    60
3    an    T    5
'''

# 1 ( Not Working)
# DF.groupby(['word']).agg(lambda x: x['tag'][ x['count'].argmax() ] )

# agg is the same as aggregate. It's callable is passed the columns (Series objects) of the DataFrame, one at a time.

# 2 (Real Solution)

def f(x):
	print type(x)
	print x

df.groupby('word').apply(f)

'''

OUT :-

<class 'pandas.core.frame.DataFrame'>
  word tag  count
0    a   S     30
2    a   T     60
<class 'pandas.core.frame.DataFrame'>
  word tag  count
0    a   S     30
2    a   T     60
<class 'pandas.core.frame.DataFrame'>
  word tag  count
3   an   T      5
<class 'pandas.core.frame.DataFrame'>
  word tag  count
1  the   S     20
4  the   T     10

'''

'''
your function just operates (in this case) on a sub-section of the frame with the grouped variable all having the same value (in this case 'word'), if you are passing a function, then you have to deal with the aggregation of potentially non-string columns; standard functions, like 'sum' do this for you.

Automatically does NOT aggregate on the string columns

'''

df.groupby('word').sum()

'''
word   count        
a        90
an        5
the      30

'''

#You ARE aggregating on all columns

df.groupby('word').apply(lambda x: x.sum())

''' 
        word tag count
word                  
a         aa  ST    90
an        an   T     5
the   thethe  ST    30

'''
# You can do pretty much anything within the function

df.groupby('word').apply(lambda x: x['count'].sum())

'''
word
a       90
an       5
the     30
'''

################################################################################################################################################

#Spark DataFrame groupBy and sort in the descending order (pyspark)

from pyspark.sql.functions import col

(group_by_dataframe
    .count()
    .filter("`count` >= 10")
    .sort(col("count").desc()))

#How could I order by sum, within a DataFrame in PySpark?

order_items.groupBy("order_item_order_id").sum("order_item_subtotal").orderBy(desc("SUM(order_item_subtotal#429)")).show()




