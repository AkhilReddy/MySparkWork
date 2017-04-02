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

################################################       End of Working with products.csv      ###################################################
print("\n\n*************************************           Working with products.csv          ********************************************\n\n")

'''
ProductID	ProductName	SupplierID	CategoryID	QuantityPerUnit	UnitPrice	UnitsInStock	UnitsOnOrder	ReorderLevel	Discontinued
'''

from pyspark.sql.types import *

ProductsFile = sc.textFile("/home/akhil/Desktop/3 Day Task/data/products.csv")

header = ProductsFile.first()

schemaString = header.replace('"','')  # get rid of the double-quotes

#print(schemaString)

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(',')]

#print(fields)

#print(len(fields) ) # how many elements in the header?

fields[0].dataType = IntegerType()
fields[2].dataType = IntegerType()
fields[3].dataType = IntegerType()
fields[5].dataType = FloatType()
fields[6].dataType = IntegerType()
fields[7].dataType = IntegerType()
fields[8].dataType = IntegerType()
fields[9].dataType = IntegerType()

#print("\n\n")
#print(fields)
#print("\n\n")

# We can get rid of any annoying leading underscores or change name of field by 
# Eg: fields[0].name = 'id'

schema = StructType(fields)

#print(schema)
#print("\n\n")

ProductsHeader = ProductsFile.filter(lambda l: "ProductID" in l)
#print(ProductsHeader.collect())
#print("\n\n")

'''
Substract is used to remove Row from Table
'''
ProductsNoHeader = ProductsFile.subtract(ProductsHeader)
#print(ProductsNoHeader.count())
#print("\n\n")

Products_temp = ProductsNoHeader.map(lambda k: k.split(",")).map(lambda p: (int(p[0]),str(p[1].encode("utf-8")),int(p[2]),int(p[3]),str(p[4].encode("utf-8")),float(p[5]),int(p[6]),int(p[7]), int(p[8]),int(p[9])))
 
#print(Products_temp.top(2)) 
#print("\n\n")

Products_df = sqlContext.createDataFrame(Products_temp, schema)

#print(Products_df.head(8)) # look at the first 8 rows
#print("\n\n")

Products_df = ProductsNoHeader.map(lambda k: k.split(",")).map(lambda p: (int(p[0]),(p[1].strip('"')),int(p[2]),int(p[3]),(p[4].strip('"')),float(p[5]),int(p[6]),int(p[7]), int(p[8]),int(p[9]))).toDF(schema)

#print(Products_df.head(10))
#print("\n\n")

Q = Products_df.groupBy("SupplierID").count().show()

#print(Q)
print("\n\n")

#If we have missing values in the Discontinued. But how many are they?

Na = Products_df.filter(Products_df.Discontinued == '').count()

#print("\nMissing values in the Discontinued Field : ",Na)
#print("\n\n")

'''
dtypes and printSchema() methods can be used to get information about the schema, which can be useful further down in the data processing pipeline
'''
#print("\n Dtypes : ",Products_df.dtypes)
print("\n\n")

#print("\nSchema:  ",Products_df.printSchema())
print("\n\n")

#Registering Table 
Products_df.registerTempTable("Products")

#SQL Query
Q1 = sqlContext.sql("SELECT SupplierID, COUNT(*) AS COUNT FROM Products GROUP BY SupplierID ").show()

#print(Q1)
print("\n\n")

''' UnicodeEncodeError: 'ascii' codec can't encode character u'\xf4' in position 847: ordinal not in range(128)
sqlContext.sql("SELECT (*) FROM Products").show()
print("\n\n")
'''

'''
Imagine that at this point we want to change some column names: say, we want to shorten ProductID to ID, and similarly for the other 3 columns with lat/long information; we certainly do not want to run all the above procedure from the beginning – or even we might not have access to the initial CSV data, but only to the dataframe. We can do that using the dataframe method withColumnRenamed, chained as many times as required.

'''

Products_df = Products_df.withColumnRenamed('ProductID', 'ID').withColumnRenamed('SupplierID', 'SupID')

#print(Products_df.dtypes)
print("\n\n")

#Apply Filter On DF
X = Products_df.filter(Products_df.CategoryID == 2).count()

#print(X)
print("\n\n")

sqlContext.registerDataFrameAsTable(Products_df, "ProductsTable")

result = sqlContext.sql("SELECT * from ProductsTable")

#print(result)
print("\n\n")



################################################       End of Working with products.csv   ######################################################
##
##
#####################################################      Working with employees.csv   ########################################################
print("\n\n******************************************      Working with employees.csv   **************************************************\n\n")

'''
EmployeeID	LastName	FirstName	Title	TitleOfCourtesy	BirthDate	HireDate	Address	City	Region	PostalCode	Country	HomePhone	Extension	Photo	Notes	ReportsTo	PhotoPath

'''
# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("My Bap") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(EmployeeID = (fields[0]), LastName = str(fields[1].encode("utf-8")),FirstName = str(fields[2].encode("utf-8")), Title = str(fields[3].encode("utf-8")), HireDate = str(fields[6].encode("utf-8")), Region = str(fields[9].encode("utf-8")))

lines = spark.sparkContext.textFile("/home/akhil/Desktop/3 Day Task/data/employees.csv")

header = lines.first()

lines = lines.filter(lambda line:line != header)
df = lines.map(mapper)
'''
SparkSession.createDataFrame(data, schema=None, samplingRatio=None, verifySchema=True)
Creates a DataFrame from an RDD, a list or a pandas.DataFrame.

When schema is a list of column names, the type of each column will be inferred from data.

When schema is None, it will try to infer the schema (column names and types) from data, which should be an RDD of Row, or namedtuple, or dict.

When schema is pyspark.sql.types.DataType or a datatype string, it must match the real data, or an exception will be thrown at runtime. If the given schema is not pyspark.sql.types.StructType, it will be wrapped into a pyspark.sql.types.StructType as its only field, and the field name will be “value”, each record will also be wrapped into a tuple, which can be converted to row later.

If schema inference is needed, samplingRatio is used to determined the ratio of rows used for schema inference. The first row will be used if samplingRatio is None.

Parameters:	
data – an RDD of any kind of SQL data representation(e.g. row, tuple, int, boolean, etc.), or list, or pandas.DataFrame.
schema – a pyspark.sql.types.DataType or a datatype string or a list of column names, default is None. The data type string format equals to pyspark.sql.types.DataType.simpleString, except that top level struct type can omit the struct<> and atomic types use typeName() as their format, e.g. use byte instead of tinyint for pyspark.sql.types.ByteType. We can also use int as a short name for IntegerType.
samplingRatio – the sample ratio of rows used for inferring
verifySchema – verify data types of every row against schema.
Returns:	
DataFrame
'''

'''
createDataFrame(data, schema=None, samplingRatio=None, verifySchema=True)
Creates a DataFrame from an RDD, a list or a pandas.DataFrame.

When schema is a list of column names, the type of each column will be inferred from data.

When schema is None, it will try to infer the schema (column names and types) from data, which should be an RDD of Row, or namedtuple, or dict.

When schema is pyspark.sql.types.DataType or a datatype string it must match the real data, or an exception will be thrown at runtime. If the given schema is not pyspark.sql.types.StructType, it will be wrapped into a pyspark.sql.types.StructType as its only field, and the field name will be “value”, each record will also be wrapped into a tuple, which can be converted to row later.

If schema inference is needed, samplingRatio is used to determined the ratio of rows used for schema inference. The first row will be used if samplingRatio is None.

Parameters:	
data – an RDD of any kind of SQL data representation(e.g. Row, tuple, int, boolean, etc.), or list, or pandas.DataFrame.
schema – a pyspark.sql.types.DataType or a datatype string or a list of column names, default is None. The data type string format equals to pyspark.sql.types.DataType.simpleString, except that top level struct type can omit the struct<> and atomic types use typeName() as their format, e.g. use byte instead of tinyint for pyspark.sql.types.ByteType. We can also use int as a short name for pyspark.sql.types.IntegerType.
samplingRatio – the sample ratio of rows used for inferring
verifySchema – verify data types of every row against schema.
Returns:	
DataFrame

'''
# Infer the schema, and register the DataFrame as a table.
EmployeesDf = spark.createDataFrame(df).cache()
EmployeesDf.createOrReplaceTempView("Employees")

print(EmployeesDf.select('EmployeeID').show(5))


EmployeesDf2 = spark.sql("SELECT EmployeeID AS ID,FirstName  as Name from Employees").show()
print(EmployeesDf2)
print("\n\n")

#Use show() result as Schema but returns None Type obeject..Cannot use collect() 
Q3 = sqlContext.sql("SELECT EmployeeID AS ID,FirstName  as Name from Employees").show()
print(Q3)
print("\n\n")

from pyspark.sql.types import IntegerType

'''
registerFunction(name, f, returnType=StringType)
Registers a python function (including lambda function) as a UDF so it can be used in SQL statements.

In addition to a name and the function itself, the return type can be optionally specified. When the return type is not given it default to a string and conversion will automatically be done. For any other return type, the produced object must match the specified type.

Parameters:	
name – name of the UDF
f – python function
returnType – a pyspark.sql.types.DataType object

'''

sqlContext.registerFunction("stringLengthInt", lambda x: len(x), IntegerType())
x = sqlContext.sql("SELECT stringLengthInt(Region) AS RegionLength  From Employees").show()
print(x)
print("\n\n")

'''
sqlContext.sql() returns DataFrame

sql(sqlQuery)
Returns a DataFrame representing the result of the given query.

Returns:	DataFrame

'''

sqlContext.registerDataFrameAsTable(EmployeesDf, "EmpTable")
'''
registerDataFrameAsTable(df, tableName)
Registers the given DataFrame as a temporary table in the catalog.

Temporary tables exist only during the lifetime of this instance of SQLContext.

dropTempTable(tableName)
Remove the temp table from catalog.

>>> sqlContext.registerDataFrameAsTable(df, "table1")
>>> sqlContext.dropTempTable("table1")

'''
df2 = sqlContext.table("EmpTable")
print(sorted(df.collect()) == sorted(df2.collect()))
print("\n\n")

sqlContext.sql("SELECT * FROM EmpTable").show()
print("\n\n")

############   Applying RRD Transforms on Data Frames

#people.filter(people.age > 30).join(department, people.deptId == department.id) \
#  .groupBy(department.name, "gender").agg({"salary": "avg", "age": "max"})

'''
class pyspark.sql.DataFrame(jdf, sql_ctx)
A distributed collection of data grouped into named columns.

A DataFrame is equivalent to a relational table in Spark SQL, and can be created using various functions in SQLContext:

people = sqlContext.read.parquet("...")
Once created, it can be manipulated using the various domain-specific-language (DSL) functions defined in: DataFrame, Column.

To select a column from the data frame, use the apply method:

ageCol = people.age
A more concrete example:

# To create DataFrame using SQLContext
people = sqlContext.read.parquet("...")
department = sqlContext.read.parquet("...")

people.filter(people.age > 30).join(department, people.deptId == department.id) \
  .groupBy(department.name, "gender").agg({"salary": "avg", "age": "max"})
'''

O = df2.agg({"EmployeeID": "max"}).collect()
print(O)

from pyspark.sql import functions as F

U = df2.agg(F.min(df2.EmployeeID)).collect()
print(U)

'''
agg(*exprs)
Aggregate on the entire DataFrame without groups (shorthand for df.groupBy.agg()).
'''

'''
alias(alias)
Returns a new DataFrame with an alias set.
'''
from pyspark.sql.functions import *

df_as1 = df2.alias("df_as1")
df_as2 = df2.alias("df_as2")

joined_df = df_as1.join(df_as2, col("df_as1.EmployeeID") == col("df_as2.EmployeeID"), 'inner')
Al = joined_df.select("df_as1.EmployeeID", "df_as2.EmployeeID", "df_as2.HireDate").collect()

print(Al)

#cache()
''' 
cache()
Persists the DataFrame with the default storage level (MEMORY_AND_DISK).

Note The default storage level has changed to MEMORY_AND_DISK to match Scala in 2.0.
'''
#checkpoint(eager=True)
'''
checkpoint(eager=True)
Returns a checkpointed version of this Dataset. Checkpointing can be used to truncate the logical plan of this DataFrame, which is especially useful in iterative algorithms where the plan may grow exponentially. It will be saved to files inside the checkpoint directory set with SparkContext.setCheckpointDir().

Parameters:	eager – Whether to checkpoint this DataFrame immediately
'''
#coalesce(numPartitions)
'''
coalesce(numPartitions)
Returns a new DataFrame that has exactly numPartitions partitions.

Similar to coalesce defined on an RDD, this operation results in a narrow dependency, e.g. if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of the 100 new partitions will claim 10 of the current partitions.

>>> df.coalesce(1).rdd.getNumPartitions()
1
'''
#collect()
'''
collect()
Returns all the records as a list of Row.

>>> df.collect()
[Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
'''
#columns
'''
columns
Returns all column names as a list.

>>> df.columns
['age', 'name']

'''
###############################################################################################################################################
# Statistics Functions on DF
#corr(col1, col2, method=None)
#cov(col1, col2)
'''
corr(col1, col2, method=None)
Calculates the correlation of two columns of a DataFrame as a double value. Currently only supports the Pearson Correlation Coefficient. DataFrame.corr() and DataFrameStatFunctions.corr() are aliases of each other.

Parameters:	
col1 – The name of the first column
col2 – The name of the second column
method – The correlation method. Currently only supports “pearson”

cov(col1, col2)
Calculate the sample covariance for the given columns, specified by their names, as a double value. DataFrame.cov() and DataFrameStatFunctions.cov() are aliases.

Parameters:	
col1 – The name of the first column
col2 – The name of the second column

'''
# Describe or Summary
'''
describe(*cols)
Computes statistics for numeric and string columns.

This include count, mean, stddev, min, and max. If no columns are given, this function computes statistics for all numerical or string columns.

Note This function is meant for exploratory data analysis, as we make no guarantee about the backward compatibility of the schema of the resulting DataFrame.
>>> df.describe(['age']).show()
+-------+------------------+
|summary|               age|
+-------+------------------+
|  count|                 2|
|   mean|               3.5|
| stddev|2.1213203435596424|
|    min|                 2|
|    max|                 5|
+-------+------------------+
>>> df.describe().show()
+-------+------------------+-----+
|summary|               age| name|
+-------+------------------+-----+
|  count|                 2|    2|
|   mean|               3.5| null|
| stddev|2.1213203435596424| null|
|    min|                 2|Alice|
|    max|                 5|  Bob|
+-------+------------------+-----+
'''

#toPandas()
'''
toPandas()
Returns the contents of this DataFrame as Pandas pandas.DataFrame.

This is only available if Pandas is installed and available.

Note This method should only be used if the resulting Pandas’s DataFrame is expected to be small, as all the data is loaded into the driver’s memory.
>>> df.toPandas()  
   age   name
0    2  Alice
1    5    Bob
'''
###############################################################################################################################################
#count()
'''
count()
Returns the number of rows in this DataFrame.

>>> df.count()
2
'''

#Views on Tables
'''
createGlobalTempView(name)
Creates a global temporary view with this DataFrame.

The lifetime of this temporary view is tied to this Spark application. throws TempTableAlreadyExistsException, if the view name already exists in the catalog.

>>> df.createGlobalTempView("people")
>>> df2 = spark.sql("select * from global_temp.people")
>>> sorted(df.collect()) == sorted(df2.collect())
True
>>> df.createGlobalTempView("people")  
Traceback (most recent call last):
...
AnalysisException: u"Temporary table 'people' already exists;"
>>> spark.catalog.dropGlobalTempView("people")

--------------------------------------------------------------------------

createOrReplaceTempView(name)
Creates or replaces a local temporary view with this DataFrame.

The lifetime of this temporary table is tied to the SparkSession that was used to create this DataFrame.

>>> df.createOrReplaceTempView("people")
>>> df2 = df.filter(df.age > 3)
>>> df2.createOrReplaceTempView("people")
>>> df3 = spark.sql("select * from people")
>>> sorted(df3.collect()) == sorted(df2.collect())
True
>>> spark.catalog.dropTempView("people")
New in version 2.0.

createTempView(name)
Creates a local temporary view with this DataFrame.

The lifetime of this temporary table is tied to the SparkSession that was used to create this DataFrame. throws TempTableAlreadyExistsException, if the view name already exists in the catalog.

>>> df.createTempView("people")
>>> df2 = spark.sql("select * from people")
>>> sorted(df.collect()) == sorted(df2.collect())
True
>>> df.createTempView("people")  
Traceback (most recent call last):
...
AnalysisException: u"Temporary table 'people' already exists;"
>>> spark.catalog.dropTempView("people")
'''
#### CUBES ON DF's
'''
cube(*cols)
Create a multi-dimensional cube for the current DataFrame using the specified columns, so we can run aggregation on them.

>>> df.cube("name", df.age).count().orderBy("name", "age").show()
+-----+----+-----+
| name| age|count|
+-----+----+-----+
| null|null|    2|
| null|   2|    1|
| null|   5|    1|
|Alice|null|    1|
|Alice|   2|    1|
|  Bob|null|    1|
|  Bob|   5|    1|
+-----+----+-----+
'''

#distinct()
'''
distinct()
Returns a new DataFrame containing the distinct rows in this DataFrame.

>>> df.distinct().count()
2

'''

#drop(*cols)
'''
drop(*cols)
Returns a new DataFrame that drops the specified column. This is a no-op if schema doesn’t contain the given column name(s).

Parameters:	cols – a string name of the column to drop, or a Column to drop, or a list of string name of the columns to drop.
>>> df.drop('age').collect()
[Row(name=u'Alice'), Row(name=u'Bob')]
>>> df.drop(df.age).collect()
[Row(name=u'Alice'), Row(name=u'Bob')]
>>> df.join(df2, df.name == df2.name, 'inner').drop(df.name).collect()
[Row(age=5, height=85, name=u'Bob')]
>>> df.join(df2, df.name == df2.name, 'inner').drop(df2.name).collect()
[Row(age=5, name=u'Bob', height=85)]
>>> df.join(df2, 'name', 'inner').drop('age', 'height').collect()
[Row(name=u'Bob')]
'''

#dropDuplicates(subset=None)
'''
dropDuplicates(subset=None)
Return a new DataFrame with duplicate rows removed, optionally only considering certain columns.

drop_duplicates() is an alias for dropDuplicates().

>>> from pyspark.sql import Row
>>> df = sc.parallelize([ \
...     Row(name='Alice', age=5, height=80), \
...     Row(name='Alice', age=5, height=80), \
...     Row(name='Alice', age=10, height=80)]).toDF()
>>> df.dropDuplicates().show()
+---+------+-----+
|age|height| name|
+---+------+-----+
|  5|    80|Alice|
| 10|    80|Alice|
+---+------+-----+
>>> df.dropDuplicates(['name', 'height']).show()
+---+------+-----+
|age|height| name|
+---+------+-----+
|  5|    80|Alice|
+---+------+-----+
'''

#dropna(how='any', thresh=None, subset=None)
'''
dropna(how='any', thresh=None, subset=None)
Returns a new DataFrame omitting rows with null values. DataFrame.dropna() and DataFrameNaFunctions.drop() are aliases of each other.

Parameters:	
how – ‘any’ or ‘all’. If ‘any’, drop a row if it contains any nulls. If ‘all’, drop a row only if all its values are null.
thresh – int, default None If specified, drop rows that have less than thresh non-null values. This overwrites the how parameter.
subset – optional list of column names to consider.
>>> df4.na.drop().show()
+---+------+-----+
|age|height| name|
+---+------+-----+
| 10|    80|Alice|
+---+------+-----+
'''

#dtypes
'''
dtypes
Returns all column names and their data types as a list.

>>> df.dtypes
[('age', 'int'), ('name', 'string')]
'''
#explain(extended=False)
'''
explain(extended=False)
Prints the (logical and physical) plans to the console for debugging purpose.

Parameters:	extended – boolean, default False. If False, prints only the physical plan.
>>> df.explain()
== Physical Plan ==
Scan ExistingRDD[age#0,name#1]
'''
#fillna(value, subset=None)
'''
fillna(value, subset=None)
Replace null values, alias for na.fill(). DataFrame.fillna() and DataFrameNaFunctions.fill() are aliases of each other.

Parameters:	
value – int, long, float, string, or dict. Value to replace null values with. If the value is a dict, then subset is ignored and value must be a mapping from column name (string) to replacement value. The replacement value must be an int, long, float, or string.
subset – optional list of column names to consider. Columns specified in subset that do not have matching data type are ignored. For example, if value is a string, and subset contains a non-string column, then the non-string column is simply ignored.
>>> df4.na.fill(50).show()
+---+------+-----+
|age|height| name|
+---+------+-----+
| 10|    80|Alice|
|  5|    50|  Bob|
| 50|    50|  Tom|
| 50|    50| null|
+---+------+-----+
>>> df4.na.fill({'age': 50, 'name': 'unknown'}).show()
+---+------+-------+
|age|height|   name|
+---+------+-------+
| 10|    80|  Alice|
|  5|  null|    Bob|
| 50|  null|    Tom|
| 50|  null|unknown|
+---+------+-------+

'''

#filter(condition)
'''
filter(condition)
Filters rows using the given condition.

where() is an alias for filter().

Parameters:	condition – a Column of types.BooleanType or a string of SQL expression.
>>> df.filter(df.age > 3).collect()
[Row(age=5, name=u'Bob')]
>>> df.where(df.age == 2).collect()
[Row(age=2, name=u'Alice')]
>>> df.filter("age > 3").collect()
[Row(age=5, name=u'Bob')]
>>> df.where("age = 2").collect()
[Row(age=2, name=u'Alice')]
'''
#first()
'''
first()
Returns the first row as a Row.

>>> df.first()
Row(age=2, name=u'Alice')
'''

#foreach(f)
'''
foreach(f)
Applies the f function to all Row of this DataFrame.

This is a shorthand for df.rdd.foreach().

>>> def f(person):
...     print(person.name)
>>> df.foreach(f)
'''

#foreachPartition(f)
'''
foreachPartition(f)
Applies the f function to each partition of this DataFrame.

This a shorthand for df.rdd.foreachPartition().

>>> def f(people):
...     for person in people:
...         print(person.name)
>>> df.foreachPartition(f)
'''

#groupBy(*cols)
'''
groupBy(*cols)
Groups the DataFrame using the specified columns, so we can run aggregation on them. See GroupedData for all the available aggregate functions.

groupby() is an alias for groupBy().

Parameters:	cols – list of columns to group by. Each element should be a column name (string) or an expression (Column).
>>> df.groupBy().avg().collect()
[Row(avg(age)=3.5)]
>>> sorted(df.groupBy('name').agg({'age': 'mean'}).collect())
[Row(name=u'Alice', avg(age)=2.0), Row(name=u'Bob', avg(age)=5.0)]
>>> sorted(df.groupBy(df.name).avg().collect())
[Row(name=u'Alice', avg(age)=2.0), Row(name=u'Bob', avg(age)=5.0)]
>>> sorted(df.groupBy(['name', df.age]).count().collect())
[Row(name=u'Alice', age=2, count=1), Row(name=u'Bob', age=5, count=1)]
'''

#intersect(other)
'''
intersect(other)
Return a new DataFrame containing rows only in both this frame and another frame.

This is equivalent to INTERSECT in SQL.

'''

#orderBy(*cols, **kwargs)
'''
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

#printSchema()
'''
printSchema()
Prints out the schema in the tree format.

>>> df.printSchema()
root
 |-- age: integer (nullable = true)
 |-- name: string (nullable = true)

'''

#replace(to_replace, value, subset=None)
'''
replace(to_replace, value, subset=None)
Returns a new DataFrame replacing a value with another value. DataFrame.replace() and DataFrameNaFunctions.replace() are aliases of each other.

Parameters:	
to_replace – int, long, float, string, or list. Value to be replaced. If the value is a dict, then value is ignored and to_replace must be a mapping from column name (string) to replacement value. The value to be replaced must be an int, long, float, or string.
value – int, long, float, string, or list. Value to use to replace holes. The replacement value must be an int, long, float, or string. If value is a list or tuple, value should be of the same length with to_replace.
subset – optional list of column names to consider. Columns specified in subset that do not have matching data type are ignored. For example, if value is a string, and subset contains a non-string column, then the non-string column is simply ignored.
>>> df4.na.replace(10, 20).show()
+----+------+-----+
| age|height| name|
+----+------+-----+
|  20|    80|Alice|
|   5|  null|  Bob|
|null|  null|  Tom|
|null|  null| null|
+----+------+-----+
>>> df4.na.replace(['Alice', 'Bob'], ['A', 'B'], 'name').show()
+----+------+----+
| age|height|name|
+----+------+----+
|  10|    80|   A|
|   5|  null|   B|
|null|  null| Tom|
|null|  null|null|
+----+------+----+
'''
#schema
'''
schema
Returns the schema of this DataFrame as a pyspark.sql.types.StructType.

>>> df.schema
StructType(List(StructField(age,IntegerType,true),StructField(name,StringType,true)))

'''
#select(*cols)
'''
select(*cols)
Projects a set of expressions and returns a new DataFrame.

Parameters:	cols – list of column names (string) or expressions (Column). If one of the column names is ‘*’, that column is expanded to include all columns in the current DataFrame.
>>> df.select('*').collect()
[Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
>>> df.select('name', 'age').collect()
[Row(name=u'Alice', age=2), Row(name=u'Bob', age=5)]
>>> df.select(df.name, (df.age + 10).alias('age')).collect()
[Row(name=u'Alice', age=12), Row(name=u'Bob', age=15)]

'''

#show(n=20, truncate=True)
'''
show(n=20, truncate=True)
Prints the first n rows to the console.

Parameters:	
n – Number of rows to show.
truncate – If set to True, truncate strings longer than 20 chars by default. If set to a number greater than one, truncates long strings to length truncate and align cells right.
>>> df
DataFrame[age: int, name: string]
>>> df.show()
+---+-----+
|age| name|
+---+-----+
|  2|Alice|
|  5|  Bob|
+---+-----+
>>> df.show(truncate=3)
+---+----+
|age|name|
+---+----+
|  2| Ali|
|  5| Bob|
+---+----+
'''

######### Sort
'''
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


sortWithinPartitions(*cols, **kwargs)
Returns a new DataFrame with each partition sorted by the specified column(s).

Parameters:	
cols – list of Column or column names to sort by.
ascending – boolean or list of boolean (default True). Sort ascending vs. descending. Specify list for multiple sort orders. If a list is specified, length of the list must equal length of the cols.
>>> df.sortWithinPartitions("age", ascending=False).show()
+---+-----+
|age| name|
+---+-----+
|  2|Alice|
|  5|  Bob|
+---+-----+
'''

#EXCEPT -> SQL
#subtract(other)
'''
subtract(other)
Return a new DataFrame containing rows in this frame but not in another frame.

This is equivalent to EXCEPT in SQL.
'''
#toDF(*cols)
'''
toDF(*cols)
Returns a new class:DataFrame that with new specified column names

Parameters:	cols – list of new column names (string)
>>> df.toDF('f1', 'f2').collect()
[Row(f1=2, f2=u'Alice'), Row(f1=5, f2=u'Bob')]
'''

#toJSON(use_unicode=True)
'''
toJSON(use_unicode=True)
Converts a DataFrame into a RDD of string.

Each row is turned into a JSON document as one element in the returned RDD.

>>> df.toJSON().first()
u'{"age":2,"name":"Alice"}'

'''
#toLocalIterator()
'''
toLocalIterator()
Returns an iterator that contains all of the rows in this DataFrame. The iterator will consume as much memory as the largest partition in this DataFrame.

>>> list(df.toLocalIterator())
[Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
'''

#UNION ALL -> SQL
#union(other)
'''
union(other)
Return a new DataFrame containing union of rows in this frame and another frame.

This is equivalent to UNION ALL in SQL. To do a SQL-style set union (that does deduplication of elements), use this function followed by a distinct.



unionAll(other)
Return a new DataFrame containing union of rows in this frame and another frame.
'''

#where // filter() -> SQL
#where(condition)
'''
where(condition)
where() is an alias for filter().
'''

#withColumn(colName, col)
'''
withColumn(colName, col)
Returns a new DataFrame by adding a column or replacing the existing column that has the same name.

Parameters:	
colName – string, name of the new column.
col – a Column expression for the new column.
>>> df.withColumn('age2', df.age + 2).collect()
[Row(age=2, name=u'Alice', age2=4), Row(age=5, name=u'Bob', age2=7)]
'''

############################ A set of methods for aggregations on a DataFrame, created by DataFrame.groupBy(). #################################

'''
agg(*exprs)
Compute aggregates and returns the result as a DataFrame.

The available aggregate functions are avg, max, min, sum, count.

If exprs is a single dict mapping from string to string, then the key is the column to perform aggregation on, and the value is the aggregate function.

Alternatively, exprs can also be a list of aggregate Column expressions.

Parameters:	exprs – a dict mapping from column name (string) to aggregate functions (string), or a list of Column.
>>> gdf = df.groupBy(df.name)
>>> sorted(gdf.agg({"*": "count"}).collect())
[Row(name=u'Alice', count(1)=1), Row(name=u'Bob', count(1)=1)]
>>> from pyspark.sql import functions as F
>>> sorted(gdf.agg(F.min(df.age)).collect())
[Row(name=u'Alice', min(age)=2), Row(name=u'Bob', min(age)=5)]



avg(*cols)
Computes average values for each numeric columns for each group.

mean() is an alias for avg().

Parameters:	cols – list of column names (string). Non-numeric columns are ignored.
>>> df.groupBy().avg('age').collect()
[Row(avg(age)=3.5)]
>>> df3.groupBy().avg('age', 'height').collect()
[Row(avg(age)=3.5, avg(height)=82.5)]



max(*cols)
Computes the max value for each numeric columns for each group.

>>> df.groupBy().max('age').collect()
[Row(max(age)=5)]
>>> df3.groupBy().max('age', 'height').collect()
[Row(max(age)=5, max(height)=85)]



mean(*cols)
Computes average values for each numeric columns for each group.

mean() is an alias for avg().

Parameters:	cols – list of column names (string). Non-numeric columns are ignored.
>>> df.groupBy().mean('age').collect()
[Row(avg(age)=3.5)]
>>> df3.groupBy().mean('age', 'height').collect()
[Row(avg(age)=3.5, avg(height)=82.5)]

'''
################################################################################################################################################

####################################################  class pyspark.sql.Column(jc) A column in a DataFrame. ####################################

'''
between(lowerBound, upperBound)
A boolean expression that is evaluated to true if the value of this expression is between the given columns.

>>> df.select(df.name, df.age.between(2, 4)).show()
+-----+---------------------------+
| name|((age >= 2) AND (age <= 4))|
+-----+---------------------------+
|Alice|                       true|
|  Bob|                      false|
+-----+---------------------------+


cast(dataType)
Convert the column into type dataType.

>>> df.select(df.age.cast("string").alias('ages')).collect()
[Row(ages=u'2'), Row(ages=u'5')]
>>> df.select(df.age.cast(StringType()).alias('ages')).collect()
[Row(ages=u'2'), Row(ages=u'5')]


getItem(key)
An expression that gets an item at position ordinal out of a list, or gets an item by key out of a dict.

>>> df = sc.parallelize([([1, 2], {"key": "value"})]).toDF(["l", "d"])
>>> df.select(df.l.getItem(0), df.d.getItem("key")).show()
+----+------+
|l[0]|d[key]|
+----+------+
|   1| value|
+----+------+
>>> df.select(df.l[0], df.d["key"]).show()
+----+------+
|l[0]|d[key]|
+----+------+
|   1| value|
+----+------+


substr(startPos, length)
Return a Column which is a substring of the column.

Parameters:	
startPos – start position (int or Column)
length – length of the substring (int or Column)
>>> df.select(df.name.substr(1, 3).alias("col")).collect()
[Row(col=u'Ali'), Row(col=u'Bob')]


when(condition, value)
Evaluates a list of conditions and returns one of multiple possible result expressions. If Column.otherwise() is not invoked, None is returned for unmatched conditions.

See pyspark.sql.functions.when() for example usage.

Parameters:	
condition – a boolean Column expression.
value – a literal value, or a Column expression.
>>> from pyspark.sql import functions as F
>>> df.select(df.name, F.when(df.age > 4, 1).when(df.age < 3, -1).otherwise(0)).show()
+-----+------------------------------------------------------------+
| name|CASE WHEN (age > 4) THEN 1 WHEN (age < 3) THEN -1 ELSE 0 END|
+-----+------------------------------------------------------------+
|Alice|                                                          -1|
|  Bob|                                                           1|
+-----+------------------------------------------------------------+

'''
################################################################################################################################################

#####################################################  ROW  ####################################################################################

'''
Row
A row in DataFrame. The fields in it can be accessed:

like attributes (row.key)
like dictionary values (row[key])
key in row will search through row keys.

Row can be used to create a row object by using named arguments, the fields will be sorted by names.

>>> row = Row(name="Alice", age=11)
>>> row
Row(age=11, name='Alice')
>>> row['name'], row['age']
('Alice', 11)
>>> row.name, row.age
('Alice', 11)
>>> 'name' in row
True
>>> 'wrong_key' in row
False
'''
#####################################################       Writing Back              ##########################################################

'''
write
Interface for saving the content of the non-streaming DataFrame out into external storage.

Returns:	DataFrameWriter


writeStream
Interface for saving the content of the streaming DataFrame out into external storage.

'''

#####################################################            JOINS ON DF's            ######################################################

#join(other, on=None, how=None)
'''
join(other, on=None, how=None)
Joins with another DataFrame, using the given join expression.

Parameters:	
other – Right side of the join
on – a string for the join column name, a list of column names, a join expression (Column), or a list of Columns. If on is a string or a list of strings indicating the name of the join column(s), the column(s) must exist on both sides, and this performs an equi-join.
how – str, default ‘inner’. One of inner, outer, left_outer, right_outer, leftsemi.
The following performs a full outer join between df1 and df2.

>>> df.join(df2, df.name == df2.name, 'outer').select(df.name, df2.height).collect()
[Row(name=None, height=80), Row(name=u'Bob', height=85), Row(name=u'Alice', height=None)]
>>> df.join(df2, 'name', 'outer').select('name', 'height').collect()
[Row(name=u'Tom', height=80), Row(name=u'Bob', height=85), Row(name=u'Alice', height=None)]
>>> cond = [df.name == df3.name, df.age == df3.age]
>>> df.join(df3, cond, 'outer').select(df.name, df3.age).collect()
[Row(name=u'Alice', age=2), Row(name=u'Bob', age=5)]
>>> df.join(df2, 'name').select(df.name, df2.height).collect()
[Row(name=u'Bob', height=85)]
>>> df.join(df4, ['name', 'age']).select(df.name, df.age).collect()
[Row(name=u'Bob', age=5)]
'''

#crossJoin(other)
'''
crossJoin(other)
Returns the cartesian product with another DataFrame.

Parameters:	other – Right side of the cartesian product.
>>> df.select("age", "name").collect()
[Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
>>> df2.select("name", "height").collect()
[Row(name=u'Tom', height=80), Row(name=u'Bob', height=85)]
>>> df.crossJoin(df2.select("height")).select("age", "name", "height").collect()
[Row(age=2, name=u'Alice', height=80), Row(age=2, name=u'Alice', height=85),
 Row(age=5, name=u'Bob', height=80), Row(age=5, name=u'Bob', height=85)]
'''

#####################################################     Working with suppliers.csv    ########################################################
print("\n\n******************************************     Working with suppliers.csv    **************************************************\n\n")

from pyspark.sql.types import *

SuppliersFile = sc.textFile("/home/akhil/Desktop/3 Day Task/data/suppliers.csv")

header = SuppliersFile.first()

schemaString = header.replace('"','')  # get rid of the double-quotes

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(',')]

#print(fields)

#print(len(fields) ) # how many elements in the header?

'''
SupplierID	CompanyName	ContactName	ContactTitle	Address	 City	Region	PostalCode	Country	Phone	Fax	HomePage
'''

fields[0].dataType = IntegerType()
#fields[7].dataType = IntegerType()

#print("\n\n")
print(fields)
print("\n\n")

# We can get rid of any annoying leading underscores or change name of field by 
# Eg: fields[0].name = 'id'

schema = StructType(fields)

print(schema)
print("\n\n")

SuppliersHeader = SuppliersFile.filter(lambda l: "SupplierID" in l)
#print(SuppliersHeader.collect())
#print("\n\n")

SuppliersNoHeader = SuppliersFile.subtract(SuppliersHeader)
#print(SuppliersNoHeader.count())
#print("\n\n")

#Remove Nullable = "False" Here
Suppliers_temp = SuppliersNoHeader.map(lambda k: k.split(",")).map(lambda p:(int(p[0]),str(p[1].encode("utf-8")),p[2],p[3],p[4],p[5],p[6],p[7], p[8],p[9],p[10],p[11]))

#str(fields[1].encode("utf-8"))

print(Suppliers_temp.top(2)) 
print("\n\n")

Suppliers_df = sqlContext.createDataFrame(Suppliers_temp, schema)

print("\n Dtypes : ",Suppliers_df.dtypes)
print("\n\n")

print("\nSchema:  ",Suppliers_df.printSchema())
print("\n\n")

#Registering Table 
Suppliers_df.registerTempTable("Suppliers")

#####################################################     Working with categories.csv   ########################################################
print("\n\n******************************************     Working with categories.csv   **************************************************\n\n")

'''
CategoryID	CategoryName	Description	Picture
'''

from pyspark.sql.types import *

CategoriesFile = sc.textFile("/home/akhil/Desktop/3 Day Task/data/categories.csv")

header = CategoriesFile.first()

schemaString = header.replace('"','')  # get rid of the double-quotes

#print(schemaString)

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(',')]

#print(fields)

#print(len(fields) ) # how many elements in the header?

fields[0].dataType = IntegerType()

#print("\n\n")
#print(fields)
#print("\n\n")

# We can get rid of any annoying leading underscores or change name of field by 
# Eg: fields[0].name = 'id'

schema = StructType(fields)

print(schema)
print("\n\n")

CategoriesHeader = CategoriesFile.filter(lambda l: "CategoryID" in l)
#print(CategoriesHeader.collect())
#print("\n\n")

CategoriesNoHeader = CategoriesFile.subtract(CategoriesHeader)
#print(CategoriesNoHeader.count())
#print("\n\n")

Categories_temp = CategoriesNoHeader.map(lambda k: k.split(",")).map(lambda p: (int(p[0]),p[1],p[2],p[3]))
 
#print(Categories_temp.top(2)) 
#print("\n\n")

Categories_df = sqlContext.createDataFrame(Categories_temp, schema)

print("\n Dtypes : ",Categories_df.dtypes)
print("\n\n")

print("\nSchema:  ",Categories_df.printSchema())
print("\n\n")

#Registering Table 
Categories_df.registerTempTable("Categories")
print("\n\n")

#####################################################     Working with customers.csv       #####################################################
print("\n\n******************************************     Working with customers.csv       ***********************************************\n\n")

'''
CustomerID	CompanyName	ContactName	ContactTitle	Address	City	Region	PostalCode	Country	Phone	Fax
'''

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("My Cap") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(CustomerID = (fields[0]), CompanyName = str(fields[1].encode("utf-8")),ContactName = str(fields[2].encode("utf-8")), ContactTitle = str(fields[3].encode("utf-8")),Address = str(fields[4].encode("utf-8")),City = str(fields[5].encode("utf-8")),Region = str(fields[6].encode("utf-8")),PostalCode = str(fields[7].encode("utf-8")),Country = str(fields[8].encode("utf-8")), Phone = str(fields[9].encode("utf-8")),Fax = fields[10] )

lines = spark.sparkContext.textFile("/home/akhil/Desktop/3 Day Task/data/customers.csv")

header = lines.first()

lines = lines.filter(lambda line:line != header)
dfi = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
CustomersDf = spark.createDataFrame(dfi).cache()
CustomersDf.createOrReplaceTempView("Customers")

print(CustomersDf.select('CustomerID').show(5))


#CustomersDf2 = spark.sql("SELECT CustomerID AS ID,FirstName  as Name from Customers").show()
#print(CustomersDf2)
#print("\n\n")

#Use show() result as Schema but returns None Type obeject..Cannot use collect() 
#Q3 = sqlContext.sql("SELECT CustomerID AS ID,FirstName  as Name from Customers").show()
#print(Q3)
#print("\n\n")

sqlContext.registerDataFrameAsTable(CustomersDf, "CusTable")

'''
df2 = sqlContext.table("CusTable")
print(sorted(df.collect()) == sorted(df2.collect()))
print("\n\n")
'''

#####################################################     Working with orders.csv          #####################################################
print("\n\n******************************************     Working with orders.csv          ***********************************************\n\n")

'''
OrderID	CustomerID	EmployeeID	OrderDate	RequiredDate	ShippedDate	ShipVia	Freight	ShipName	ShipAddress	ShipCity	ShipRegion	ShipPostalCode	ShipCountry
'''

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("My Dap") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(OrderID = (fields[0]),CustomerID = str(fields[1].encode("utf-8")),EmployeeID = (fields[2]), OrderDate = str(fields[3].encode("utf-8")),RequiredDate = str(fields[4].encode("utf-8")), ShippedDate = str(fields[5].encode("utf-8")),ShipVia = fields[6] ,Freight = float(fields[7]),ShipName = str(fields[8].encode("utf-8")),ShipAddress = str(fields[9].encode("utf-8")), ShipCity = str(fields[10].encode("utf-8")),ShipRegion = str(fields[11].encode("utf-8")),ShipPostalCode = fields[12],ShipCountry = str(fields[13].encode("utf-8")))

lines = spark.sparkContext.textFile("/home/akhil/Desktop/3 Day Task/data/orders.csv")

header = lines.first()

lines = lines.filter(lambda line:line != header)
dfo = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
OrdersDf = spark.createDataFrame(dfo).cache()
OrdersDf.createOrReplaceTempView("Orders")

print(OrdersDf.select('OrderID').show(5))

sqlContext.registerDataFrameAsTable(OrdersDf, "OrdTable")


#####################################################     Working with order-details.csv    ####################################################
print("\n\n******************************************     Working with order-details.csv    **********************************************\n\n")

'''
OrderID	ProductID	UnitPrice	Quantity	Discount
'''

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("My Eap") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(OrderID = (fields[0]),ProductID = (fields[1]), UnitPrice = str(fields[2].encode("utf-8")),Quantity = str(fields[3].encode("utf-8")), Discount = fields[4] )

lines = spark.sparkContext.textFile("/home/akhil/Desktop/3 Day Task/data/order-details.csv")

header = lines.first()

lines = lines.filter(lambda line:line != header)
dfm = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
OrderDetailsDf = spark.createDataFrame(dfm).cache()
OrderDetailsDf.createOrReplaceTempView("OrderDetails")

print(OrderDetailsDf.select('OrderID').show(5))

sqlContext.registerDataFrameAsTable(OrderDetailsDf, "OrdDetTable")

################################################################################################################################################
#                                                                                                                                              #
#                             Working on Data Frames for Constructing Components of Resultant Star Schema                                       #                                                                                                                                              #
################################################################################################################################################

#######################################################     Product Table            ###########################################################

'''
ProductID	ProductName	SupplierID	CategoryID	QuantityPerUnit	UnitPrice	UnitsInStock	UnitsOnOrder	ReorderLevel	Discontinued
'''
sqlContext.sql("SELECT ID ,SupID,CategoryID,Discontinued FROM ProductsTable").show()
print("\n\n")

'''
SupplierID	CompanyName	ContactName	ContactTitle	Address	 City	Region	PostalCode	Country	Phone	Fax	HomePage
'''
sqlContext.sql("SELECT SupplierID FROM Suppliers").show()
print("\n\n")

'''
CategoryID	CategoryName	Description	Picture
'''
sqlContext.sql("SELECT * FROM Categories").show()
print("\n\n")

#NO SQL
#ProductsDF.select('ID' ,'SupID','CategoryID','Discontinued').dropDuplicates().show()

#INNER JOIN ON 3 Tables
'''
  SELECT column_Name1,column_name2,...... 
  From tbl_name1,tbl_name2,tbl_name3 
  where tbl_name1.column_name = tbl_name2.column_name 
  and tbl_name2.column_name = tbl_name3.column_name
'''

sqlContext.sql("SELECT p.ProductID, p.SupplierID, p.CategoryID, c.CategoryName,c.Description,p.Discontinued FROM Products p INNER JOIN Suppliers s on p.SupplierID = s.SupplierID INNER JOIN Categories c on p.CategoryID = c.CategoryID").show()
print("\n\n")

'''   Exact Solution
sqlContext.sql("SELECT p.ProductID,p.ProductName,p.SupplierID,s.CompanyName,p.CategoryID,c.CategoryName,c.Description,p.Discontinued FROM Products p INNER JOIN Suppliers s on p.SupplierID = s.SupplierID INNER JOIN Categories c on p.CategoryID = c.CategoryID").show()
print("\n\n")
'''
#NON SQL
Products_df.join(Suppliers_df, Products_df.SupplierID == Suppliers_df.SupplierID).select(Products_df.ProductID,Products_df.SupplierID,Products_df.CategoryID,Products_df.Discontinued).join(Categories_df, Products_df.CategoryID  == Categories_df.CategoryID).select(Products_df.ProductID,Products_df.SupplierID,Products_df.CategoryID,Products_df.Discontinued,Categories_df.CategoryName,Categories_df.Description).show()

#######################################################    Employee Table            ###########################################################

'''
EmployeeID	LastName	FirstName	Title	TitleOfCourtesy	BirthDate	HireDate	Address	City	Region	PostalCode	Country	HomePhone	Extension	Photo	Notes	ReportsTo	PhotoPath

'''
sqlContext.sql("SELECT * FROM EmpTable").show()
print("\n\n")

sqlContext.sql("SELECT EmployeeID,LastName,FirstName,Title,HireDate,Region FROM EmpTable").show()
print("\n\n")

#NON SQL
EmployeesDf.select('EmployeeID','LastName','FirstName','Title','HireDate','Region').show()

#####################################################          Customer Table              #####################################################

'''
CustomerID	CompanyName	ContactName	ContactTitle	Address	City	Region	PostalCode	Country	Phone	Fax
'''

'''
Error: 
sqlContext.sql("SELECT * FROM CusTable").show()
print("\n\n")
'''

''' Exact Solution
sqlContext.sql("SELECT CustomerID,CompanyName,City,Region,PostalCode,Country FROM CusTable").show()
print("\n\n")
'''

sqlContext.sql("SELECT CustomerID,Region,PostalCode,Country FROM CusTable").show()
print("\n\n")

CustomersDF.select('CustomerID','Region','PostalCode','Country').show()

#####################################################           Time Table                 #####################################################


#OrderID	CustomerID	EmployeeID	OrderDate	RequiredDate	ShippedDate

'''
OrderID	CustomerID	EmployeeID	OrderDate	RequiredDate	ShippedDate	ShipVia	Freight	ShipName	ShipAddress	ShipCity	ShipRegion	ShipPostalCode	ShipCountry
'''

sqlContext.sql("SELECT OrderID FROM OrdTable").show()
print("\n\n")

''' 
UnicodeEncodeError: 'ascii' codec can't encode characters in position 959-960: ordinal not in range(128)

sqlContext.sql("SELECT * FROM OrdTable").show()
print("\n\n")
'''

sqlContext.sql("SELECT OrderID,CustomerID,EmployeeID,OrderDate,RequiredDate,ShippedDate FROM OrdTable").show()
print("\n\n")

#######                        Creating New SparkSession to Extract Data
# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("My Map") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(OrderID = (fields[0]),CustomerID = str(fields[1].encode("utf-8")),EmployeeID = (fields[2]), OrderDate = (fields[3].encode("utf-8")),RequiredDate = str(fields[4].encode("utf-8")), ShippedDate = str(fields[5].encode("utf-8")))

lines = spark.sparkContext.textFile("/home/akhil/Desktop/3 Day Task/data/orders.csv")

header = lines.first()

lines = lines.filter(lambda line:line != header)
dfo_2 = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
OrdersDf_2 = spark.createDataFrame(dfo_2).cache()
OrdersDf_2.createOrReplaceTempView("Orders2")

print(OrdersDf_2.select('OrderID').show(5))

sqlContext.registerDataFrameAsTable(OrdersDf_2, "OrdsNTable")

from pyspark.sql.types import IntegerType

'''
registerFunction(name, f, returnType=StringType)
Registers a python function (including lambda function) as a UDF so it can be used in SQL statements.

In addition to a name and the function itself, the return type can be optionally specified. When the return type is not given it default to a string and conversion will automatically be done. For any other return type, the produced object must match the specified type.

Parameters:	
name – name of the UDF
f – python function
returnType – a pyspark.sql.types.DataType object

'''
q = {'01' : 'Q1' , '02' : 'Q1' ,'03' : 'Q1' ,'04' : 'Q2', '05' : 'Q2', '06' : 'Q2', '07' : 'Q3', '08' : 'Q3', '09' : 'Q3', '10' : 'Q4', '11' : 'Q4', '12' : 'Q4'}

d = {'01' : 'Jan' , '02' : 'Feb' ,'03' : 'Mar' ,'04' : 'Apr', '05' : 'May', '06' : 'Jun', '07' : 'July', '08' : 'Aug', '09' : 'Sep', '10' : 'Oct', '11' : 'Nov', '12' : 'Dec'}

w = {'01' : 'Week 1' , '02' : 'Week 1' ,'03' : 'Week 1' ,'04' : 'Week 1', '05' : 'Week 1', '06' : 'Week 1', '07' : 'Week 1', '08' : 'Week 2', '09' : 'Week 2', '10' : 'Week 2', '11' : 'Week 2', '12' : 'Week 2', '13' : 'Week 2', '14' : 'Week 2','15' : 'Week 3', '16' : 'Week 3', '17' : 'Week 3', '18' : 'Week 3', '19' : 'Week 3', '20' : 'Week 3', '21' : 'Week 3', '22' : 'Week 4', '23' : 'Week 4', '24' : 'Week 4','25' : 'Week 4', '26' : 'Week 4', '27' : 'Week 4', '28' : 'Week 4', '29' : 'Week 4', '30' : 'Week 4', '31' : 'Week 4'}
# 1996-07-04 00:00:00.000
sqlContext.registerFunction("Date", lambda x: x.split(' ')[0], StringType())

sqlContext.registerFunction("Year", lambda x: x.split(' ')[0].split('-')[0], StringType())

sqlContext.registerFunction("Month", lambda x: d[x.split(' ')[0].split('-')[1]], StringType())

sqlContext.registerFunction("Quarter", lambda x: q[x.split(' ')[0].split('-')[1]], StringType())

sqlContext.registerFunction("Week", lambda x: w[x.split(' ')[0].split('-')[2]], StringType())

sqlContext.sql("SELECT  Date(OrderDate) As Date, Week(OrderDate) As WeekBy, Month(OrderDate) As Month , Quarter(OrderDate) As Quarter , Year(OrderDate) AS Year From OrdsNTable").show()
#print(x)
print("\n\n")


