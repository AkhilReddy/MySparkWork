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



conf = SparkConf().setMaster("local").setAppName("My App")

sc = SparkContext(conf = conf) #Initalising or Configuring "Spark Context"

sqlContext = SQLContext(sc)

#************************************************************************************************************************************************#
#Working ON CSV files 
#************************************************************************************************************************************************#

'''  This is conversion to RDD

#This is full list of products csv
rdd = sc.textFile("/home/akhil/Desktop/Ass@SparkSQL/northwind-mongo-master/products.csv").map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[0],line[1],line[2],line[3],line[4],line[5],line[6],line[7],line[8],line[9]))

'''

'''       #This is not working as pipelined RDD 

DF = SQLContext.createDataFrame(rdd, ["ProductID", "ProductName"])
print(DF.collect())

'''

'''       # It wont Work
rdd_prod = sc.textFile("/home/akhil/Desktop/3 Day Task/data/products.csv")

DF = SQLContext.createDataFrame(rdd_prod, ["ProductID", "ProductName"])
print(DF.collect())

Error: TypeError: unbound method createDataFrame() must be called with SQLContext instance as first argument (got RDD instance instead)

'''

'''       # It wont Work
DF = sc.createDataFrame("/home/akhil/Desktop/3 Day Task/data/products.csv")

print(DF.take(5))

AttributeError: 'SparkContext' object has no attribute 'createDataFrame'

'''

'''
DF = SQLContext.read.csv("/home/akhil/Desktop/Ass@SparkSQL/northwind-mongo-master/products.csv")

print(DF.take(5))

AttributeError: 'property' object has no attribute 'csv'

'''

'''
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("My App") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID = (fields[0]), field1=str(fields[1].encode("utf-8")), field2= (fields[2]), field3= (fields[3]))

lines = spark.sparkContext.textFile("/home/akhil/Desktop/3 Day Task/data/products.csv")
df = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
schemaDf = spark.createDataFrame(df).cache()
schemaDf.createOrReplaceTempView("Products")

#print(schemaDf.take(5))

print(schemaDf.select('ID').show(5)) ## Here Even Row1 is considered with Column Names

OUT:

+---------+
|       ID|
+---------+
|ProductID|
|        1|
|        2|
|        3|
|        4|
+---------+
only showing top 5 rows

print(schemaDf.printSchema())

'''
################################################################    1    #######################################################################
''' # 1 . Working Model 

from pyspark.sql.types import StringType

sqlContext = SQLContext(sc)

Products = sc.textFile("/home/akhil/Desktop/3 Day Task/data/products.csv").map(lambda line: line.split(","))

header = Products.first()

Products_rdd = Products.filter(lambda line:line != header)

Products_df = Products_rdd.toDF(['ProductID','ProductName','SupplierID','CategoryID','QuantityPerUnit','UnitPrice','UnitsInStock','UnitsOnOrder','ReorderLevel','Discontinued' ])

print(Products_df.show())

'''

###############################################################   2    #########################################################################

''' # 2. Working Model

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("My App") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ProductID = int(fields[0]), ProductName = str(fields[1].encode("utf-8")), SupplierID = int(fields[2]), CategoryID = int(fields[3]))

lines = spark.sparkContext.textFile("/home/akhil/Desktop/3 Day Task/data/products.csv")

header = lines.first()

lines = lines.filter(lambda line:line != header)
df = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
schemaDf = spark.createDataFrame(df).cache()
schemaDf.createOrReplaceTempView("Products")

print(schemaDf.select('ProductID').show(5)) 

'''

################################################################  3   ##########################################################################

''' # 3. Working Model
from pyspark.sql.types import *

ProductsFile = sc.textFile("/home/akhil/Desktop/3 Day Task/data/products.csv")

header = ProductsFile.first()

schemaString = header.replace('"','')  # get rid of the double-quotes

print(schemaString)

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(',')]

print(fields)

print(len(fields) ) # how many elements in the header?

fields[0].dataType = IntegerType()
fields[2].dataType = IntegerType()
fields[3].dataType = IntegerType()
fields[5].dataType = FloatType()
fields[6].dataType = IntegerType()
fields[7].dataType = IntegerType()
fields[8].dataType = IntegerType()
fields[9].dataType = IntegerType()

# fields[i].dataType = TimestampType() can be used

print("\n\n")
print(fields)
print("\n\n")

# We can get rid of any annoying leading underscores or change name of field by 
# Eg: fields[0].name = 'id'

schema = StructType(fields)

print(schema)
print("\n\n")

'''
#The existence of the header along with the data in a single file is something that needs to be taken care of. It is rather easy to isolate the #header from the actual data, and then drop it using Spark’s .subtract() method for RDD’s.
'''
ProductsHeader = ProductsFile.filter(lambda l: "ProductID" in l)
print(ProductsHeader.collect())
print("\n\n")

ProductsNoHeader = ProductsFile.subtract(ProductsHeader)
print(ProductsNoHeader.count())
print("\n\n")


Products_temp = ProductsNoHeader.map(lambda k: k.split(",")).map(lambda p: (p[0], p[1], parse(p[2].strip('"')), float(p[3]), float(p[4]) , p[5], p[6] , int(p[7]), parse(p[8].strip('"')), float(p[9]), float(p[10]), int(p[11]), p[12], float(p[13]), int(p[14]), p[15] ))

Products_temp = ProductsNoHeader.map(lambda k: k.split(",")).map(lambda p: (int(p[0]),p[1],int(p[2]),int(p[3]),p[4],float(p[5]),int(p[6]),int(p[7]), int(p[8]),int(p[9])))
 
print(Products_temp.top(2)) 
print("\n\n")

Products_df = sqlContext.createDataFrame(Products_temp, schema)

print(Products_df.head(8)) # look at the first 8 rows
print("\n\n")

Products_df = ProductsNoHeader.map(lambda k: k.split(",")).map(lambda p: (int(p[0]),(p[1].strip('"')),int(p[2]),int(p[3]),(p[4].strip('"')),float(p[5]),int(p[6]),int(p[7]), int(p[8]),int(p[9]))).toDF(schema)

print(Products_df.head(10))
print("\n\n")

Q = Products_df.groupBy("SupplierID").count().show()

print(Q)
print("\n\n")

#If we have missing values in the Discontinued. But how many are they?

Na = Products_df.filter(Products_df.Discontinued == '').count()

print("\nMissing values in the Discontinued Field : ",Na)
print("\n\n")

'''
#dtypes and printSchema() methods can be used to get information about the schema, which can be useful further down in the data processing #pipeline
'''
print("\n Dtypes : ",Products_df.dtypes)
print("\n\n")

print("\nSchema:  ",Products_df.printSchema())
print("\n\n")

#Registering Table 
Products_df.registerTempTable("Products")

#SQL Query
Q1 = sqlContext.sql("SELECT SupplierID, COUNT(*) FROM Products GROUP BY SupplierID ").show()

print(Q1)
print("\n\n")

'''
#Imagine that at this point we want to change some column names: say, we want to shorten ProductID to ID, and similarly for the other 3 columns with lat/long information; we certainly do not want to run all the above procedure from the beginning – or even we might not have access to the initial CSV data, but only to the dataframe. We can do that using the dataframe method withColumnRenamed, chained as many times as required.

'''

Products_df = Products_df.withColumnRenamed('ProductID', 'ID').withColumnRenamed('SupplierID', 'SupID')

print(Products_df.dtypes)
print("\n\n")

'''
#we want to keep only the records from SupplierID 1 that do not have missing values in Discontinued column, and store the result in a pandas dataframe. Then Follow This:
'''
import pandas as pd
Products_1 = Products_df.filter("SupplierID = 1 and Discontinued != '' ").toPandas()
'''
'''

'''
#There exist already some third-party external packages, like [EDIT: spark-csv and] pyspark-csv, that attempt to do this in an automated manner, more or less similar to R’s read.csv or pandas’ read_csv
'''

'''
#####################################################################  4   #####################################################################
#http://spark.apache.org/docs/latest/sql-programming-guide.html
##############################################              sql programming guide                   ############################################

from pyspark.sql import SparkSession

#The entry point into all functionality in Spark is the SparkSession class. To create a basic SparkSession, just use SparkSession.builder
#1
# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("My App") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#2            JSON
#With a SparkSession, applications can create DataFrames from an existing RDD, from a Hive table, or from Spark data sources.
#As an example, the following creates a DataFrame based on the content of a JSON file:

# spark is an existing SparkSession
df_json = spark.read.json("examples/src/main/resources/people.json")
# Displays the content of the DataFrame to stdout
df_json.show()

#3            CSV

# spark is an existing SparkSession
df_csv = spark.read.csv("/home/akhil/Desktop/3 Day Task/data/products.csv")
header = df_csv.first()

print(header)

#$$$
# TypeError: condition should be string or Column
#df_csv = df_csv.filter(lambda line:line != header)

# Displays the content of the DataFrame to stdout
df_csv.show()


'''
In Python it’s possible to access a DataFrame’s columns either by attribute (df.age) or by indexing (df['age']). While the former is convenient for interactive data exploration, users are highly encouraged to use the latter form, which is future proof and won’t break with column names that are also attributes on the DataFrame class.
'''

# spark, df are from the previous example
# Print the schema in a tree format
df_csv.printSchema()


#4
'''
In Python it’s possible to access a DataFrame’s columns either by attribute (df.age) or by indexing (df['age']). While the former is convenient for interactive data exploration, users are highly encouraged to use the latter form, which is future proof and won’t break with column names that are also attributes on the DataFrame class.
'''
# spark, df are from the previous example
# Print the schema in a tree format
df_json.printSchema()

# Select only the "name" column
df_json.select("name").show()

# Select everybody, but increment the age by 1
df_json.select(df_json['name'], df_json['age'] + 1).show()

# Select people older than 21
df_json.filter(df_json['age'] > 21).show()

# Count people by age
df_json.groupBy("age").count().show()

# Register the DataFrame as a SQL temporary view
df_json.createOrReplaceTempView("people")

sqlDF = spark.sql("SELECT * FROM people")
sqlDF.show()

#5
'''
Global Temporary View
Temporary views in Spark SQL are session-scoped and will disappear if the session that creates it terminates. If you want to have a temporary view that is shared among all sessions and keep alive until the Spark application terminates, you can create a global temporary view. Global temporary view is tied to a system preserved database global_temp, and we must use the qualified name to refer it, e.g. SELECT * FROM global_temp.view1.
'''

# Register the DataFrame as a global temporary view
df.createGlobalTempView("people")

# Global temporary view is tied to a system preserved database `global_temp`
spark.sql("SELECT * FROM global_temp.people").show()
# +----+-------+
# | age|   name|
# +----+-------+
# |null|Michael|
# |  30|   Andy|
# |  19| Justin|
# +----+-------+

# Global temporary view is cross-session
spark.newSession().sql("SELECT * FROM global_temp.people").show()


#6
#Interoperating with RDDs

#1. The first method uses reflection to infer the schema of an RDD that contains specific types of objects. This reflection based approach leads to more concise code and works well when you "already know the schema" while writing your Spark application.

#2. The second method for creating Datasets is through a programmatic interface that allows you to construct a schema and then apply it to an existing RDD. While this method is more verbose, it allows you to "construct Datasets when the columns and their types are not known until runtime".

#A.Inferring the Schema Using Reflection

from pyspark.sql import Row

sc = spark.sparkContext

# Load a text file and convert each line to a Row.
lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people)
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are Dataframe objects.
# rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
for name in teenNames:
    print(name)
# Name: Justin

#B.Programmatically Specifying the Schema

'''
When a dictionary of kwargs cannot be defined ahead of time (for example, the structure of records is encoded in a string, or a text dataset will be parsed and fields will be projected differently for different users), a DataFrame can be created programmatically with three steps.

1.Create an RDD of tuples or lists from the original RDD;
2.Create the schema represented by a StructType matching the structure of tuples or lists in the RDD created in the step 1.
3.Apply the schema to the RDD via createDataFrame method provided by SparkSession.
'''

# Import data types
from pyspark.sql.types import *

sc = spark.sparkContext

# Load a text file and convert each line to a Row.
lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))

# Each line is converted to a tuple.
people = parts.map(lambda p: (p[0], p[1].strip()))

# The schema is encoded in a string.
schemaString = "name age"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
print(fields)
print("\n\n")

#Getting schema
schema = StructType(fields)
print(schema)
print("\n\n")

# Apply the schema to the RDD.
schemaPeople = spark.createDataFrame(people, schema)

# Creates a temporary view using the DataFrame
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
results = spark.sql("SELECT name FROM people")

results.show()

#7
#Data Sources
'''
Spark SQL supports operating on a variety of data sources through the DataFrame interface. A DataFrame can be operated on using relational transformations and can also be used to create a temporary view. Registering a DataFrame as a temporary view allows you to run SQL queries over its data. This section describes the general methods for loading and saving data using the Spark Data Sources and then goes into specific options that are available for the built-in data sources.
'''

df = spark.read.load("examples/src/main/resources/users.parquet")
df.select("name", "favorite_color").write.save("namesAndFavColors.parquet")

#Run SQL on files directly
#Instead of using read API to load a file into DataFrame and query it, you can also query that file directly with SQL.

df = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")

#8
#Save Modes
'''
Save operations can optionally take a SaveMode, that specifies how to handle existing data if present. It is important to realize that these save modes do not utilize any locking and are not atomic. Additionally, when performing an Overwrite, the data will be deleted before writing out the new data.

Scala/Java	Any Language	Meaning
SaveMode.ErrorIfExists (default) "error" (default) When saving a DataFrame to a data source, if data already exists, an exception                                                        is expected to be thrown.
SaveMode.Append	"append"	When saving a DataFrame to a data source, if data/table already exists, contents of the DataFrame are expected to be appended to existing data.
SaveMode.Overwrite	"overwrite"	Overwrite mode means that when saving a DataFrame to a data source, if data/table already exists, existing data is expected to be overwritten by the contents of the DataFrame.
SaveMode.Ignore	"ignore"	Ignore mode means that when saving a DataFrame to a data source, if data already exists, the save operation is expected to not save the contents of the DataFrame and to not change the existing data. This is similar to a CREATE TABLE IF NOT EXISTS in SQL.

'''

#9
#Saving to Persistent Tables
'''
DataFrames can also be saved as persistent tables into Hive metastore using the saveAsTable command. Notice existing Hive deployment is not necessary to use this feature. Spark will create a default local Hive metastore (using Derby) for you. Unlike the createOrReplaceTempView command, saveAsTable will materialize the contents of the DataFrame and create a pointer to the data in the Hive metastore. Persistent tables will still exist even after your Spark program has restarted, as long as you maintain your connection to the same metastore. A DataFrame for a persistent table can be created by calling the table method on a SparkSession with the name of the table.

By default saveAsTable will create a “managed table”, meaning that the location of the data will be controlled by the metastore. Managed tables will also have their data deleted automatically when a table is dropped.
'''

#10
#Schema Merging
'''
Like ProtocolBuffer, Avro, and Thrift, Parquet also supports schema evolution. Users can start with a simple schema, and gradually add more columns to the schema as needed. In this way, users may end up with multiple Parquet files with different but mutually compatible schemas. The Parquet data source is now able to automatically detect this case and merge schemas of all these files.

Since schema merging is a relatively expensive operation, and is not a necessity in most cases, we turned it off by default starting from 1.5.0. You may enable it by

setting data source option mergeSchema to true when reading Parquet files (as shown in the examples below), or
setting the global SQL option spark.sql.parquet.mergeSchema to true.
'''

from pyspark.sql import Row

# spark is from the previous example.
# Create a simple DataFrame, stored into a partition directory
sc = spark.sparkContext

squaresDF = spark.createDataFrame(sc.parallelize(range(1, 6))
                                  .map(lambda i: Row(single=i, double=i ** 2)))
squaresDF.write.parquet("data/test_table/key=1")

# Create another DataFrame in a new partition directory,
# adding a new column and dropping an existing column
cubesDF = spark.createDataFrame(sc.parallelize(range(6, 11))
                                .map(lambda i: Row(single=i, triple=i ** 3)))
cubesDF.write.parquet("data/test_table/key=2")

# Read the partitioned table
mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
mergedDF.printSchema()

# The final schema consists of all 3 columns in the Parquet files together
# with the partitioning column appeared in the partition directory paths.
# root
#  |-- double: long (nullable = true)
#  |-- single: long (nullable = true)
#  |-- triple: long (nullable = true)
#  |-- key: integer (nullable = true)


#11
#JSON Datasets
##########################################################             JSON WORKING           ##################################################
'''
Spark SQL can automatically infer the schema of a JSON dataset and load it as a DataFrame. This conversion can be done using SparkSession.read.json on a JSON file.

Note that the file that is offered as a json file is not a typical JSON file. Each line must contain a separate, self-contained valid JSON object. For more information, please see JSON Lines text format, also called newline-delimited JSON. As a consequence, a regular multi-line JSON file will most often fail.
'''

# spark is from the previous example.
sc = spark.sparkContext

# A JSON dataset is pointed to by path.
# The path can be either a single text file or a directory storing text files
path = "examples/src/main/resources/people.json"
peopleDF = spark.read.json(path)

# The inferred schema can be visualized using the printSchema() method
peopleDF.printSchema()
# root
#  |-- age: long (nullable = true)
#  |-- name: string (nullable = true)

# Creates a temporary view using the DataFrame
peopleDF.createOrReplaceTempView("people")

# SQL statements can be run by using the sql methods provided by spark
teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
teenagerNamesDF.show()
# +------+
# |  name|
# +------+
# |Justin|
# +------+

# Alternatively, a DataFrame can be created for a JSON dataset represented by
# an RDD[String] storing one JSON object per string
jsonStrings = ['{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}']
otherPeopleRDD = sc.parallelize(jsonStrings)
otherPeople = spark.read.json(otherPeopleRDD)
otherPeople.show()
# +---------------+----+
# |        address|name|
# +---------------+----+
# |[Columbus,Ohio]| Yin|
# +---------------+----+

################################################################################################################################################

#12                                            JDBC To Other Databases                                                                         #

################################################################################################################################################

'''
Spark SQL also includes a data source that can read data from other databases using JDBC. This functionality should be preferred over using JdbcRDD. This is because the results are returned as a DataFrame and they can easily be processed in Spark SQL or joined with other data sources. The JDBC data source is also easier to use from Java or Python as it does not require the user to provide a ClassTag. (Note that this is different than the Spark SQL JDBC server, which allows other applications to run queries using Spark SQL).

To get started you will need to include the JDBC driver for you particular database on the spark classpath. For example, to connect to postgres from the Spark Shell you would run the following command:

bin/spark-shell --driver-class-path postgresql-9.4.1207.jar --jars postgresql-9.4.1207.jar
Tables from the remote database can be loaded as a DataFrame or Spark SQL temporary view using the Data Sources API. Users can specify the JDBC connection properties in the data source options. user and password are normally provided as connection properties for logging into the data sources. In addition to the connection properties, Spark also supports the following case-sensitive options:

http://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases

# Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods

# Loading data from a JDBC source
jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql:dbserver") \
    .option("dbtable", "schema.tablename") \
    .option("user", "username") \
    .option("password", "password") \
    .load()

jdbcDF2 = spark.read \
    .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
          properties={"user": "username", "password": "password"})

# Saving data to a JDBC source
jdbcDF.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql:dbserver") \
    .option("dbtable", "schema.tablename") \
    .option("user", "username") \
    .option("password", "password") \
    .save()

jdbcDF2.write \
    .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
          properties={"user": "username", "password": "password"})

'''

#13
#Performance Tuning
'''
For some workloads it is possible to improve performance by either caching data in memory, or by turning on some experimental options.

Caching Data In Memory
Spark SQL can cache tables using an in-memory columnar format by calling spark.cacheTable("tableName") or dataFrame.cache(). Then Spark SQL will scan only required columns and will automatically tune compression to minimize memory usage and GC pressure. You can call spark.uncacheTable("tableName") to remove the table from memory.

Configuration of in-memory caching can be done using the setConf method on SparkSession or by running SET key=value commands using SQL.

Property Name	                              Default	 Meaning
spark.sql.inMemoryColumnarStorage.compressed  true       When set to true Spark SQL will automatically select a compression  codec  
                                                         each column based on statistics of the data.
spark.sql.inMemoryColumnarStorage.batchSize   10000      Controls the size of batches for columnar caching. Larger batch sizes can                                                      utilization and compression, but risk OOMs when caching data.
'''

#Refer : http://spark.apache.org/docs/latest/sql-programming-guide.html#other-configuration-options



#14
#Distributed SQL Engine
'''
Spark SQL can also act as a distributed query engine using its JDBC/ODBC or command-line interface. In this mode, end-users or applications can interact with Spark SQL directly to run SQL queries, without the need to write any code.
'''
#########                         Running the Thrift JDBC/ODBC server on My System
# Refer Link : http://spark.apache.org/docs/latest/sql-programming-guide.html#running-the-thrift-jdbcodbc-server
'''
root@akhil-Inspiron-3521:/usr/local/spark# ./sbin/start-thriftserver.sh
org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 running as process 20190.  Stop it first.
root@akhil-Inspiron-3521:/usr/local/spark# ./bin/beeline
Beeline version 1.2.1.spark2 by Apache Hive
beeline> !connect jdbc:hive2://localhost:10000
Connecting to jdbc:hive2://localhost:10000
Enter username for jdbc:hive2://localhost:10000: local
Enter password for jdbc:hive2://localhost:10000: ******
log4j:WARN No appenders could be found for logger (org.apache.hive.jdbc.Utils).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
Connected to: Spark SQL (version 2.0.0)
Driver: Hive JDBC (version 1.2.1.spark2)
Transaction isolation: TRANSACTION_REPEATABLE_READ
0: jdbc:hive2://localhost:10000> 
'''

# DataFrame.groupBy retains grouping columns
#####################################################       DataFrame.groupBy().agg()           ################################################
import pyspark.sql.functions as func

# In 1.3.x, in order for the grouping column "department" to show up,
# it must be included explicitly as part of the agg function call.
df.groupBy("department").agg(df["department"], func.max("age"), func.sum("expense"))

# In 1.4+, grouping column "department" is included automatically.
df.groupBy("department").agg(func.max("age"), func.sum("expense"))

# Revert to 1.3.x behavior (not retaining grouping column) by:
sqlContext.setConf("spark.sql.retainGroupColumns", "false")


###################################################                 SUPPORTS                 ################################################## 
'''
Supported Hive Features
Spark SQL supports the vast majority of Hive features, such as:

1.Hive query statements, including:
SELECT
GROUP BY
ORDER BY
CLUSTER BY
SORT BY

2.All Hive operators, including:
Relational operators (=, ⇔, ==, <>, <, >, >=, <=, etc)
Arithmetic operators (+, -, *, /, %, etc)
Logical operators (AND, &&, OR, ||, etc)
Complex type constructors
Mathematical functions (sign, ln, cos, etc)
String functions (instr, length, printf, etc)

3.User defined functions (UDF)
4.User defined aggregation functions (UDAF)
5.User defined serialization formats (SerDes)

6.Window functions

7.Joins
JOIN
{LEFT|RIGHT|FULL} OUTER JOIN
LEFT SEMI JOIN
CROSS JOIN

8.Unions
9.Sub-queries
SELECT col FROM ( SELECT a + b AS col from t1) t2

10.Sampling
11.Explain
12.Partitioned tables including dynamic partition insertion
13.View
14.All Hive DDL Functions, including:
   CREATE TABLE
   CREATE TABLE AS SELECT
   ALTER TABLE

15.Most Hive Data types, including:
TINYINT
SMALLINT
INT
BIGINT
BOOLEAN
FLOAT
DOUBLE
STRING
BINARY
TIMESTAMP
DATE
ARRAY<>
MAP<>
STRUCT<>
'''

############################################################       Data Types       ############################################################
#Spark SQL and DataFrames support the following data types:
'''
Numeric types
ByteType: Represents 1-byte signed integer numbers. The range of numbers is from -128 to 127.
ShortType: Represents 2-byte signed integer numbers. The range of numbers is from -32768 to 32767.
IntegerType: Represents 4-byte signed integer numbers. The range of numbers is from -2147483648 to 2147483647.
LongType: Represents 8-byte signed integer numbers. The range of numbers is from -9223372036854775808 to 9223372036854775807.
FloatType: Represents 4-byte single-precision floating point numbers.
DoubleType: Represents 8-byte double-precision floating point numbers.
DecimalType: Represents arbitrary-precision signed decimal numbers. Backed internally by java.math.BigDecimal. A BigDecimal consists of an arbitrary precision integer unscaled value and a 32-bit integer scale.

String type
StringType: Represents character string values.

Binary type
BinaryType: Represents byte sequence values.

Boolean type
BooleanType: Represents boolean values.

Datetime type
TimestampType: Represents values comprising values of fields year, month, day, hour, minute, and second.
DateType: Represents values comprising values of fields year, month, day.

Complex types
ArrayType(elementType, containsNull): Represents values comprising a sequence of elements with the type of elementType. containsNull is used to indicate if elements in a ArrayType value can have null values.

MapType(keyType, valueType, valueContainsNull): Represents values comprising a set of key-value pairs. The data type of keys are described by keyType and the data type of values are described by valueType. For a MapType value, keys are not allowed to have null values. valueContainsNull is used to indicate if values of a MapType value can have null values.

StructType(fields): Represents values with the structure described by a sequence of StructFields (fields).

StructField(name, dataType, nullable): Represents a field in a StructType. The name of a field is indicated by name. The data type of a field is indicated by dataType. nullable is used to indicate if values of this fields can have null values.
'''

#All data types of Spark SQL are located in the package of pyspark.sql.types. You can access them by doing

from pyspark.sql.types import *

##################### NaN Semantics
'''
There is specially handling for not-a-number (NaN) when dealing with float or double types that does not exactly match standard floating point semantics. Specifically:

NaN = NaN returns true.
In aggregations all NaN values are grouped together.
NaN is treated as a normal value in join keys.
NaN values go last when in ascending order, larger than any other numeric value.
'''
