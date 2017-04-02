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

#Normal Spark Code

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")

sc = SparkContext(conf = conf) #Initalising or Configuring "Spark Context"
'''
#************************************************************************************************************************************************#
#Working ON Text files
#************************************************************************************************************************************************#
lines = sc.textFile('/home/akhil/Desktop/SampleTextFile.txt')

#print(lines.collect()) // It gives full set of lines

#print(lines.take(10))  // It gives out top ten elements of LIST 

print(lines.first())    # It gives sample part of file 

lines_nonempty = lines.filter( lambda x: len(x) > 0 ) # We are using "filter" to use len() to remove all nonempty lines

print(lines_nonempty)   # It prints a RDD

print(lines_nonempty.count()) # It prints "count" of value

words = lines_nonempty.flatMap(lambda x: x.split()) #flatmap will return only all words in lines

print(words.take(10))

wordc1 = words.map(lambda x: (x, 1))        # Initalise each word with "1" and "word" as KEY
wordc2 = wordc1.reduceByKey(lambda x,y:x+y) # Now collect all values of "KEYS" by adding counts
print(wordc2.take(5)) 
'''
'''
[(u'semper,', 12), (u'semper.', 19), (u'ipsum.', 54), (u'ipsum,', 37), (u'mollis.', 12)]
'''
'''
wordc3 = wordc2.map(lambda x:(x[1],x[0]))   # Changing oritation of Key by "map"
print(wordc3.take(5))
'''
'''
[(12, u'semper,'), (19, u'semper.'), (54, u'ipsum.'), (37, u'ipsum,'), (12, u'mollis.')]
'''
'''
wordcounts = wordc3.sortByKey(False) # False means Decreasing order
print(wordcounts.take(10))
'''
'''
[(473, u'et'), (416, u'sit'), (352, u'at'), (350, u'ac'), (327, u'in'), (326, u'quis'), (315, u'vitae'), (314, u'non'), (310, u'a'), (304, u'vel')]
'''
'''
wordcounts2 = wordc3.sortByKey() # True means Increasing or leave as default
print(wordcounts2.take(10))
'''
'''
[(3, u'imperdiet,'), (3, u'purus,'), (3, u'vehicula,'), (3, u'maximus.'), (3, u'laoreet,'), (3, u'feugiat,'), (3, u'dignissim,'), (3, u'iaculis,'), (3, u'vulputate,'), (3, u'sodales,')]
'''
'''

#Transformation in a single invocation, with a few changes to deal with some punctuation characters and convert #the text to lower case.

#We use lower() to convert from Uppper to lower Case
wordcountsrev = sc.textFile('/home/akhil/Desktop/SampleTextFile.txt') \
        .map( lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').lower()) \
        .flatMap(lambda x: x.split()) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x,y:x+y) \
        .map(lambda x:(x[1],x[0])) \
        .sortByKey(False) 

print(wordcountsrev.take(10))

#Finding frequent word bigrams (Two words comeing as a Pair)
#Work on sentences
sent1 = sc.textFile('/home/akhil/Desktop/SampleTextFile.txt').glom() #glom() helps to get uniform list of set of lists like [[]]            
print(sent1.take(10))

sent2 = sent1.map(lambda x: " ".join(x))
print(sent2.take(3))

sentences = sent2.flatMap(lambda x: x.split("."))
print(sentences.take(5))

'''
'''
[u'Lorem ipsum dolor sit amet, consectetur adipiscing elit', u' Vivamus condimentum sagittis lacus, laoreet luctus ligula laoreet ut', u' Vestibulum ullamcorper accumsan velit vel vehicula', u' Proin tempor lacus arcu', u' Nunc at elit condimentum, semper nisi et, condimentum mi']
'''
'''
bigrams = sentences.map(lambda x:x.split()).flatMap(lambda x: [((x[i],x[i+1]),1) for i in range(0,len(x)-1)])
print(bigrams.take(10))
#new RDD contains tuples containing the word bigram (itself a tuple containing the first and second word) as the first value and the number #1 as the second value.

'''
'''
[((u'Lorem', u'ipsum'), 1), ((u'ipsum', u'dolor'), 1), ((u'dolor', u'sit'), 1), ((u'sit', u'amet,'), 1), ((u'amet,', u'consectetur'), 1), ((u'consectetur', u'adipiscing'), 1), ((u'adipiscing', u'elit'), 1), ((u'Vivamus', u'condimentum'), 1), ((u'condimentum', u'sagittis'), 1), ((u'sagittis', u'lacus,'), 1)]
'''
'''
freq_bigrams = bigrams.reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(False)
print(freq_bigrams.take(10))

'''
'''
The map and reduceByKey RDD transformations will be immediately recognizable to aficionados of the MapReduce paradigm. Spark supports the efficient parallel application of map and reduce operations by dividing data up into multiple partitions. In the example above, each file will by default generate one partition. What Spark adds to existing frameworks like Hadoop are the ability to add multiple map and reduce tasks to a single workflow.
'''
'''
#This Step is used to look at :
# The distribution of objects in each partition in our rdd
lines = sc.textFile('/home/akhil/Desktop/SampleTextFile.txt')
def countPartitions(id,iterator): 
	c = 0 
	for _ in iterator: 
		c += 1 
        yield (id,c) 

x = lines.mapPartitionsWithSplit(countPartitions).collectAsMap()

print(x)
'''
'''
* Each partition within an RDD is replicated across multiple workers running on different nodes in a cluster so that failure of a single worker should not cause the RDD to become unavailable.

* Many operations including map and flatMap operations can be applied independently to each partition, running as concurrent jobs based on the number of available cores. Typically these operations will preserve the number of partitions.

* When processing reduceByKey, Spark will create a number of output partitions based on the default paralellism based on the numbers of nodes and cores available to Spark. Data is effectively reshuffled so that input data from different input partitions with the same key value is passed to the same output partition and combined there using the specified reduce function. sortByKey is another operation which transforms N input partitions to M output partitions.
'''
'''
print(sc.defaultParallelism)

wordcounters = sc.textFile('/home/akhil/Desktop/SampleTextFile.txt') \
            .map( lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').lower()) \
            .flatMap(lambda x: x.split()) \
            .map(lambda x: (x, 1)) \
            .reduceByKey(lambda x,y:x+y)

print(wordcounters.mapPartitionsWithSplit(countPartitions).collectAsMap())

#Same thing with control on partitions @ numPartitions = 2

wordcountering = sc.textFile('/home/akhil/Desktop/SampleTextFile.txt') \
             .map( lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').lower()) \
             .flatMap(lambda x: x.split()) \
             .map(lambda x: (x, 1)) \
             .reduceByKey(lambda x,y:x+y,numPartitions=2)

print(wordcountering.mapPartitionsWithSplit(countPartitions).collectAsMap())
'''
#************************************************************************************************************************************************#
#Working ON CSV files (northwind Data Base)
#************************************************************************************************************************************************#
#This is just used to collect sample of csv i.e first 2 columns
df1 = sc.textFile("/home/akhil/Desktop/Ass@SparkSQL/northwind-mongo-master/orders.csv").map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[0],line[1]))

#print(df1.collect()[0])

print("\n\n")

#This is full list of orders csv
df1 = sc.textFile("/home/akhil/Desktop/Ass@SparkSQL/northwind-mongo-master/orders.csv").map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[0],line[1],line[2],line[3],line[4],line[5],line[6],line[7],line[8],line[9]))

#print(df1.collect()[1:5])

print("\n\n")

#This is full list of products csv
df2 = sc.textFile("/home/akhil/Desktop/Ass@SparkSQL/northwind-mongo-master/products.csv").map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[0],line[1],line[2],line[3],line[4],line[5],line[6],line[7],line[8],line[9]))

#print(df2.collect()[1:5])

print("\n\n ##This is order-details:  ")

#This is full list of order-details csv
df3 = sc.textFile("/home/akhil/Desktop/Ass@SparkSQL/northwind-mongo-master/order-details.csv").map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[0],line[1],line[2],line[3],line[4]))

#print(df3.collect()[1:5])

print("\n\n")



#Product Dimension Table

print("'''''''''''''''''''''''''''''''''''''''''''''''      Products Dimension Table      ''''''''''''''''''''''''''''''''''''''''''''''''''")

ProductDimenRDD = sc.textFile("/home/akhil/Desktop/Ass@SparkSQL/northwind-mongo-master/products.csv").map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[0],line[1],line[3]))

#print(ProductDimenRDD.collect()[1:5])

print("\n\n")

#Employee Dimension Table

print("'''''''''''''''''''''''''''''''''''''''''''''''      Employee Dimension Table      ''''''''''''''''''''''''''''''''''''''''''''''''''")


ProductDimenDf = sc.textFile("/home/akhil/Desktop/Ass@SparkSQL/northwind-mongo-master/employees.csv").map(lambda line: line.split(",")).filter(lambda line: len(line)>1)

def customFunc3(row):
	x = row[1]+row[2]
	return(x , row[3])			

samy = ProductDimenDf.map(customFunc3)
print(samy.collect()[1:5])

print("\n\n")

#df_EmpDimen = samy.toDF(['column', 'value'])
#newDF = SQLContext.createDataFrame(samy, ["Name", "Profession"])   #### TypeError: unbound method createDataFrame() must be called with SQLContext instance as first argument (got PipelinedRDD instance instead)

'''
RDD_of_Rows = samy.map(lambda x : Row(**x))
print(RDD_of_Rows)
df = SQLContext.createDataFrame(RDD_of_Rows)
#df.printschema()
''' 
# Even with above, I am facing same error.

print("\n\n")

header = samy.first()
print(header)

rddy = samy.filter(lambda line:line != header)
#Location Dimension Table
print(rddy.take(2))

print('\n\n Checking toDF() \n\n ')

print(hasattr(rddy, "toDF"))

print('\n\n')

print('\n\n Checking toDF()  Again \n\n')

sqlContext = SQLContext(sc) # or HiveContext

print(hasattr(rddy, "toDF"))

print('\n\n Now Finally ---------------- KKKKKKKKKKKKKKKKKKKK: \n\n')

df = rddy.map(lambda line: Row(Name = line[0], Profession = line[1])).toDF()

print(df.take(5))

print("\n\n")

print(df)

print("\n\n")

print(df.select('Name').show(5))

print("\n\n")

print("'''''''''''''''''''''''''''''''''''''''''''''''      Location Dimension Table     ''''''''''''''''''''''''''''''''''''''''''''''''''")

LocationDimenRDD = sc.textFile("/home/akhil/Desktop/Ass@SparkSQL/northwind-mongo-master/customers.csv").map(lambda line: line.split(",")).filter(lambda line: len(line)>1)

#.map(lambda line: (line[5],line[8]))
count = 1000

def customFunction(row):
	
   return (count + 1, row[5] ,row[8])

sample = LocationDimenRDD.map(customFunction)

#sample3 = LocationDimenRDD.withColumn('LocationCode', LocationDimenDf.CustomerID + 2)

#print(sample.collect()[1:5])

print("\n\n")


#Period Dimension Table

print("'''''''''''''''''''''''''''''''''''''''''''''''      Period Dimension Table      ''''''''''''''''''''''''''''''''''''''''''''''''''")

PeriodDimenRDD = sc.textFile("/home/akhil/Desktop/Ass@SparkSQL/northwind-mongo-master/orders.csv").map(lambda line: line.split(",")).filter(lambda line: len(line)>1)

#d = { '1' : 'Q1' , '2' : 'Q1' , '3' : 'Q1' , '4' : 'Q2' , '5' : 'Q2' , '6' : 'Q2' , '7' : 'Q3', '8' : 'Q3' , '9' : 'Q3' , '10' : 'Q4' , '11' : 'Q4' , '12' : 'Q4' ,' ' : ' '} 

d = { '01' : 'Q1' , '02' : 'Q1' , '03' : 'Q1' , '04' : 'Q2' , '05' : 'Q2' , '06' : 'Q2' , '07' : 'Q3', '08' : 'Q3' , '09' : 'Q3' , '10' : 'Q4' , '11' : 'Q4' , '12' : 'Q4' ,' ' : ' '}
 
def customfunction2(row):
	x = row[3].split(" ")
	y = x[0].split("-")
        yr = y[1]
        #q = str(y[0][1])
	#qr = d[q]
	#prc = yr + qr
        #return(prc , yr , qr)
	return(yr,y)

sam1 = PeriodDimenRDD.map(customfunction2)

#print(sam1.collect())

print("\n\n")

'''
df3n = df3.map(lambda x: (x[1],x[0],x[2],x[3],x[4]))

print(df3n.collect()[1:5])

print("\n\n")

df4 = df2.join(df3n)

print(df4.collect()[1:5])

print('\n\n')

a = df4.groupByKey()
#print(a.collect())

val = a.collect()[1:3]
print(val)
print('\n\n')

print([(j[0],[i for i in j[1]]) for j in val)
'''
#print([(j[0],[i for i in j[1]]) for j in a.collect()])
'''
print('\n\n')
'''
