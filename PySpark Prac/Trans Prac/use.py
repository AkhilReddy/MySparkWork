
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
