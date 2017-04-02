from pyspark import SparkContext  
from pyspark import AccumulatorParam  

import random  

#creating spark context   
sc=SparkContext()  


#custom accumulator class  
class VectorAccumulatorParam(AccumulatorParam):  
      def zero(self, value):  
           dict1={}  
           for i in range(0,len(value)):  
                     dict1[i]=0  
           return dict1  

      def addInPlace(self, val1, val2):  
           for i in val1.keys():  
                     val1[i] += val2[i]  
           return val1  
 

#creating zero vector for addition  

c={}  
rand=[]  

for i in range(0,100):  
	c[i]=0   
 
#creating 10 vectors each with dimension 100 and randomly initialized   

rand=[]  
for j in range(0,10):  
      dict1={}  
      for i in range(0,100):  
           dict1[i]=random.random()  
     rand.append(dict1)       

#creating rdd from 10 vectors  
rdd1=sc.parallelize(rand)  

#creating accumulator   
va = sc.accumulator(c, VectorAccumulatorParam())  

#action to be executed on rdd in order to sumup vectors  
def sum(x):  
	global va  
	va += x  


rdd1.foreach(sum)   
#print the value of accumulator  

print va.value   




