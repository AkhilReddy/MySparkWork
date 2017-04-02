from pyspark import SparkConf, SparkContext
from operator import add
import sys

APP_NAME = "Project"

def main(sc,filename):
	rdd = sc.textFile(filename) 
	print("Akhil")
	print(rdd.collect())

if __name__ == "__main__":
   # Configure Spark
   conf = SparkConf().setAppName(APP_NAME)
   conf = conf.setMaster("local[*]")
   sc   = SparkContext(conf=conf)
   filename = sys.argv[1]
   # Execute Main functionality
   main(sc, filename)
