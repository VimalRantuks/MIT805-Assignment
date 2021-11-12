
# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import time


begintime = time.time()
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

sc = spark.sparkContext

def parseLine(line):
    fields = line.split(',')
    cust = str(fields[7])
    amt = float(fields[6])
    return (cust, amt)

def mapper(entry):
    return (entry[0],map(lambda x : 1,entry[2]))

lines = sc.textFile("file:///SparkCourse/2020-Jan.csv")
tagsheader = lines.first()
header = sc.parallelize([tagsheader])
tagsdata = lines.subtract(header)
rdd = tagsdata.map(parseLine)
totalsByCust = rdd.map(lambda x: (x[0], x[1])).reduceByKey(lambda x, y:int(x+y))
results = totalsByCust.map(lambda x: (x[1], x[0])).sortByKey()
results = results.collect()

dfWithoutSchema = spark.createDataFrame(rdd)

dfWithoutSchema.show()

finaltime = time.time()

executionTime = finaltime-begintime

print('Time to run script: ' + str(executionTime))