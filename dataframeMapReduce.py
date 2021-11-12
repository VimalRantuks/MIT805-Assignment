from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as max_,count,sum,udf,round
from datetime import datetime
from pyspark.sql.types import DateType
import time


begintime = time.time()

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

salesPrelim = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///Users/ranch/OneDrive/Documents/MIT 805/Jan_Feb2020.csv")
    
print("The inferred schema:")
salesPrelim.printSchema()



print("Filter out event types that are not purchases")
salesPurchase = salesPrelim.filter(salesPrelim.event_type == "purchase")
salesPurchase.show()


print("Display Selected columns:")
dffinal = salesPurchase.select("event_time","user_id","price")
dffinal.show()

func =  udf (lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S UTC'), DateType())

total = dffinal.withColumn('dt', func(col('event_time')))



print("Group by user_id where total ")
finaldf = total.groupBy("user_id").agg(round(sum("price"),2),count("user_id"),max_("dt"))

final = finaldf.withColumnRenamed('round(sum("price"),2)',"Monetary")\
        .withColumnRenamed("count(user_id)","Frequency")\
        .withColumnRenamed("max(dt)","Recent_Date")

final.show()

df = final.toPandas()
df.to_csv('CosmeticEC.csv')
finaltime = time.time()

executionTime = finaltime-begintime

print('Time to run script: ' + str(executionTime))

spark.stop()
#testing a line