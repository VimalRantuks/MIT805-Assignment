from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as max_,count,sum,to_timestamp,udf,to_date,datediff
from datetime import datetime
from pyspark.sql.types import DateType
import pandas as pd
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

sales = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///SparkCourse/Assignment/2020-Jan.csv")
    
print("Here is our inferred schema:")
sales.printSchema()



print("Filter out anyone over 21:")
sales1 = sales.filter(sales.event_type == "purchase")
sales1.show()


print("Let's display the product column:")
dffinal = sales1.select("event_time","user_id","price")
dffinal.show()

print("Group by product_count")

func =  udf (lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S UTC'), DateType())

total = dffinal.withColumn('dt', func(col('event_time')))


print("Group by product_min time")
finaldf = total.groupBy("user_id").agg(sum("price"),count("user_id"),max_("dt"))

df = finaldf.toPandas()
df.to_csv('mycsv.csv')

spark.stop()
#take me home
