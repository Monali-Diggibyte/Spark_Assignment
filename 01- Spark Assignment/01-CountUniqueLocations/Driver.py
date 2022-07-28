from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utility import *

## creating Spark Session
spark = getSparkSession("MySparkPractise")
print(spark)

## Reading user.csv file
# userDF = getReadCsvFile("./Data/user.csv")
userDF = spark.read.format("csv").options(header= True, inferschema= True, sep="," ).load("./Data/user.csv")
userDF.show()
userDF.printSchema()

## Reading transaction.csv file
transactionDF = spark.read.format("csv").options(header= True, inferschema= True, sep="," ).load("./Data/transaction.csv")
transactionDF.show()
transactionDF.printSchema()

## Joining Dataframes
joinDF = userDF.join(transactionDF, userDF.user_id == transactionDF.userid, "rightouter").orderBy(userDF.user_id)
joinDF.show()
joinDF.printSchema()

## Count of unique locations where each product is sold.
print("Total Count: " + str(joinDF.distinct().count()))
joinDF.select("location ").distinct().show()
print("Count of Unique Locations where each Product is sold: "+ str(joinDF.select("location ").distinct().count()))