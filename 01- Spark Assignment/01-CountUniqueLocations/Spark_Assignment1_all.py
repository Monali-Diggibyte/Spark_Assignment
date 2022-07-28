from pyspark.sql import SparkSession

## creating Spark Session
spark = SparkSession \
        .builder \
        .appName("MySparkPractise") \
        .master("local[2]") \
        .getOrCreate()
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

## a) Count of unique locations where each product is sold.
print("Total Count: " + str(joinDF.distinct().count()))
joinDF.select("location ").distinct().show()
print("Count of Unique Locations where each Product is sold: "+ str(joinDF.select("location ").distinct().count()))

## b) Find out products bought by each user.
DF2 = joinDF.select('user_id', 'emailid', 'product_description').distinct().orderBy('user_id')
DF2.show(20, False)

## c) Total spending done by each user on each product.
DF3 = joinDF.select('user_id', 'product_description', 'price').distinct().orderBy('user_id')
DF3.show(20, False)
