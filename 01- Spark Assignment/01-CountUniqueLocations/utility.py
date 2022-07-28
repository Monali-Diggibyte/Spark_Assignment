from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def getSparkSession(appname):
    spark = SparkSession \
        .builder \
        .appName(appname) \
        .master("local[2]") \
        .getOrCreate()
    return(spark)

def getReadCsvFile(filepath):
    DF = spark.read.format("csv").options(header=True, inferschema=True, sep=",").load(filepath)
    return(DF)

def getLocation():
