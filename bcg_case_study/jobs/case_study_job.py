from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Case Study").getOrCreate()