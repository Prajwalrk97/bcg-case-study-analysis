from pyspark.sql import SparkSession

from src.main.data_processing import DataProcessingClass

spark = SparkSession.builder.appName("Case Study Analysis").getOrCreate()
data_processor = DataProcessingClass(spark=spark, env="local")
data_processor.process_data()