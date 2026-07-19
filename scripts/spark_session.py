import os

from dotenv import load_dotenv

load_dotenv()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("main") \
    .config("spark.jars", os.getenv("JAR_PATH") ) \
    .getOrCreate()