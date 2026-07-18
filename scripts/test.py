# ----------------------------
#we have to recreate the SparkSession for the silver_layer
# ----------------------------
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("silver_layer").getOrCreate()

# ----------------------------
#We have to import pyspark.sql.functions to use pyspark functions such as col
# ----------------------------
from pyspark.sql.functions import *

# ----------------------------
#We have to import the path for the *_raw_csv files
# ----------------------------
from config import CUSTOMER_PATH,CLAIMS_PATH,POLICIES_PATH

# ----------------------------
#We have to import our def from our bronze_layer
# ----------------------------
from bronze_layer import load_customer_raw_csv, load_claims_raw_csv, load_policies_raw_csv

bronze_customer_raw = load_customer_raw_csv(spark,CUSTOMER_PATH)
bronze_claims_raw = load_claims_raw_csv(spark,CLAIMS_PATH)
bronze_policies_raw = load_policies_raw_csv(spark,POLICIES_PATH)


bronze_claims_raw.filter(col("claim_date").isNull()).show()