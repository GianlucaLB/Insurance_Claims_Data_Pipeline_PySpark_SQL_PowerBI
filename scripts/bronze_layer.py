# bronze_layer
# ----------------------------
# In this script, I will run pyspark to load the raw CSV file into pyspark dataframe
# ----------------------------

#I will import the path from a config.py
from config import CUSTOMER_PATH,CLAIMS_PATH,POLICIES_PATH

#We need to import SparkSession to create a connection with Spark API.
from spark_session import spark

#To create a schema based on pyspark datatype
from pyspark.sql.types import *

# ----------------------------
# customer raw CSV
# ----------------------------

def load_customer_raw_csv(spark,customer_path):
    customer_schema = StructType([
                                    StructField("customer_id",StringType(),False),
                                    StructField("customer_age",IntegerType(),True),
                                    StructField("customer_region",StringType(),True),
                                    StructField("customer_tenure_days",IntegerType(),False)

    ])

    customer_bronze_layer = spark.read.option("header",True).schema(customer_schema).csv(customer_path)

    return customer_bronze_layer

customer_path = CUSTOMER_PATH

customer_raw_csv = load_customer_raw_csv(spark, customer_path)

# ----------------------------
# claims raw CSV
# ----------------------------
def load_claims_raw_csv(spark,claims_path):
    claims_schema = StructType([
                                    StructField("claim_id",StringType(),False),
                                    StructField("policy_id",StringType(),False),
                                    StructField("claim_date",DateType(),True),
                                    StructField("decision_date",DateType(),True),
                                    StructField("claim_amount",FloatType(),True),
                                    StructField("claim_status",StringType(),True),
                                    StructField("claim_type",StringType(),False)

    ])

    claims_bronze_csv = spark.read.option("header",True).schema(claims_schema).csv(claims_path)

    return claims_bronze_csv

claims_path = CLAIMS_PATH

claims_raw_csv = load_claims_raw_csv(spark, claims_path)

# ----------------------------
# policies raw CSV
# ----------------------------

def load_policies_raw_csv(spark,policies_path):
    policies_schema = StructType([
                                    StructField("policy_id",StringType(),False),
                                    StructField("customer_id",StringType(),False),
                                    StructField("policy_start_date",DateType(),True),
                                    StructField("policy_type",StringType(),False),
                                    StructField("policy_region",StringType(),True)

    ])

    policies_raw_csv = spark.read.option("header",True).schema(policies_schema).csv(policies_path)
    return policies_raw_csv

policies_path = POLICIES_PATH

policies_raw_csv = load_policies_raw_csv(spark, policies_path)


# ----------------------------
# We import these 3 functions into silver_layer.py, and we don't want the
# print/show statements below to run when this file is imported elsewhere.
# __name__ is a special variable Python sets automatically for every file:
# - if this file is run directly (python bronze_layer.py), __name__ == "__main__"
# - if this file is imported from another script, __name__ == "bronze_layer" instead
# So this block only runs when the file is executed directly, not when imported.
# ----------------------------
if __name__ == "__main__":
    customer_raw_csv = load_customer_raw_csv(spark, customer_path)
    claims_raw_csv = load_claims_raw_csv(spark, claims_path)
    policies_raw_csv = load_policies_raw_csv(spark, policies_path)

    # ----------------------------
    #Printing the column types with .printSchem() and print the content of the *_raw_csv dataframes with .show()
    # ----------------------------


    print("Content and column type of the table : customer_raw_csv")
    customer_raw_csv.show()
    customer_raw_csv.printSchema()
    print("-" * 50 )

    print("Content and column type of the table : claims_raw_csv")
    claims_raw_csv.show()
    claims_raw_csv.printSchema()
    print("-" * 50 )

    print("Content and column type of the table : policies_raw_csv")

    policies_raw_csv.show()
    policies_raw_csv.printSchema()
    print("-" * 50 )

