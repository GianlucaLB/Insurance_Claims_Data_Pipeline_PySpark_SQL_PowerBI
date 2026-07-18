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
#We have to import pyspark.sql.window to use pyspark Window functions
# ----------------------------
from pyspark.sql.window import Window

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
"""
tables = [("customer", bronze_customer_raw), ("claims", bronze_claims_raw), ("policies", bronze_policies_raw)]

#-------------------------
#How to check/count how many null values are present in our columns
# ----------------------------
def check_null(table, name):
    for cols in table.columns:
        null_count = table.filter(col(cols).isNull()).count()
        print(f"{name} -- {cols}: {null_count} null values")

for name, table in tables:
    check_null(table, name)
    print(f"{'-' * 30}\n")


#We create a flag column to flag missing information in the claim_amount column
bronze_claims_raw = bronze_claims_raw.withColumn("is_amount_missing" , when(col("claim_amount").isNull(),True).otherwise(False))
# ----------------------------

# ----------------------------
#How to find duplicates 
# ----------------------------
for name, table in tables:
    total = table.count()
    distinct = table.distinct().count()
    print(f"{name} — Total: {total}, Distinct: {distinct}, Duplicates: {total - distinct}")


#How to remove duplicates

bronze_claims_raw = bronze_claims_raw.dropDuplicates()
bronze_customer_raw = bronze_customer_raw.dropDuplicates()
bronze_policies_raw = bronze_policies_raw.dropDuplicates()
"""
# ----------------------------

# ----------------------------
#How to calculate how many days between the policy start and the claim date
# ----------------------------

#First we need to join claim and policies tables to have claim_date and policy_start_date

df_claim_policy_join = bronze_claims_raw.join(bronze_policies_raw, on="policy_id",how="left")

#With datediff we calculate how many days between the start of the policy untill the claim date
df_claim_policy_join = df_claim_policy_join\
                                        .withColumn("policy_age_days", datediff(col("claim_date"),col("policy_start_date")))\
                                        .withColumn("processing_time_days", datediff(col("decision_date"),col("claim_date")))  

df_claim_policy_join = df_claim_policy_join\
                                        .withColumn("claim_date_secs", col("claim_date").cast("timestamp").cast("long"))
                                                  

window_func = Window.partitionBy("policy_id")\
                    .orderBy("claim_date_secs")\
                    .rangeBetween(-365*86400, -1)

df_claim_policy_join = df_claim_policy_join.withColumn("claims_last_12m",count(col("claim_id")).over(window_func))


df_claim_policy_join = df_claim_policy_join.withColumn("risk_score", (
                                            when(col("claim_amount") > 10000, 2).otherwise(0) +
                                            when(col("claims_last_12m") > 3, 2).otherwise(0) +
                                            when(col("policy_age_days") < 30, 3).otherwise(0)
))


df_claim_policy_join = df_claim_policy_join.withColumn("risk_category", when(col("risk_score") <= 2 , "low")\
                                                       .when(col("risk_score") <= 4 , "Medium")\
                                                        .when(col("risk_score") <= 6 , "High")\
                                                        .otherwise("Critical"))

df_claim_policy_join.select("policy_id","claim_id","policy_age_days","claim_date_secs",
                            "claims_last_12m","processing_time_days","risk_score"
                            ,"risk_category").show()
