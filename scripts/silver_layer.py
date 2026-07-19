#file:silver_layer.py
# ----------------------------
#we have to recreate the SparkSession for the silver_layer
# ----------------------------
from spark_session import spark

# ----------------------------
# Import the PySpark functions used for data cleaning and feature engineering.
# # ----------------------------
from pyspark.sql.functions import (
    col,
    when,
    datediff,
    count,
    initcap
)
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

silver_customer = load_customer_raw_csv(spark,CUSTOMER_PATH)
silver_claims = load_claims_raw_csv(spark,CLAIMS_PATH)
silver_policies = load_policies_raw_csv(spark,POLICIES_PATH)

# ----------------------------
# How to remove duplicates
# ----------------------------
silver_claims = silver_claims.dropDuplicates()
silver_claims = silver_claims.dropDuplicates(["claim_id","policy_id"])

silver_customer = silver_customer.dropDuplicates()
silver_customer = silver_customer.dropDuplicates(["customer_id"])

silver_policies = silver_policies.dropDuplicates()
silver_policies = silver_policies.dropDuplicates(["policy_id"])


# ----------------------------
#this is needed for the for loop
# ----------------------------
tables = [
    ("customer", silver_customer),
    ("claims", silver_claims),
    ("policies", silver_policies)
]

# ----------------------------
#
def build_transformation_silver_claims_policies(spark, claims_df, policies_df):
    
    # ----------------------------
    # Join claims with policies so we can derive additional business metrics
    # such as policy age and processing time.
    # ----------------------------

    # We create a flag column to flag missing information in the claim_amount column in the claims table
    claims_df = claims_df.withColumn(
        "is_amount_missing",
        when(col("claim_amount").isNull(), True).otherwise(False)
    )
    # we need to join claim and policies tables to have claim_date and policy_start_date

    df_claim_policy_join = claims_df.join(
        policies_df,
        on="policy_id",
        how="left"
    )


    #With datediff we calculate how many days between the start of the policy untill the claim date
    df_claim_policy_join = df_claim_policy_join\
                                            .withColumn("policy_age_days", datediff(col("claim_date"),col("policy_start_date")))\
                                            .withColumn("processing_time_days", datediff(col("decision_date"),col("claim_date")))  

    df_claim_policy_join = df_claim_policy_join\
                                            .withColumn("claim_date_secs", col("claim_date").cast("timestamp").cast("long"))
                                                    
    # Create a rolling 12-month window for each policy.
    # This allows us to count how many claims existed before
    # the current claim, which is later used as part of the fraud
    # risk score.

    window_func = Window.partitionBy("policy_id")\
                        .orderBy("claim_date_secs")\
                        .rangeBetween(-365*86400, -1)

    df_claim_policy_join = df_claim_policy_join.withColumn("claims_last_12m",count(col("claim_id")).over(window_func))

    # Calculate a simple rule-based fraud risk score.
    # Business rules:
    # +2 -> high claim amount
    # +2 -> frequent claims in the last 12 months
    # +3 -> recently started policy

    df_claim_policy_join = df_claim_policy_join.withColumn("risk_score", (
                                                when(col("claim_amount") > 10000, 2).otherwise(0) +
                                                when(col("claims_last_12m") > 3, 2).otherwise(0) +
                                                when(col("policy_age_days") < 30, 3).otherwise(0)
    ))

    # Convert the numerical risk score into business-friendly
    # categories that can be used directly in Power BI dashboards.

    df_claim_policy_join = df_claim_policy_join.withColumn("risk_category",
                                                            when(col("risk_score") <= 2 , "low")\
                                                        .when(col("risk_score") <= 4 , "Medium")\
                                                        .when(col("risk_score") <= 6 , "High")\
                                                        .otherwise("Critical"))
    return df_claim_policy_join



def build_silver_customer(customer_df):
    customer_df = customer_df.withColumn("customer_region",initcap(col("customer_region")))
    return customer_df

def get_silver_claims_policies(spark):
    claims_df = load_claims_raw_csv(spark, CLAIMS_PATH).dropDuplicates()
    policies_df = load_policies_raw_csv(spark, POLICIES_PATH).dropDuplicates()
    return build_transformation_silver_claims_policies(spark, claims_df, policies_df)

def get_silver_customer(spark):
    customer_df = load_customer_raw_csv(spark, CUSTOMER_PATH).dropDuplicates()
    return build_silver_customer(customer_df)

def build_silver_policies(policies_df):
    policies_df = policies_df.withColumn("policy_region", initcap(col("policy_region")))
    return policies_df

def get_silver_policies(spark):
    policies_df = load_policies_raw_csv(spark, POLICIES_PATH).dropDuplicates()
    return build_silver_policies(policies_df)


#-------------------------
# Utility function used during development to inspect
# data quality before applying transformations.
#-------------------------

if __name__ == "__main__":
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
    # We create a flag column to flag missing information in the claim_amount column
    silver_claims = silver_claims.withColumn("is_amount_missing",when(col("claim_amount").isNull(), True).otherwise(False))


    #----------------------------
    #How to find duplicates 
    # ----------------------------

    for name, table in tables:
        total = table.count()
        distinct = table.distinct().count()
        print(f"{name} — Total: {total}, Distinct: {distinct}, Duplicates: {total - distinct}")

    # ----------------------------