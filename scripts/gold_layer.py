#gold_layer.py
from pyspark.sql.functions import *

from pyspark.sql.window import Window

from silver_layer import get_silver_claims_policies, get_silver_customer,get_silver_policies


from spark_session import spark


silver_claims_policies_enriched = get_silver_claims_policies(spark)
silver_customer_enriched = get_silver_customer(spark)
silver_policy_enriched = get_silver_policies(spark)
#----------------------------
#Creation of dim_customer
#----------------------------

def build_dim_customer(silver_customer_df):
    return silver_customer_df.select(
        "customer_id", "customer_age", "customer_region", "customer_tenure_days")

dim_customer = build_dim_customer(silver_customer_enriched)
#----------------------------
#Creation of dim_policy
#----------------------------

def build_dim_policy(silver_policies_df):
    return silver_policies_df.select(
        "policy_id", "policy_start_date", "policy_type", "policy_region")

dim_policy = build_dim_policy(silver_policy_enriched )

#----------------------------
#Creation of dim_claim_type
#----------------------------
def build_dim_claim_type(silver_claims_df):
    distinct_types = silver_claims_df.select("claim_type").distinct()
    window_id = Window.orderBy("claim_type")
    return distinct_types.withColumn("claim_type_id", row_number().over(window_id))

dim_claim_type = build_dim_claim_type(silver_claims_policies_enriched)

#----------------------------
#Creation of fact_claims
#----------------------------
def build_fact_claims(silver_claims_policies_df):
    silver_claims_policies_df  = silver_claims_policies_df.join(dim_claim_type,on="claim_type",how="left")
    return silver_claims_policies_df.select(
        "claim_id", "policy_id", "customer_id",
        "claim_type_id","claim_date", "decision_date",
        "claim_amount", "claim_status",
        "policy_age_days", "processing_time_days",
        "claims_last_12m", "risk_score", "risk_category",
        "is_amount_missing"
    )

fact_claims = build_fact_claims(silver_claims_policies_enriched)
