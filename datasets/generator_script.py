# ----------------------------
# This was genereted by Claude AI under my instruction.
# ----------------------------
from faker import Faker
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

fake = Faker()
np.random.seed(42)
random.seed(42)

# ----------------------------
# CONFIG
# ----------------------------
N_CUSTOMERS = 1000
N_POLICIES = 1500
N_CLAIMS = 4000

REGION_MISMATCH_RATE = 0.03  # 3% intentional mismatches

regions = [
    "California", "Texas", "Florida", "New York",
    "Illinois", "Georgia", "Ohio", "Arizona"
]

policy_types = ["Auto", "Home", "Health", "Life"]

claim_types = [
    "Collision", "Theft", "Fire", "Flood",
    "Injury", "Wind", "Water Damage"
]

claim_statuses = ["Approved", "Denied", "Pending"]

# ----------------------------
# CUSTOMERS TABLE
# ----------------------------
customers = []

for i in range(1, N_CUSTOMERS + 1):
    customer_id = f"CUST{i:05d}"

    customers.append({
        "customer_id": customer_id,
        "customer_age": random.randint(18, 85),
        "customer_region": random.choice(regions),
        "customer_tenure_days": random.randint(30, 3650)
    })

customers_df = pd.DataFrame(customers)

# Add intentional duplicate customers
customers_df = pd.concat([
    customers_df,
    customers_df.sample(5, random_state=1)
], ignore_index=True)

# ----------------------------
# POLICIES TABLE
# ----------------------------
policies = []

unique_customer_ids = customers_df["customer_id"].unique()

for i in range(1, N_POLICIES + 1):
    policy_id = f"POL{i:06d}"
    customer_id = random.choice(unique_customer_ids)

    customer_region = customers_df.loc[
        customers_df["customer_id"] == customer_id,
        "customer_region"
    ].iloc[0]

    # 3% region mismatch
    if random.random() < REGION_MISMATCH_RATE:
        possible_regions = [r for r in regions if r != customer_region]
        policy_region = random.choice(possible_regions)
    else:
        policy_region = customer_region

    start_date = datetime.today() - timedelta(days=random.randint(1, 1825))

    policies.append({
        "policy_id": policy_id,
        "customer_id": customer_id,
        "policy_start_date": start_date.date(),
        "policy_type": random.choice(policy_types),
        "policy_region": policy_region
    })

policies_df = pd.DataFrame(policies)

# Introduce some missing policy regions
missing_idx = np.random.choice(policies_df.index, size=10, replace=False)
policies_df.loc[missing_idx, "policy_region"] = None

# ----------------------------
# CLAIMS TABLE
# ----------------------------
claims = []

policy_lookup = policies_df.set_index("policy_id")

for i in range(1, N_CLAIMS + 1):
    claim_id = f"CLM{i:07d}"
    policy_id = random.choice(policies_df["policy_id"].tolist())

    policy_start = pd.to_datetime(
        policy_lookup.loc[policy_id, "policy_start_date"]
    )

    # Claim must occur after policy start
    claim_date = policy_start + timedelta(
        days=random.randint(1, 730)
    )

    claim_date = min(claim_date, datetime.today())

    status = random.choices(
        claim_statuses,
        weights=[0.7, 0.15, 0.15]
    )[0]

    # Decision date logic
    if status == "Approved":
        decision_date = claim_date + timedelta(
            days=random.randint(3, 30)
        )
    elif status == "Denied":
        decision_date = claim_date + timedelta(
            days=random.randint(1, 21)
        )
    else:  # Pending
        decision_date = None

    amount = round(
        np.random.gamma(shape=2, scale=2500),
        2
    )

    claims.append({
        "claim_id": claim_id,
        "policy_id": policy_id,
        "claim_date": claim_date.date(),
        "decision_date": (
            decision_date.date() if decision_date else None
        ),
        "claim_amount": amount,
        "claim_status": status,
        "claim_type": random.choice(claim_types)
    })

claims_df = pd.DataFrame(claims)

# Introduce some null claim amounts
null_idx = np.random.choice(claims_df.index, size=15, replace=False)
claims_df.loc[null_idx, "claim_amount"] = None

# Add duplicate claims
claims_df = pd.concat([
    claims_df,
    claims_df.sample(8, random_state=2)
], ignore_index=True)

# ----------------------------
# WRITE RAW FILES
# ----------------------------
customers_df.to_csv(r"datasets\customer_raw\customers_raw.csv", index=False)
policies_df.to_csv(r"datasets\policies_raw\policies_raw.csv", index=False)
claims_df.to_csv(r"datasets\claims_raw\claims_raw.csv", index=False)

# ----------------------------
# SUMMARY
# ----------------------------
mismatch_count = (
    policies_df.merge(customers_df, on="customer_id")
    .query("policy_region != customer_region")
    .shape[0]
)

print("Files created:")
print(" - customers_raw.csv")
print(" - policies_raw.csv")
print(" - claims_raw.csv")

print("\nRow counts:")
print(f"Customers: {len(customers_df):,}")
print(f"Policies : {len(policies_df):,}")
print(f"Claims   : {len(claims_df):,}")

print(f"\nIntentional region mismatches: {mismatch_count}")

print("\nClaim status distribution:")
print(claims_df["claim_status"].value_counts())