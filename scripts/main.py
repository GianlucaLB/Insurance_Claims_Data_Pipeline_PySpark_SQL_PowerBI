from gold_layer import dim_customer, dim_policy, dim_claim_type, fact_claims

import os

from dotenv import load_dotenv
load_dotenv()

from spark_session import spark

# ----------------------------
#How to connect using PYSPARK to SQL Server and write the tables into a database.
# ----------------------------
tables_to_write = [
    (dim_customer, "dim_customer"),
    (dim_policy, "dim_policy"),
    (dim_claim_type, "dim_claim_type"),
    (fact_claims, "fact_claims"),
]

for df, table_name in tables_to_write:
    df.write \
        .format("jdbc") \
        .option("url", os.getenv("JDBC_URL")) \
        .option("dbtable", table_name) \
        .option("user", os.getenv("DB_USER")) \
        .option("password", os.getenv("DB_PASSWORD")) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .mode("overwrite") \
        .save()
    print(f"Table: {table_name}")

# ----------------------------
#How to save files in parquet for storing or importing in others tools
# ----------------------------

for df,table_name in tables_to_write:
    df.write\
        .format("parquet")\
        .mode("overwrite")\
        .save(f"{os.getenv('PARQUET_BASE_PATH')}/{table_name}")