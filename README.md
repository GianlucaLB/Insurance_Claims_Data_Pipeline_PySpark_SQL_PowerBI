# Insurance Claims Data Pipeline with PySpark & SQL

## Overview

Insurance companies collect data from multiple operational systems such as policy management, customer management, and claims processing.

Since these datasets are typically stored separately, they must be integrated through an ETL pipeline before they can be used for analytics.

This project simulates that workflow by building an end-to-end data pipeline using PySpark. The pipeline:

- Ingests multiple raw datasets
- Performs data quality checks
- Cleans and standardises the data
- Engineers business features
- Builds a star schema
- Exports the curated data to SQL Server and Parquet

The project follows a Medallion Architecture (Bronze → Silver → Gold).

## Dataset

The dataset was generated using Python and the Faker library to simulate a realistic insurance environment.

It consists of three independent raw tables that require joins during the ETL process.

### Customers

Contains customer demographic information.

### Policies

Contains policy information associated with customers.

### Claims

Contains insurance claim transactions.

Claims reference policies but not customers directly. This relationship requires joining the Policies table during the ETL process to retrieve customer information.

# Script files

## Bronze Layer

The Bronze layer is responsible for ingesting the raw CSV files into Spark DataFrames while enforcing an explicit schema.

Using explicit schemas ensures:

- Consistent data types
- Faster ingestion
- Protection against unexpected schema changes caused by malformed input files

No transformations are performed in this layer. The objective is simply to preserve the raw data.

## Silver Layer

The Silver layer is responsible for data cleaning, standardisation, data quality checks and feature engineering.

The main transformations include:

- Removing duplicate records
- Identifying missing claim amounts
- Standardising region names
- Joining Claims and Policies datasets
- Engineering business features

The following columns are created during the transformation process:

- policy_age_days
- processing_time_days
- claims_last_12m
- risk_score
- risk_category

A rolling 12-month window is also implemented using a PySpark Window Function to calculate the number of previous claims submitted for each policy. This feature is later used as part of the fraud risk score.

## Gold Layer

The Gold layer prepares the curated datasets for analytics by creating a dimensional model composed of one fact table and multiple dimension tables.

The resulting tables can then be exported to SQL Server or stored as Parquet files for downstream analytical workloads.

Parquet was chosen because it is a columnar storage format widely used in modern data platforms. Compared to CSV, it provides better compression and improved query performance.

A star schema was implemented because separating dimensions from the fact table reduces redundancy and simplifies analytical queries.

## SQL Analytics

The curated star schema is loaded into SQL Server and analysed using SQL.

The repository includes example analytical queries for:

- Claim approval rate
- Average claim payout
- High-risk claims
- Average claim processing time
- Monthly claims trend

## Data Quality Decisions

Several data quality rules are applied during the ETL process.

| Decision | Reason |
|----------|--------|
| Remove duplicate records | Prevent duplicate transactions |
| Flag missing claim amounts | Preserve the original record while highlighting incomplete data |
| Standardise region names | Ensure consistent joins and reporting |
| Calculate policy_age_days using claim_date | Reflect the policy age at the time of the claim |
| Calculate processing_time_days only for completed claims | Pending claims do not have a decision date |

## Technologies

| Technology | Purpose |
|------------|---------|
| PySpark | ETL and feature engineering |
| Spark SQL | Data processing |
| SQL Server | Analytical data storage |
| Parquet | Curated storage format |
| Python | Data generation and ETL |
| Faker | Synthetic dataset generation |
| Git | Version control |
| GitHub | Source code hosting |


##  About

Created by Gianluca La Barbera as part of a personal data analytics portfolio

