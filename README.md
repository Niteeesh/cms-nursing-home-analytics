# CMS Nursing Home Risk Analytics Pipeline (AWS)

## Overview
This project implements an end-to-end data engineering and analytics pipeline to assess compliance and risk levels of U.S. nursing homes using publicly available CMS datasets.

The solution follows a RAW → SILVER → GOLD data architecture and is built entirely using AWS-native services.

---

## Architecture
The pipeline is designed using a layered approach:

CMS CSV Files  
→ Amazon S3 (RAW)  
→ AWS Glue ETL (PySpark)  
→ Amazon S3 (SILVER – Parquet)  
→ AWS Glue ETL (Business Logic)  
→ Amazon S3 (GOLD – Analytics Tables)  
→ Amazon Athena (SQL Analytics)

---

## Data Sources
- CMS Provider Information
- CMS Quality Measures (MDS & Claims-based)
- CMS Health Inspection Citations
- CMS Fire Safety Citations

---

## Data Engineering
- Ingested and processed 15+ CMS datasets
- Normalized CMS Certification Numbers (CCN)
- Converted raw CSV files to partitioned Parquet
- Built Silver tables for cleaned and standardized data
- Created Gold tables with enrichment and aggregation logic
- Implemented severity-weighted risk scoring

---

## Analytics Outputs
### Gold Tables
- **citations_enriched**  
  Event-level citation fact table enriched with deficiency descriptions and severity classification.

- **facility_risk_profile**  
  Facility-level risk metrics including:
  - Total citations
  - Infection-control citation rate
  - Severity-weighted risk score
  - Health vs fire citation breakdown
  - CMS overall rating alignment

---

## Query & Analysis
Final analytics were generated using Amazon Athena.
Key queries include:
- Identification of highest-risk facilities
- Relationship between CMS ratings and citation severity
- Infection-control risk analysis

---

## Tools & Technologies
- Amazon S3  
- AWS Glue (ETL & Data Catalog)  
- Amazon Athena  
- PySpark  
- Parquet  

---

## Cost Management
All Glue jobs and crawlers were decommissioned after analytics were generated.
Only final Gold outputs were retained to minimize ongoing cloud costs.

---

## Repository Structure
# cms-nursing-home-analytics
