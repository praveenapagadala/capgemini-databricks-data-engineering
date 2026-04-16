# Insurance Data Pipeline – PySpark & SQL

## Objective

This project focuses on building an end-to-end **data engineering pipeline** in the insurance domain using **PySpark and SQL**. The goal is to clean, validate, transform, and analyze insurance data to derive meaningful insights.

---

## Datasets Used

The pipeline uses the following datasets:

1. **customers**

   * Customer details (ID, name, age, city)

2. **policies**

   * Insurance policies purchased by customers
   * Includes premium and policy type

3. **claims**

   * Claims raised by customers against policies

4. **agents**

   * Agents managing policies

5. **policy_agent**

   * Mapping between policies and agents

---

## Business Understanding

```text id="o2z2l8"
Customer → buys Policy → pays Premium  
Policy → may generate Claims  
Agent → manages policies  
```
Goal:

* Maximize premium collection
* Minimize claim risk

---
## Pipeline Workflow

```text id="j0r0fq"
Data Ingestion → Cleaning → Validation → Transformation → Analysis
```

---

## Phase 1: Data Understanding

* Loaded datasets using PySpark
* Checked schema using `printSchema()`
* Verified row counts
* Identified:

  * Null values
  * Negative premium values
  * Invalid foreign keys

---

##  Phase 2: Data Cleaning

* Removed or corrected:

  * Negative `premium` values
  * Negative `claim_amount`
* Handled null values
* Standardized string columns using:

  * `trim()`
  * `lower()`
* Converted date columns using `to_date()`

---

## Phase 3: Data Validation

* Used **left anti join** to detect invalid records
* Validated relationships:

  * Customer → Policy
  * Policy → Claim
* Created validation checks:

  * Total records before cleaning
  * Invalid records
  * Cleaned dataset counts

---

## Phase 4: Data Transformation

* Joined all datasets carefully to avoid duplication
* Computed:

  * **Total Premium per Customer**
  * **Total Claim per Customer**

```python
df.groupBy("customer_id").agg(
    sum("premium").alias("total_premium"),
    sum("claim_amount").alias("total_claim")
)
```

* Calculated:

  * **Risk Score = total_claim / total_premium**

---

## Phase 5: SQL using CTE

* Converted DataFrames to temporary views
* Used **CTEs (WITH clause)** to break complex logic into steps
* Performed:

  * Aggregations
  * Risk analysis
  * Customer-level insights

---

## Phase 6: Window Functions

* Applied:

  * `ROW_NUMBER()`
  * `RANK()`
  * `DENSE_RANK()`

* Use cases:

  * Top risky customers per city
  * Ranking agents by performance

---

## Phase 7: Final Output

* Generated final analytical datasets
* Verified:

  * No negative values
  * No null critical fields
  * Consistent aggregations

---

## Key Insights

* Customers with high claim-to-premium ratio are **high risk**
* Certain cities show higher claim patterns
* Top agents contribute significantly to premium revenue

---

## Concepts Used

* Data Cleaning & Validation
* Joins & Anti Joins
* Aggregations
* Window Functions
* CTEs (Common Table Expressions)
* Data Pipeline Design

---

## Architecture

```text id="qz9d9y"
Bronze Layer → Raw Data  
Silver Layer → Cleaned Data  
Gold Layer → Aggregated Insights  
```

## Validation Highlights

* No negative premium or claim values
* No invalid foreign keys
* Aggregations are consistent across levels
* No duplicate amplification after joins

(Validated using guidelines )

---

## Tools & Technologies

* PySpark
* SQL
* Databricks
* Delta Lake (conceptual)

---

## Conclusion

This project demonstrates how to design a data pipeline by:

* Cleaning and validating real-world data
* Performing transformations and aggregations
* Generating insights for business decision-making
---

