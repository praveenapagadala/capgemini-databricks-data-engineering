## Phase 3A – Data Quality and Cleaning Challenge

## Project Objective

The goal of this phase is to understand how messy real-world datasets are cleaned before performing analytics or building data pipelines. In real data engineering scenarios, raw datasets often contain missing values, duplicate records, and incorrect data entries. These issues must be handled properly to ensure that downstream processing produces accurate results.

In this project, data cleaning techniques were implemented using PySpark and the same logic was demonstrated using SQL/MySQL queries. This helps understand how data cleaning concepts can be applied across different technologies used in data engineering.

## Dataset Overview

The dataset used in this phase represents customer information and contains the following fields:

- customer_id – unique identifier for each customer
- name – customer name
- city – city of the customer
- age – age of the customer
However, the dataset intentionally includes several issues to simulate real-world data problems.

Example dataset:

(1, "Ravi", "Hyderabad", 25)
(2, None, "Chennai", 32)
(None, "Arun", "Hyderabad", 28)
(4, "Meena", None, 30)
(4, "Meena", None, 30)
(5, "John", "Bangalore", -5)
This dataset contains multiple inconsistencies that must be cleaned before further processing.

## Data Quality Issues Identified

During the inspection of the dataset, the following problems were observed:

- Missing values in important columns such as customer_id, name, and city
- Duplicate records representing the same customer
- Invalid data such as negative age values
- Incomplete customer information
If these issues are not corrected, they can lead to incorrect aggregation results and unreliable analytics.

## Data Cleaning Rules Applied

To fix the issues present in the dataset, the following cleaning steps were applied:

- Rows with null customer_id were removed because customer_id acts as the unique identifier.
- Missing values in name were replaced with "Unknown".
- Missing values in city were replaced with "Unknown".
- Duplicate rows were removed to prevent repeated records.
- Rows containing negative age values were filtered out.
These steps ensure that the dataset becomes clean, reliable, and suitable for further analysis.

## PySpark Implementation

The data cleaning process was implemented using PySpark DataFrame operations.

## PySpark Code
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CustomerSales").getOrCreate()

data = [
(1, "Ravi", "Hyderabad", 25),
(2, None, "Chennai", 32),
(None, "Arun", "Hyderabad", 28),
(4, "Meena", None, 30),
(4, "Meena", None, 30),
(5, "John", "Bangalore", -5)
]

columns = ["customer_id","name","city","age"]

df = spark.createDataFrame(data, columns)

print("Row count before cleaning:", df.count())

df_clean = df.filter(col("customer_id").isNotNull())

df_clean = df_clean.fillna({
"name": "Unknown",
"city": "Unknown"
})

df_clean = df_clean.dropDuplicates()

df_clean = df_clean.filter(col("age") >= 0)

print("Cleaned Data")
df_clean.show()

print("Row count after cleaning:", df_clean.count())
```python

The pipeline performs multiple cleaning operations to ensure that only valid records remain in the dataset.

## SQL / MySQL Implementation

The same cleaning logic was also implemented using SQL queries.

## Create Raw Table
```sql
CREATE TABLE customers_raw (
    customer_id INT,
    name VARCHAR(50),
    city VARCHAR(50),
    age INT
);
```
## Insert Raw Data
```sql
INSERT INTO customers_raw VALUES
(1, 'Ravi', 'Hyderabad', 25),
(2, NULL, 'Chennai', 32),
(NULL, 'Arun', 'Hyderabad', 28),
(4, 'Meena', NULL, 30),
(4, 'Meena', NULL, 30),
(5, 'John', 'Bangalore', -5);
```

## Cleaning Query
```sql
CREATE TABLE customers_clean AS
SELECT DISTINCT
    customer_id,
    IFNULL(name,'Unknown') AS name,
    IFNULL(city,'Unknown') AS city,
    age
FROM customers_raw
WHERE customer_id IS NOT NULL
AND age >= 0;
```

## Verify Cleaned Data
```sql
SELECT * FROM customers_clean;

SELECT COUNT(*) FROM customers_clean;
```

## PySpark vs SQL Operation Mapping
| PySpark Operation         | SQL Equivalent           |
| ------------------------- | ------------------------ |
| filter(col().isNotNull()) | WHERE column IS NOT NULL |
| fillna()                  | IFNULL()                 |
| dropDuplicates()          | SELECT DISTINCT          |
| filter(condition)         | WHERE condition          |
| count()                   | COUNT()                  |

Understanding this mapping helps in translating logic between Spark pipelines and relational databases.

## Key Learnings

From this phase, several important data engineering concepts were learned:

- Real-world data is often messy and requires preprocessing.
- Data cleaning is an essential step before performing analytics or transformations.
- Duplicate and invalid records can significantly affect aggregation results.
- Validation steps such as checking row counts help confirm successful cleaning.
- PySpark provides efficient functions for cleaning large-scale datasets.

## Conclusion

This phase demonstrated how data quality issues can be handled using both PySpark and SQL approaches. By identifying and correcting problems such as missing values, duplicate records, and invalid data, the dataset was transformed into a clean and reliable format.
