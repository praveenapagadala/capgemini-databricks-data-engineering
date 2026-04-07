This repository contains all the things we've learned in the data-engineering training.

## WEEK 0

## PHASE 0

Completed the Databricks Associate Data Engineering course

## PHASE 1 - SQL Basics & PySpark Basics

--> Topics Studied
- Basic SQL commands: ```SELECT, FILTER, GROUP BY```
- PySpark Data Frames
- Mapping of SQL commands in PySpark

```Highlights```

- Understood the idea behind GROUP BY command
- Translated SQL commands into PySpark code
- Built basics of data manipulation skills

```Challenges Encountered```

- GROUP BY aggregation
- Writing the equivalent code in PySpark

## PHASE 2 - Joins & Aggregation

## Topics Studied

Joins in datasets using PySpark

Aggregate functions

Translating SQL into PySpark operations

## Highlights

Learned how to use several key functions in PySpark:
```join()```
```groupBy()```
```sum()```
```avg()```
```filter()```
```orderBy()```

Understood the concept of mapping SQL joins/aggregations in PySpark

## Challenges

- Correct writing of join conditions
- Different types of join
- Combination of multiple aggregations in one statement

## PHASE 3 – ETL Pipeline with PySpark

## Topics Covered: 

ETL pipeline concepts ```(Extract → Transform → Load)```
Data ingestion from CSV files
Handling missing values
Schema validation
Dataset joins and aggregations
Pipeline-based transformations

--> Key Learnings

1. Learned how to process real-world datasets

2. Handled missing values using:

```dropna()```
```fillna()```

3. Converted incorrect data types using cast()

4. Built structured pipelines using:

join()
groupBy()
sum()
count()

5. window functions

--> Challenges

Managing null values

Converting incorrect data types (string → numeric)

Writing accurate join conditions

Understanding the correct order of operations in an ETL workflow

## PHASE 4 – Business Data Pipeline and Segmentation

--> Topics Covered

Building an end-to-end data pipeline
- Data cleaning:
- removing null values
- removing duplicates
- filtering invalid data
- Dataset joins and aggregations
- Customer segmentation
- Final reporting pipeline

## PHASE 4A additionally covered:

Different bucketing and segmentation techniques

--> Key Learnings

Built a complete pipeline from raw data to business insights
--> Generated analytics such as:
1. daily sales
2. city-wise revenue
3. customer spending
4. Implemented customer segmentation (Gold / Silver / Bronze)
5. Combined multiple transformations into a final reporting dataset
6. Implemented segmentation using multiple bucketing methods

--> Challenges

Handling null keys before joins
Deciding the correct join order
Managing duplicates and invalid data
Writing aggregation logic for multiple metrics
Combining all results into a single final dataset

## PHASE 5 – Databricks Pipeline with Real Dataset

--> Topics Covered

- Databricks environment setup
- Working with the Olist real-world dataset
- Data ingestion from DBFS-
- Schema validation
- Dataset joins to create a fact table
- Aggregations and analytics

```Window functions```
  
- ranking
- Customer segmentation

--> Key Learnings: 

- Learned how to upload and manage datasets in Databricks
- Created a unified dataset fact_orders using joins
- Applied advanced analytics using window functions
- Built a full pipeline from raw data → analytics-ready dataset
- Understood real-world data engineering concepts like fact tables and segmentation

--> Challenges

- Understanding relationships between multiple tables
- Handling missing columns such as product category
- Managing file paths in Databricks
- Writing correct join conditions
- Implementing window functions for ranking and cumulative calculations

## Summary

By the end of Week 0, the training progressed from basic SQL queries to building complete data engineering pipelines using PySpark and Databricks.

Key areas covered include:

- SQL fundamentals
- PySpark DataFrame operations
- ETL pipeline development
- Data cleaning and validation
- Business analytics transformations
- Customer segmentation
- Real-world data pipeline implementation in Databricks

This week helped build a strong foundation for data engineering workflows and big data processing.
