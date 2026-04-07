This repository contains all the things we've learned in the data-engineering training.

## WEEK 0:

## Phase 0:

Completed the Databricks Associate Data Engineering course

## Phase 1 - SQL Basics & PySpark Basics
--> Topics Studied
Basic SQL commands: SELECT, FILTER, GROUP BY
PySpark Data Frames
Mapping of SQL commands in PySpark

--> Highlights
Understood the idea behind GROUP BY command
Translated SQL commands into PySpark code
Built basics of data manipulation skills

--> Challenges Encountered
- GROUP BY aggregation
- Writing the equivalent code in PySpark

## Phase 2 - Joins & Aggregation

--> Topics Studied
Joins in datasets using PySpark
Aggregate functions
Translating SQL into PySpark operations

--> Highlights
Learned how to use several key functions in PySpark:
join()
groupBy()
sum()
avg()
filter()
orderBy()
Understood the concept of mapping SQL joins/aggregations in PySpark

--> Challenges
Correct writing of join conditions
Different types of join
Combination of multiple aggregations in one statement# capgemini-databricks-data-engineering

## Phase 3 – ETL Pipeline with PySpark

--> Topics Covered
ETL pipeline concepts (Extract → Transform → Load)
Data ingestion from CSV files
Handling missing values
Schema validation
Dataset joins and aggregations
Pipeline-based transformations

--> Key Learnings
Learned how to process real-world datasets
Handled missing values using:
dropna()
fillna()
Converted incorrect data types using cast()
Built structured pipelines using:
join()
groupBy()
sum()
count()
window functions
Understood how to transition from SQL queries to full PySpark pipelines

--> Challenges
Managing null values
Converting incorrect data types (string → numeric)
Writing accurate join conditions
Understanding the correct order of operations in an ETL workflow

## Phase 4 – Business Data Pipeline and Segmentation

--> Topics Covered
Building an end-to-end data pipeline
- Data cleaning:
- removing null values
- removing duplicates
- filtering invalid data
- Dataset joins and aggregations
- Customer segmentation
- Final reporting pipeline

## Phase 4A additionally covered:

Different bucketing and segmentation techniques
--> Key Learnings
Built a complete pipeline from raw data to business insights
Generated analytics such as:
daily sales
city-wise revenue
customer spending
Implemented customer segmentation (Gold / Silver / Bronze)
Combined multiple transformations into a final reporting dataset
Implemented segmentation using multiple bucketing methods

--> Challenges
Handling null keys before joins
Deciding the correct join order
Managing duplicates and invalid data
Writing aggregation logic for multiple metrics
Combining all results into a single final dataset

## Phase 5 – Databricks Pipeline with Real Dataset

--> Topics Covered
Databricks environment setup
Working with the Olist real-world dataset
Data ingestion from DBFS
Schema validation
Dataset joins to create a fact table
Aggregations and analytics
- Window functions:
ranking
Customer segmentation

Building a complete pipeline in PySpark
--> Key Learnings
Learned how to upload and manage datasets in Databricks
Created a unified dataset fact_orders using joins
Applied advanced analytics using window functions
Built a full pipeline from raw data → analytics-ready dataset
Understood real-world data engineering concepts like fact tables and segmentation
--> Challenges
Understanding relationships between multiple tables
Handling missing columns such as product category
Managing file paths in Databricks
Writing correct join conditions
Implementing window functions for ranking and cumulative calculations

## Summary

By the end of Week 0, the training progressed from basic SQL queries to building complete data engineering pipelines using PySpark and Databricks.

Key areas covered include:
SQL fundamentals
PySpark DataFrame operations
ETL pipeline development
Data cleaning and validation
Business analytics transformations
Customer segmentation
Real-world data pipeline implementation in Databricks

This week helped build a strong foundation for data engineering workflows and big data processing.