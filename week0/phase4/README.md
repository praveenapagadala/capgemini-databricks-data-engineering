## Phase 4 – Mini Project: Business Pipeline & Analytics 

--> Objective

Build an end-to-end data pipeline using PySpark to transform raw data into meaningful business insights.

--> Project Overview

In this project, we worked with multiple datasets (customers, orders, sales) and performed:

Data cleaning Data transformation Aggregations Business analysis 

--> Steps Performed

Data CleaningRemoved null values (e.g., customer_id) Removed duplicate records Filtered invalid values (negative amounts) Checked and corrected data types

Data Transformation & Analysis

- Task 1: Daily Sales

Calculated total sales per day Output: date, total_sales

- Task 2: City-wise Revenue

Calculated revenue for each city Output: city, total_revenue

- Task 3: Top 5 Customers

Identified top spending customers Output: customer_name, total_spend

- Task 4: Repeat Customers

Found customers with more than 1 order Output: customer_id, order_count

- Task 5: Customer Segmentation

Categorized customers based on spending: Gold: > 100 Silver: 50– 100 Bronze: < 50 Output: customer_name, total_spend, segment

Final Reporting Table
Combined all insights into one dataset Output columns: customer_name city total_spend order_count segment