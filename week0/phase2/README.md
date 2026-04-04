# Phase 2 – SQL to PySpark Joins and Aggregations

## Objective

The goal of Phase 2 was to bridge the gap between simple SQL queries and real-world data engineering tasks. In this phase, I worked with multiple datasets and learned how to translate SQL logic into PySpark DataFrame operations.

## Concepts Learned

During this phase, I learned several important data processing concepts.

The first concept was working with **multiple datasets** such as customers and orders. I learned how these datasets can be connected using a common column like `customer_id`.

Another key concept was **data cleaning**. Before performing joins and aggregations, it is important to remove rows with missing key values to avoid incorrect results.

I also learned about **joins**, which allow combining data from different tables. I practiced using inner joins and left joins to connect customer and order data.

Aggregation was another important concept. Using operations like `SUM`, `COUNT`, and `AVG`, I was able to calculate business metrics such as total spend, number of orders, and average order value.

Finally, I learned how to **sort and rank data** to identify top customers based on their total spending.

## Tasks Completed

The following exercises were completed in this phase:

1. Calculated total order amount for each customer
2. Identified the top 3 customers based on total spending
3. Found customers who have not placed any orders
4. Calculated city-wise total revenue
5. Calculated average order amount per customer
6. Identified customers with more than one order
7. Sorted customers by total spending

These exercises helped me understand how SQL queries can be implemented using PySpark transformations.

## Outputs Obtained

The outputs from this phase included:

* Total spending for each customer
* Top customers contributing the most revenue
* Customers who did not place any orders
* Revenue generated from each city
* Average order values for customers


## Key Learnings

From this phase, I learned that data engineering tasks often involve combining datasets, cleaning data, and performing aggregations to generate useful insights.

I also gained confidence in translating SQL queries into PySpark code, which is an essential skill for working with big data platforms like Databricks.

This phase helped me move from simple data exploration to more realistic data analysis scenarios.

