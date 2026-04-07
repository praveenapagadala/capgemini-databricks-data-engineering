## Day 2 – SQL GROUP BY and Aggregations

## Objective

The objective of Day 2 training was to understand how GROUP BY and aggregation functions work in SQL.
We practiced multiple queries using employee and sales datasets to learn how data can be summarized and analyzed.

## Dataset Description

Two tables were used for practice:

## Employee Table

Contains employee details such as:

```emp_id```
```emp_name```
```department```
```salary```
```joining_date```

## Sales Table

Contains sales information such as:

```sales_id```
```emp_id```
```product```
```amount```
```sale_date```

These tables were used to practice GROUP BY queries, joins, and aggregations.

--> Key Concepts Practiced

During the exercise we practiced:

```GROUP BY```
```COUNT()```
```SUM()```
```AVG()```
```MAX()```
```MIN()```
```HAVING```
```JOIN with aggregations```

These operations help summarize large datasets into meaningful insights.

## Example Queries

1. Total salary by department
```sql
SELECT department, SUM(salary) AS total_salary
FROM Employee
GROUP BY department;
``` 

2. Number of employees in each department
```sql
SELECT department, COUNT(*) AS employee_count
FROM Employee
GROUP BY department;
``` 

3. Average salary by department
```sql
SELECT department, AVG(salary) AS avg_salary
FROM Employee
GROUP BY department;
```
4. Total sales amount by employee
```sql
SELECT emp_id, SUM(amount) AS total_sales
FROM Sales
GROUP BY emp_id;
```

--> Challenges Faced

Some difficulties encountered during the practice included:

Understanding how GROUP BY works with aggregations
Writing correct queries using multiple conditions
Using HAVING after grouping data

## Conclusion

Day 2 training focused on data aggregation using SQL.
By practicing multiple GROUP BY queries, we learned how to summarize and analyze data effectively using SQL operations.

These concepts are important for performing data analysis and building data pipelines in data engineering workflows.