## Day 3 – Conditional Logic using CASE and WHEN in SQL

## Objective

The objective of Day 3 training was to understand how conditional logic works in SQL using ```CASE and WHEN statements.```

These conditions help categorize data, apply business rules, and generate meaningful insights from datasets.

## Dataset Used

For this exercise, two tables were used:

```Employee Table```

Contains employee information such as:

- emp_id
- emp_name
- department
- salary
- joining_date

```Sales Table```

Contains sales transaction details:

- sales_id
- emp_id
- product
- amount
- sale_date

These datasets were used to practice conditional queries and business logic implementation.

During this session, the following concepts were implemented:

```CASE WHEN conditions```

- Conditional column creation
- Categorizing records based on values
- Applying business logic using SQL
- Using conditions along with aggregations and joins

--> Example Query

1. Categorizing Employees Based on Salary
```sql
SELECT emp_name,
       salary,
       CASE
           WHEN salary >= 75000 THEN 'High Salary'
           WHEN salary BETWEEN 60000 AND 74999 THEN 'Medium Salary'
           ELSE 'Low Salary'
       END AS salary_category
FROM Employee;
```

This query classifies employees into different salary categories using CASE conditions.

## Key Learnings

From this exercise, the following concepts were learned:

- How to use CASE WHEN for conditional logic in SQL

## Challenges Faced

Some difficulties encountered during practice included:

Writing correct CASE conditions

Deciding appropriate ranges for categorization

Understanding how conditions affect query results

## Conclusion

Day 3 training focused on implementing conditional logic using CASE and WHEN statements.
These concepts are important in real-world data analysis where data often needs to be categorized or transformed based on business rules.