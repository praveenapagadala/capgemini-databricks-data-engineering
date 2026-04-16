# Subqueries & CTEs

## Objective

This notebook focuses on understanding and implementing **Subqueries** and **Common Table Expressions (CTEs)** using SQL. It covers real-world query patterns that are essential for data analysis and interviews.

---

## Topics Covered

### Subqueries

Subqueries are queries nested inside another query. They are used to perform operations that depend on intermediate results.

#### Types of Subqueries:

* **Single-row subqueries**
* **Multi-row subqueries**
* **Correlated subqueries**
* **Subqueries in SELECT, WHERE, FROM**

---

### Common Table Expressions (CTEs)

CTEs are temporary result sets defined using the `WITH` clause.

```sql
WITH cte_name AS (
    SELECT ...
)
SELECT * FROM cte_name;
```

---

## Key Concepts Practiced

* Filtering using subqueries
* Aggregation with subqueries
* Using subqueries in `WHERE` and `FROM`
* Writing reusable queries using CTEs
* Improving query readability and structure
* Replacing complex subqueries with CTEs

---

## Sample Problems Solved

* Find employees with salary above average
* Get department-wise aggregations
* Use subqueries for filtering results
* Rewrite queries using CTEs
* Compare GROUP BY vs CTE approaches

---

## Subqueries vs CTEs

| Feature     | Subqueries             | CTEs                    |
| ----------- | ---------------------- | ----------------------- |
| Readability | Less readable          | More readable           |
| Reusability | No                     | Yes                     |
| Complexity  | Hard for large queries | Easier to manage        |
| Performance | Can be slower          | Optimized in many cases |

---

## Use Cases

### Use Subqueries When:

* Query is simple
* Used only once
* Nested filtering is required

### Use CTEs When:

* Query is complex
* Need better readability
* Reuse same logic multiple times

---

## Learning Outcome

By completing this notebook, you will:

* Understand different types of subqueries
* Learn how to write efficient SQL queries
* Use CTEs to simplify complex logic
* Prepare for SQL interview questions

---

## Tools Used

* SQL
* Databricks Notebook

---

##  Conclusion

This notebook strengthens core SQL concepts and improves problem-solving skills using **subqueries and CTEs**, which are frequently used in real-world data analysis and technical interviews.

