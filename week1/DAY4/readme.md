## SQL String, Date & Numeric Functions

## Objective

This assignment helped me understand how to use SQL functions to clean, transform, and analyze data.
I learned how to combine string, numeric, date, NULL handling, and CASE logic to solve real-world problems.

## Concepts Covered

1. String Functions

Used to manipulate text data.

Functions I used:

```UPPER() → convert to uppercase```
```LOWER() → convert to lowercase```
```INITCAP() → capitalize first letter```
```SUBSTRING() / RIGHT() → extract part of string```
```LENGTH() → find string length```

Example:
```sql
SELECT UPPER(name), INITCAP(name)
FROM table;
```

--> Used for formatting names and extracting values like email domain.

2. Numeric Functions

Used for calculations and formatting numbers.

Functions I used:

```ROUND() → round values```
```TRUNCATE() → remove decimals```
```CEIL() → round up```
```FLOOR() → round down```
```ABS() → convert to positive```
```MOD() → remainder (used for patterns)```

Example:
```sql
SELECT ROUND(salary), ABS(balance)
FROM table;
```

--> Used for salary calculations, percentages, and pattern logic.

3. Date Functions

Used to handle date and time values.

Functions I used:

```DATEDIFF() → difference between dates```
```CURRENT_DATE → today’s date```
```YEAR(), MONTH() → extract parts```
```DATE_FORMAT() → display month/day name```

Example:
```sql
SELECT DATEDIFF(current_date, join_date)
FROM table;
```

--> Used for experience, delays, and duration calculations.

4. NULL Handling

NULL means missing or unknown data.

Problem:
```salary + bonus → NULL (if bonus is NULL)```

Solution:
```salary + COALESCE(bonus, 0)```

## COALESCE Definition:

Returns the first non-null value.

Helps avoid calculation errors.

5. CASE Statement

Used for conditional logic.

Syntax:
```
CASE
 WHEN condition THEN result
 ELSE result
END
```
Example:
```
CASE
 WHEN salary > 50000 THEN 'High'
 ELSE 'Low'
END
```
How I Solved Each Question

For every question, I followed this approach:

1. Identify required operations
2. Map them to SQL functions
3. Handle NULL values (if needed)
4. Apply CASE for classification

## Key Learnings

- SQL is not just queries, it’s about logic building
- Functions help convert raw data into meaningful insights
- NULL handling is very important in real-world data
- CASE statements make SQL powerful for decision-making
Combining multiple functions is the key skill

## Conclusion

Through this assignment, I learned how to:

- Clean and transform data
- Perform calculations
- Handle real-world scenarios
- Write structured SQL queries