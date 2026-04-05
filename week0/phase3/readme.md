## Phase 3 – ETL Data Pipeline using PySpark
## Objective

The main goal of this phase is to understand how real-world data engineering pipelines are built using the ETL (Extract → Transform → Load) workflow.

Instead of writing isolated queries, the focus is on creating a structured data processing pipeline that reads raw data, cleans it, transforms it, and produces meaningful outputs.

This phase introduces the mindset of a data engineer, where data is processed step by step in a logical workflow.

## Problem Overview

In this phase we worked with datasets such as:

customers.csv
sales.csv

These datasets contained common real-world issues such as:

Missing values
Incorrect or inconsistent data
Invalid records
Improper data types

The objective was to process these datasets and generate useful analytical outputs by building a proper ETL pipeline using PySpark.

## Approach Followed

The implementation was carried out in multiple stages to simulate a real data engineering workflow.

## Data Extraction

The first step was loading raw datasets from CSV files into PySpark DataFrames.
 df = spark.read.format("csv") \
.option("header","true") \
.load("/samples/customers.csv")

This step represents the Extract stage of the ETL pipeline.

## Data Inspection

Before performing transformations, the structure of the dataset was inspected using:

df.show()
df.printSchema()

This helped identify:

column names
data types
potential data quality issues

## Data Cleaning

Since raw data often contains inconsistencies, several cleaning operations were applied.

Cleaning steps included:

Removing rows with missing key values
Handling null values
Filtering invalid records
Validating dataset integrity

Example:

clean_df = df.dropna()
filtered_df = clean_df.filter(clean_df.age > 0)

## Data Transformation

After cleaning the data, transformations were performed to extract meaningful insights.

Key transformations included:

Aggregations using groupBy()
Filtering records based on business logic
Joining datasets using customer_id
Calculating summary metrics

--Example transformation:

customers.join(sales,"customer_id") \
.groupBy("city") \
.sum("amount")

## Building the Data Pipeline

All operations were structured as a continuous workflow.

The overall pipeline followed this structure:

read data
   ↓
inspect schema
   ↓
clean data
   ↓
filter invalid records
   ↓
join datasets
   ↓
aggregate results
   ↓
generate output

This pipeline structure represents how real data engineering systems process large datasets.

## Key PySpark Operations Used

The following PySpark functions were used throughout the pipeline:
| Operation           | Purpose                |
| ------------------- | ---------------------- |
| `read()`            | Load data from files   |
| `show()`            | Preview dataset        |
| `printSchema()`     | Inspect schema         |
| `dropna()`          | Remove missing values  |
| `fillna()`          | Replace null values    |
| `filter()`          | Remove invalid records |
| `join()`            | Combine datasets       |
| `groupBy()`         | Perform aggregations   |
| `sum()` / `count()` | Calculate metrics      |

These operations form the core building blocks of PySpark data processing pipelines.

## Outputs Generated

After implementing the ETL pipeline, several analytical outputs were produced:

Daily sales calculations
City-wise revenue analysis
Repeat customers based on order count
Highest spending customer per city
Final reporting dataset containing customer-level metrics

These outputs simulate typical insights that businesses generate from transactional data.

## Data Engineering Considerations

During implementation, the following best practices were followed:

Data was cleaned before performing joins
Correct schema validation was done before aggregations
Transformations were applied in the correct sequence
Data was validated after each processing step

These practices help ensure the pipeline remains reliable and scalable.

## Challenges Encountered

Some challenges faced during this phase included:

- Handling null values in CSV datasets
- Dealing with incorrect data types
- Writing accurate join conditions
- Structuring the pipeline logically
- Translating SQL logic into PySpark operations

Overcoming these challenges helped strengthen understanding of real-world data processing.

## Key Learnings

Through this phase, several important data engineering concepts were learned:

- Real datasets are rarely clean and require preprocessing
- Schema validation is essential before performing transformations
- ETL pipelines provide a structured way to process data
- PySpark enables efficient processing of large datasets
- Thinking in terms of pipelines rather than isolated queries is crucial for data engineers

## Conclusion

Phase 3 introduced the concept of building structured ETL pipelines using PySpark. By reading raw data, cleaning inconsistencies, applying transformations, and generating insights, a complete data processing workflow was created.

This phase helped develop the foundational skills required to design scalable data pipelines used in modern data engineering environments.
