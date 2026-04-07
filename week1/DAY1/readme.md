## Day 1 – Data Preprocessing and ETL using PySpark
--> Objective

The objective of Day 1 training was to understand the basic ETL (Extract → Transform → Load) workflow using PySpark.
We worked with a sample dataset and performed several data preprocessing steps such as reading the dataset, cleaning missing values, handling duplicates, and standardizing column values.

This exercise helped us understand how raw datasets are prepared before performing analytics or building data pipelines.

## Dataset Description

The sample dataset contained the following columns:

CustomerID – Unique identifier for each customer
Name – Name of the customer
Country – Country of the customer
JoinDate – Date when the customer joined
Sales – Sales amount associated with the customer
Category – Customer category

While inspecting the dataset, several issues were observed such as:

Missing values in Sales and Category
Blank values represented as "blank" or "Blank"
Inconsistent formatting in Country names (e.g., india, India)
Potential duplicate records

These issues needed to be cleaned before further processing.

## ETL Process Performed

The dataset was processed through the following ETL steps:

1️. Data Extraction

The dataset was read from the catalog using PySpark.
df = spark.read.csv("/Volumes/day1/default/sample1", header=True, inferSchema=True)

df.display()
df.show()

2. Handling Blank Values

Some fields contained "blank" or "Blank" instead of proper null values.
These were replaced with None so that PySpark can treat them as missing values.
df = df.replace(["blank","Blank"], None)

3. Standardizing Country Names

Country names appeared with inconsistent casing such as india and India.
To maintain consistency, all country names were converted to uppercase.

from pyspark.sql.functions import upper, col

df = df.withColumn("Country", upper(col("Country")))

4️. Handling Missing Join Dates

If the JoinDate column contained null values, they were handled to ensure proper formatting.

from pyspark.sql.functions import when

df = df.withColumn("joinDate",
                   when(col("joinDate").isNull(), None)
                   .otherwise(col("joinDate")))

5️. Filling Missing Values

Missing values in Category and Sales were replaced with default values.

Category → "Unknown"
Sales → "0"
df = df.fillna({
    "Category": "Unknown",
    "Sales": "0"
})

This ensures the dataset does not contain empty values for these columns.

6️. Filtering Data

Records where Sales equals 0 were filtered to inspect customers with no sales activity.

df = df.filter(col("Sales") == 0)

df.display()

## Final Output

After applying the transformations:

Blank values were converted to null values
Country names were standardized
Missing values were replaced
Data inconsistencies were resolved
Filtered records were displayed for analysis

This resulted in a cleaner dataset ready for further analysis or transformation.

## Conclusion

Day 1 training focused on understanding how to clean and preprocess datasets using PySpark.
Data preprocessing is an important step in any data engineering workflow because raw data often contains inconsistencies.