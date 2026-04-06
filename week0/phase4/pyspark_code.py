from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder .appName("Phase4_Business_Pipeline") .getOrCreate()

# Load Data
customers = spark.read.format("csv").option("header", "true") .option("inferSchema", "true") .load("/samples/customers.csv")

sales = spark.read.format("csv") .option("header", "true") .option("inferSchema", "true") .load("/samples/sales.csv")

customers.filter(F.col("customer_id").isNull())

# data Cleaning

customers = customers.dropna(subset=["customer_id"]).dropDuplicates(["customer_id"])
#customer_id	first_name	last_name	email	phone_number	address	city	state	zip_code

sales = sales.dropna(subset=["customer_id"]) .dropDuplicates(["sale_id"]) .filter(F.col("total_amount") > 0)
#sale_id	customer_id	product_id	sale_date	quantity	total_amount

#task1
daily_sales = sales.groupBy("sale_date") .agg(F.sum("total_amount").alias("total_sales"))
display(daily_sales)

#task2
city_revenue = sales.join(customers, "customer_id") .groupBy("city") .agg(F.sum("total_amount").alias("total_revenue"))
display(city_revenue)

#task33
customers = customers.withColumn("customer_name",F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
#display(customers)
top_customers = sales.join(customers, "customer_id").groupBy("customer_name").agg(F.sum("total_amount").alias("total_spend")).orderBy(F.desc("total_spend")).limit(5)
display(top_customers)

#task4
repeat_customers = sales.groupBy("customer_id").agg(F.count("sale_id").alias("order_count")).filter(F.col("order_count") > 1)
display(repeat_customers)

#task5

#created a new table with customer id and total spend
customer_spend = sales.groupBy("customer_id").agg(F.sum("total_amount").alias("total_spend"))
#display(customer_spend)

#Customer Segmentation
segmentation = customer_spend.join(customers, "customer_id") .withColumn("segment", F.when(F.col("total_spend") > 1000, "Gold")
     .when((F.col("total_spend") >= 50) & (F.col("total_spend") <= 100), "Silver")
     .otherwise("Bronze")
    )
display(segmentation)

#task6
final_df = segmentation.join(
    repeat_customers, "customer_id", "left").fillna({"order_count": 1}) .select("customer_name", "city", "total_spend", "order_count", "segment")
display(final_df)