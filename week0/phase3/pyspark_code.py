--QUERY1

from pyspark.sql.functions import col, sum
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
options = {"inferschema": True, "header": True}
df = spark.read.format("csv").options(**options).load("/datasets/customers_raw.csv")
df = df.filter((df.customer_id.isNotNull() & df.email.isNotNull()))

customers =spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')
sales = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

# Convert total_amount to numeric
sales_clean = sales.withColumn("total_amount", col("total_amount").cast("double"))

# calculate total sales
daily_sales = sales_clean.groupBy("sale_date") .agg(sum("total_amount").alias("daily_total_sales"))
daily_sales.show()


--QUERY2

from pyspark.sql.functions import col, sum
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
options = {"inferschema": True, "header": True}
df = spark.read.format("csv").options(**options).load("/datasets/customers_raw.csv")
df = df.filter((df.customer_id.isNotNull() & df.email.isNotNull()))

customers =spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')
sales = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

# Convert total_amount to numeric
sales_clean = sales.withColumn("total_amount", col("total_amount").cast("double"))

# Join customers and sales
df = customers.join(sales_clean, "customer_id")

# Group by city and calculate revenue
city_revenue = df.groupBy("city") 
    .agg(sum("total_amount").alias("total_revenue"))

# Show result
city_revenue.show()


--QUERY3

from pyspark.sql import SparkSession
from pyspark.sql.functions import count
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
# Load the customers.csv dataset
customers =spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')
sales = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')
repeat_customers = sales.groupBy("customer_id").agg(count("sale_id").alias("order_count")) .filter("order_count > 2")
repeat_customers.show()


--QUERY4

# Initialize 
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.functions import col, sum
from pyspark.sql.functions import concat_ws

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

# Load  customers.csv dataset
customers =spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')
sales = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')
customer_totals = customers.join(sales, "customer_id") .withColumn("total_amount", col("total_amount")).withColumn("cus_name", concat_ws(" ", col("first_name"), col("last_name")).alias("cus_name")).groupBy( "cus_name", "city") .agg(sum("total_amount").alias("amount")) 
display(customer_totals)

--QUERY5

# Initialize 
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum


spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

#df = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')
customers =spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')
sales = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

final_report = sales .join(customers, "customer_id") .groupBy("customer_id", "first_name", "last_name", "city") .agg(sum("total_amount").alias("total_spend"), count("sale_id").alias("order_count") )

final_report.show()