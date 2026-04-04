from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,col
from pyspark.sql.functions import avg
from pyspark.sql.functions import count
spark=SparkSession.builder.appName("CustomerSales").getOrCreate()

customers = spark.read.option("header", "true").csv("/samples/customers.csv")
sales = spark.read.option("header", "true").csv("/samples/sales.csv")
customers = customers.dropna(subset=["customer_id"])
sales = sales.dropna(subset=["customer_id"])


#QUERY1 sales.groupBy("customer_id").agg(sum(col("total_amount").cast("int")).alias("totalAmount")).show()
# QUERY2 sales.groupBy("customer_id").agg(sum(col("total_amount").cast("int")).alias("totalamt")).orderBy(col("totalamt").desc()).limit(3).show()
# QUERY3 customers.join(sales, "customer_id", "left").filter(sales.customer_id.isNull()).show()
# QUERY4  customers.join(sales, "customer_id").groupBy("city").agg(sum(col("total_amount").cast("int")).alias("totrevenue")).show()
# QUERY5 sales.groupBy("customer_id").agg(avg(col("total_amount").cast("int")).alias("avg_amount")).show()
# QUERY6 sales.groupBy("customer_id").agg(count("*").alias("orders")).filter(col("orders") > 1).show()
# QUERY7 sales.groupBy("customer_id").agg(sum(col("total_amount").cast("int")).alias("totalamt")).orderBy(col("totalamt").desc()).show()

