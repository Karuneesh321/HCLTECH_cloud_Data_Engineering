import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

spark = SparkSession.builder.appName("Q9").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

customers_data = [(1, "Karunesh", "Bengaluru"), (2, "Amit", "Mumbai"), (3, "Priya", "Delhi"), (4, "Rahul", "Bengaluru"), (5, "Sneha", "Chennai")]
orders_data = [(1001, 1, 1500.00, "2026-10-25"), (1002, 2, 2500.50, "2026-10-26"), (1003, 1, 350.00, "2026-10-27"), (1004, 4, 4200.00, "2026-10-28")]

customers_df = spark.createDataFrame(customers_data, ["CustomerID", "CustomerName", "City"])
orders_df = spark.createDataFrame(orders_data, ["OrderID", "CustomerID", "OrderAmount", "OrderDate"])
customers_df.show()
orders_df.show()

joined_df = orders_df.join(customers_df, on="CustomerID", how="inner")
joined_df.show()

customers_df.join(orders_df, on="CustomerID", how="left_anti").show()

joined_df.groupBy("City").sum("OrderAmount").withColumnRenamed("sum(OrderAmount)", "TotalOrderValue").orderBy(col("TotalOrderValue").desc()).show()
spark.stop()
