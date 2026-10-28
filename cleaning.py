import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, coalesce, lit

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

spark = SparkSession.builder.appName("Q8").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

csv_data = [
    ("TXN001", "C001", "2026-10-01", "Laptop", "1", "75000.0"),
    ("TXN002", "C002", "2026-10-02", "Mouse", "2", "1500.0"),
    ("TXN001", "C001", "2026-10-01", "Laptop", "1", "75000.0"),
    ("TXN003", "C003", None, "Keyboard", "1", "3500.0"),
    ("TXN004", "C004", "2026/10/04", "Monitor", None, "15000.0"),
    ("TXN005", "C005", "2026-10-05", "Headphones", "3", None),
    ("TXN006", "C006", "invalid-date", "USB Cable", "5", "500.0")
]
spark.createDataFrame(csv_data, ["TransactionID", "CustomerID", "TransactionDate", "Product", "Quantity", "UnitPrice"]).write.csv("transactions.csv", header=True, mode="overwrite")

df = spark.read.option("header", "true").csv("transactions.csv")
df.show()

cleaned_df = df.dropDuplicates(["TransactionID"])
cleaned_df.show()

cleaned_df = cleaned_df.withColumn("Quantity", coalesce(col("Quantity").cast("int"), lit(1))) \
                       .withColumn("UnitPrice", coalesce(col("UnitPrice").cast("double"), lit(0.0)))
cleaned_df.show()

cleaned_df = cleaned_df.withColumn("TransactionDate", coalesce(to_date(col("TransactionDate"), "yyyy-MM-dd"), to_date(col("TransactionDate"), "yyyy/MM/dd")))
cleaned_df.show()

cleaned_df = cleaned_df.withColumn("Revenue", col("Quantity") * col("UnitPrice"))
cleaned_df.show()

cleaned_df.groupBy("Product").sum("Revenue").withColumnRenamed("sum(Revenue)", "TotalRevenue").orderBy(col("TotalRevenue").desc()).show()
spark.stop()
