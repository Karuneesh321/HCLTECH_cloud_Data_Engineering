import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

spark = SparkSession.builder.appName("Q7").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

data = [
    (101, "Aarav Sharma", "IT", 65000.0, "2023-01-15"),
    (102, "Diya Patel", "HR", 44000.0, "2022-06-20"),
    (103, "Vihaan Gupta", "Finance", 72000.0, "2021-03-12"),
    (104, "Ananya Iyer", "IT", 58000.0, "2024-02-10"),
    (105, "Kabir Singh", "Marketing", 50000.0, "2023-11-01"),
    (106, "Ishaan Reddy", "IT", 90000.0, "2020-05-18"),
    (107, "Meera Nair", "HR", 62000.0, "2021-08-25"),
    (108, "Arjun Verma", "Finance", 85000.0, "2019-12-05"),
    (109, "Sai Rao", "Marketing", 42000.0, "2024-05-14"),
    (110, "Riya Sen", "IT", 61000.0, "2023-07-19"),
    (111, "Rohan Das", "Finance", 55000.0, "2022-10-30"),
    (112, "Neha Joshi", "HR", 48000.0, "2023-04-11"),
    (113, "Aditya Mishra", "Marketing", 67000.0, "2021-11-22"),
    (114, "Kavya Bhat", "IT", 75000.0, "2022-01-08"),
    (115, "Dev Bajwa", "Finance", 95000.0, "2020-09-14")
]

df = spark.createDataFrame(data, ["EmployeeID", "EmployeeName", "Department", "Salary", "JoiningDate"]) \
    .withColumn("EmployeeID", col("EmployeeID").cast("int")) \
    .withColumn("Salary", col("Salary").cast("double")) \
    .withColumn("JoiningDate", col("JoiningDate").cast("date"))

df.printSchema()
df.show(5)

df.filter(col("Salary") > 60000).show()

df_annual = df.withColumn("AnnualSalary", col("Salary") * 12)
df_annual.show(5)

df.groupBy("Department").agg({"Salary": "avg", "*": "count"}) \
  .withColumnRenamed("avg(Salary)", "AverageSalary") \
  .withColumnRenamed("count(1)", "EmployeeCount").show()

df.orderBy(col("Salary").desc()).limit(5).show()
spark.stop()
