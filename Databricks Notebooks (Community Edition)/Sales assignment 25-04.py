# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark lab Data Engineering").getOrCreate()
file_path = '/FileStore/tables/sales_data2/*.csv'
df = spark.read.csv(file_path, header=True, inferSchema=True)
display(df.limit(100))

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
orderSchema = StructType([
     StructField("SalesOrderNumber", StringType()),
     StructField("SalesOrderLineNumber", IntegerType()),
     StructField("OrderDate", DateType()),
     StructField("CustomerName", StringType()),
     StructField("Email", StringType()),
     StructField("Item", StringType()),
     StructField("Quantity", IntegerType()),
     StructField("UnitPrice", FloatType()),
     StructField("Tax", FloatType())
])
df = spark.read.load('/FileStore/tables/sales_data2/*.csv', format='csv', schema=orderSchema)
display(df.limit(100))

# COMMAND ----------

 from pyspark.sql.functions import col
 df = df.dropDuplicates()
 df = df.withColumn('Tax', col('UnitPrice') * 0.08)
 df = df.withColumn('Tax', col('Tax').cast("float"))
 display(df.limit(100))

# COMMAND ----------

customers = df['CustomerName', 'Email']
print(customers.count())
print(customers.distinct().count())
display(customers.distinct())

# COMMAND ----------

customers = df.select("CustomerName", "Email").where(df['Item']=='Road-250 Red, 52')
print(customers.count())
print(customers.distinct().count())
display(customers.distinct())

# COMMAND ----------

productSales = df.select("Item", "Quantity").groupBy("Item").sum()
display(productSales)

# COMMAND ----------

yearlySales = df.select(year("OrderDate").alias("Year")).groupBy("Year").count().orderBy("Year")
display(yearlySales)

# COMMAND ----------

df.createOrReplaceTempView("salesorders")

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC SELECT YEAR(OrderDate) AS OrderYear,
# MAGIC        SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue
# MAGIC FROM salesorders
# MAGIC GROUP BY YEAR(OrderDate)
# MAGIC ORDER BY OrderYear;

# COMMAND ----------

