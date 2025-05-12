# Databricks notebook source
x = 1 
y = 4
print(x+y)

# COMMAND ----------

list = [1,2,3,4,5]
print(sum(list))

# COMMAND ----------

list = [1,2,3,4,5]
print(sum(list)/len(list))

# COMMAND ----------

tuple = (10,20,30)
print(tuple)

# COMMAND ----------

dict = {"Name":"Raj", "Age":"23"}
print(dict["Name"])
dict["Age"]=24
print(dict.keys())
print(dict.values())


# COMMAND ----------

set = {1,2,3,4,5}
set.add(6)
print(set)
set.update([2,7,8])
print(set)

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PythonDF").getOrCreate()
data = [(1,"Alice"),(2,"Bob"),(3,"Carol")]
df = spark.createDataFrame(data,["id","name"])
df.show()


# COMMAND ----------

# Select specific columns
df.select("name").show()

# Filter rows
df.filter(df["id"] > 1).show()

# Count rows
print(df.count())

# Describe summary statistics (numeric columns)
df.describe().show()

# Add new column with literal value
from pyspark.sql.functions import lit
df = df.withColumn("country", lit("India"))
df.show()

# COMMAND ----------

sales_data = [
    ("2024-01-01", "North", "Product A", 10, 200.0),
    ("2024-01-01", "South", "Product B", 5, 300.0),
    ("2024-01-02", "North", "Product A", 20, 400.0),
    ("2024-01-02", "South", "Product B", 10, 600.0),
    ("2024-01-03", "East",  "Product C", 15, 375.0),
]
columns = ["date", "region", "product", "quantity", "revenue"]
sales_df = spark.createDataFrame(sales_data, columns)
sales_df.show()

# COMMAND ----------

display(sales_df.groupBy("product").agg(sum("revenue").alias("total_revenue")))

# COMMAND ----------

from pyspark.sql.functions import sum
sales_df.groupBy("product").agg(sum("revenue").alias("total_revenue")).show()



# COMMAND ----------

sales_df.groupBy("region").agg(sum("quantity").alias("total_quantity"),sum("revenue").alias("total_revenue")).show()

# COMMAND ----------

from pyspark.sql.functions import avg
sales_df.groupBy("product").agg(avg("revenue").alias("avg_revenue")).show()

# COMMAND ----------

from pyspark.sql.functions import sum, col
sales_df.groupBy("product").agg((sum("revenue") / sum("quantity")).alias("avg_rev_per_unit")).show()

# COMMAND ----------

#average revenue per region
from pyspark.sql.functions import sum, col
sales_df.groupBy("region").agg((sum("revenue") / sum("quantity")).alias("avg_rev_per_unit")).show()

# COMMAND ----------

