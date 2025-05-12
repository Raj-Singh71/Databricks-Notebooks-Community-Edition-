# Databricks notebook source
# Load the dataset from DBFS (Databricks Filesystem)
file_path = "/databricks-datasets/samples/population-vs-price/data_geo.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)
df.printSchema()
df.show(5)

# COMMAND ----------

# Select relevant columns for analysis
df_selected = df.select(
    "City", "State", "State Code", 
    "2014 Population estimate", "2015 median sales price"
)
df_selected.show(5)

# COMMAND ----------

#top 10 most populated cities
df_selected.orderBy("2014 Population estimate", ascending=False).show(10)

# COMMAND ----------

display(df_selected.select("City","2014 Population estimate").orderBy("2014 Population estimate", ascending=False))

# COMMAND ----------

#Average median sales price by State
from pyspark.sql.functions import avg

df.groupBy("State").agg(
    avg("2015 median sales price").alias("avg_median_sales_price")
).orderBy("avg_median_sales_price", ascending=False).show()


# COMMAND ----------

#cities with population>1 million
display(df_selected.select("City","2014 Population estimate").filter(df_selected["2014 Population estimate"]>1000000))

# COMMAND ----------

#Scenario 4: Identify cities with missing or zero price values
from pyspark.sql.functions import col
df_null = df_selected.filter(col("2015 median sales price").isNull() | (col("2015 median sales price"))==0)
df_null.show()
