# Databricks notebook source
# MAGIC %md
# MAGIC # EDA on NYC Taxi Tip Data

# COMMAND ----------

# Load data
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("NYC Taxi Tip EDA").getOrCreate()
df = spark.read.csv("/FileStore/tables/NYC_Taxi_Trip_Record_Clean.csv", header=True, inferSchema=True)
df.cache()

# COMMAND ----------

df.printSchema()

# COMMAND ----------


df.show(5)

# COMMAND ----------


df.count()

# COMMAND ----------

# 5. Get summary statistics (count, mean, stddev, min, max) of numeric columns?

df.describe().show()

# COMMAND ----------

# 6. Calculate average tip amount grouped by passenger count
# TODO: Write the code to answer the above question
df.groupBy("passenger_count").agg({'tip_amount': 'avg'}).withColumnRenamed("avg(tip_amount)", "avg_tip").show()


# COMMAND ----------

# 7. Calculate total tip amount by payment type
# TODO: Write the code to answer the above question
df.groupBy("payment_type").agg({'tip_amount': 'sum'}).withColumnRenamed("sum(tip_amount)", "total_tip").show()


# COMMAND ----------

# 8. Display records where the tip amount is greater than 5
# TODO: Write the code to answer the above question
df.filter(df.tip_amount > 5).show()


# COMMAND ----------

# 9. Identify outliers where tip amount is greater than 50?
# TODO: Write the code to answer the above question
df.filter(df.tip_amount > 50).show()


# COMMAND ----------

# 10. How to calculate the correlation between trip distance and tip amount?
# TODO: Write the code to answer the above question
print("Correlation:", df.stat.corr("trip_distance", "tip_amount"))


# COMMAND ----------

# 11. Get average tip amount by day of the week
# TODO: Write the code to answer the above question
df.groupBy("day_category").agg({'tip_amount': 'avg'}).withColumnRenamed("avg(tip_amount)", "avg_tip").show()


# COMMAND ----------

# 12. Get average tip amount by hour of the day
# TODO: Write the code to answer the above question
from pyspark.sql.functions import hour, to_timestamp
df_with_hour = df.withColumn("hour", hour(to_timestamp("lpep_pickup_datetime", "MM-dd-yyyy H.mm")))
df_with_hour.groupBy("hour").agg({'tip_amount': 'avg'}).withColumnRenamed("avg(tip_amount)", "avg_tip").orderBy("hour").show()


# COMMAND ----------

# 13. Calculate tip amount per mile and describe its statistics
# TODO: Write the code to answer the above question
from pyspark.sql.functions import col
df_with_tip_per_mile = df.withColumn("tip_per_mile", col("tip_amount") / col("trip_distance"))
df_with_tip_per_mile.select("tip_per_mile").describe().show()


# COMMAND ----------

# 14. Get records with invalid fare or tip amounts. Then remove these invalid records from dataframe (make it clean)
# TODO: Write the code to answer the above question
df_invalid = df.filter((col("fare_amount") <= 0) | (col("tip_amount") < 0))
df_invalid.show()
df_clean = df.filter((col("fare_amount") > 0) & (col("tip_amount") >= 0))


# COMMAND ----------

# 15. Calculate average tip by trip time
# TODO: Write the code to answer the above question



# COMMAND ----------

# 16. Get average tip amount per hour. Show graph as visualization
# TODO: Write the code to answer the above question



# COMMAND ----------

# 17. Get average tip by passenger count and display as bar chart
# TODO: Write the code to answer the above question


# COMMAND ----------

# 18. Get fare vs tip values and display scatter plot style visualization
# TODO: Write the code to answer the above question
