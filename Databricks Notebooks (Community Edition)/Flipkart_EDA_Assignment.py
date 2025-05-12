# Databricks notebook source
# MAGIC %md
# MAGIC # Flipkart EDA assignment
# MAGIC
# MAGIC ## TODO: Upload csv file before moving to next

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Flipkart Data Engineering").getOrCreate()
file_path = '/FileStore/tables/Flipkart.csv'
flipkart_df = spark.read.csv(file_path, header=True, inferSchema=True)

# COMMAND ----------

flipkart_df.printSchema()
flipkart_df.count()

# COMMAND ----------

flipkart_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

#Display all category names
flipkart_df.select("maincateg").distinct().show()


# COMMAND ----------

# Filter products with rating > 4 and more than 1000 reviews
from pyspark.sql.functions import col

df_filtered = flipkart_df.filter(
    (col("Rating") > 4) & (col("noreviews1") > 1000)
)

df_filtered.show()


# COMMAND ----------

# Display Products in 'Men' category that are fulfilled
from pyspark.sql.functions import col

df_filtered = flipkart_df.filter(
    (col("maincateg") == "Men") & (col("fulfilled1") == 1)
)

df_filtered.show()


# COMMAND ----------

# Dsiplay number of products per category
flipkart_df.groupBy("maincateg").count().show()


# COMMAND ----------

# Display Average rating per category
from pyspark.sql.functions import avg

flipkart_df.groupBy("maincateg").agg(avg("Rating").alias("avg_rating")).show()



# COMMAND ----------

# Dsiplay Category with highest average number of reviews
flipkart_df.groupBy("maincateg").agg(avg("noreviews1").alias("avg_reviews")).orderBy("avg_reviews", ascending=False).show(1)


# COMMAND ----------

# Top 5 products with highest price. display product name and price
flipkart_df.select("title", "actprice1").orderBy("actprice1", ascending=False).show(5)


# COMMAND ----------

# Display Min, max, and avg price per category
from pyspark.sql.functions import min, max, avg
flipkart_df.groupBy("maincateg").agg(min("actprice1").alias("min_price"), max("actprice1").alias("max_price"), avg("actprice1").alias("avg_price")).show()


# COMMAND ----------

# Display number of nulls in each column
from pyspark.sql.functions import col, sum
flipkart_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in flipkart_df.columns]).show()

# COMMAND ----------

# calculate and display the category name, number of fulfilled, and unfulfilled products
flipkart_df.groupBy("maincateg").agg(
    sum((flipkart_df["fulfilled1"] == 1).cast("int")).alias("fulfilled_count"),
    sum((flipkart_df["fulfilled1"] == 0).cast("int")).alias("unfulfilled_count")
).show()



# COMMAND ----------

# Display Count of products per category
flipkart_df.groupBy("maincateg").count().show()


# COMMAND ----------

# Display Average rating per category
flipkart_df.groupBy("maincateg").agg(avg("Rating").alias("avg_rating")).show()


# COMMAND ----------

# Display Category with highest average number of reviews
flipkart_df.groupBy("maincateg").agg(avg("noreviews1").alias("avg_reviews")).orderBy("avg_reviews", ascending=False).show(1)


# COMMAND ----------

# Display Bar chart of product count per category
display(flipkart_df.groupBy("maincateg").count().withColumnRenamed("count", "product_count"))

# Use Databricks UI to visualize this as a bar chart


# COMMAND ----------

# Bar chart of average rating per category
display(flipkart_df.groupBy("maincateg").agg(avg("Rating").alias("average_rating")))


# COMMAND ----------

# Display Bar chart of total number of reviews per category
display(flipkart_df.groupBy("maincateg").agg(avg("Rating").alias("average_rating")))

# COMMAND ----------

# Display product name and 5 star rating for those products which have highest 5 star rating
flipkart_df.orderBy(flipkart_df["star_5f"], ascending=False).select("title", "star_5f").show(1)
