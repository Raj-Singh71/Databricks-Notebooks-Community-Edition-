# Databricks notebook source
# MAGIC %md
# MAGIC # Spark DataFrames Project Exercise 

# COMMAND ----------

# MAGIC %md
# MAGIC Let's get some quick practice with your new Spark DataFrame skills, you will be asked some basic questions about some stock market data, in this case Walmart Stock from the years 2012-2017. This exercise will just ask a bunch of questions, unlike the future machine learning exercises, which will be a little looser and be in the form of "Consulting Projects", but more on that later!
# MAGIC
# MAGIC For now, just answer the questions and complete the tasks below.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use the walmart_stock.csv file to Answer and complete the  tasks below!

# COMMAND ----------

# MAGIC %md
# MAGIC #### Start a simple Spark Session

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number, mean, max, min, col, year, month

# Start Spark Session
spark = SparkSession.builder.appName("WalmartStock").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the Walmart Stock CSV File, have Spark infer the data types.

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/Walmart_stock.csv", inferSchema=True, header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### What are the column names?

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### What does the Schema look like?

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Print out the first 5 columns.

# COMMAND ----------

df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use describe() to learn about the DataFrame.

# COMMAND ----------

df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Question!
# MAGIC #### There are too many decimal places for mean and stddev in the describe() dataframe. Format the numbers to just show up to two decimal places. Pay careful attention to the datatypes that .describe() returns, we didn't cover how to do this exact formatting, but we covered something very similar. [Check this link for a hint](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column.cast)
# MAGIC
# MAGIC If you get stuck on this, don't worry, just view the solutions.

# COMMAND ----------

summary = df.describe()

# COMMAND ----------

for col_name in summary.columns[1:]:
    summary = summary.withColumn(col_name, format_number(col(col_name).cast("float"), 2))
summary.show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day.

# COMMAND ----------

df = df.withColumn("HV Ratio", col("High") / col("Volume"))
df.select("HV Ratio").show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### What day had the Peak High in Price?

# COMMAND ----------

df.orderBy(col("High").desc()).select("Date", "High").show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is the mean of the Close column?

# COMMAND ----------

df.select(mean("Close")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is the max and min of the Volume column?

# COMMAND ----------

df.select(max("Volume"), min("Volume")).show()

# COMMAND ----------

df.select(max("Volume"), min("Volume")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### How many days was the Close lower than 60 dollars?

# COMMAND ----------

df.filter(col("Close") < 60).count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### What percentage of the time was the High greater than 80 dollars ?
# MAGIC #### In other words, (Number of Days High>80)/(Total Days in the dataset)

# COMMAND ----------

high_over_80 = df.filter(col("High") > 80).count()
total_days = df.count()
percentage = (high_over_80 / total_days) * 100
print(percentage)

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is the Pearson correlation between High and Volume?
# MAGIC #### [Hint](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameStatFunctions.corr)

# COMMAND ----------

from pyspark.sql.functions import corr
df.select(corr("High", "Volume")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is the max High per year?

# COMMAND ----------

df.withColumn("Year", year(df["Date"])).groupBy("Year").max("High").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is the average Close for each Calendar Month?
# MAGIC #### In other words, across all the years, what is the average Close price for Jan,Feb, Mar, etc... Your result will have a value for each of these months. 

# COMMAND ----------

df.withColumn("Month", month(df["Date"])) \
  .groupBy("Month") \
  .avg("Close") \
  .orderBy("Month") \
  .select("Month", "avg(Close)") \
  .show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Great Job!