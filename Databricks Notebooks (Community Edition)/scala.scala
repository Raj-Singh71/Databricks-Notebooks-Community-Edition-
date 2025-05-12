// Databricks notebook source

// Converted from Python
val x=1
val y=2
print(x+y)

// COMMAND ----------

// Converted from Python
val a = List(1, 2, 3, 4, 5)
println(a.sum)


// COMMAND ----------

// Converted from Python
val a = List(1, 2, 3, 4, 5)
println(a.sum.toDouble / a.length)


// COMMAND ----------

// Converted from Python
val b = (1, "m", 0, 10.0)
println(b)


// COMMAND ----------

// Converted from Python
val dictt = Map("name" -> "none", "Age" -> 97)
println(dictt)
println(dictt("Age"))
println(dictt.keys)
println(dictt.values)


// COMMAND ----------

// Converted from Python
val c = scala.collection.mutable.Set(1, 2, 3, 4, 5, 6)
c.add(7)
println(c)
c ++= Set(8, 9, 10)
println(c)


// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

val spark = SparkSession.builder.appName("ScalaDF").getOrCreate()

val data = Seq((1, "Alice"), (2, "Bob"), (3, "Carol"))
val df = spark.createDataFrame(data).toDF("id", "name")

// Select specific columns
df.select("name").show()

// Filter rows
df.filter($"id" > 1).show()

// Count rows
println(df.count())

// Describe summary statistics (numeric columns)
df.describe().show()

// Add new column with literal value
val dfWithCountry = df.withColumn("country", lit("India"))
dfWithCountry.show()


// COMMAND ----------

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.appName("SalesData").getOrCreate()

val salesData = Seq(
  ("2024-01-01", "North", "Product A", 10, 200.0),
  ("2024-01-01", "South", "Product B", 5, 300.0),
  ("2024-01-02", "North", "Product A", 20, 400.0),
  ("2024-01-02", "South", "Product B", 10, 600.0),
  ("2024-01-03", "East", "Product C", 15, 375.0)
)

val columns = Seq("date", "region", "product", "quantity", "revenue")
val salesDF = spark.createDataFrame(salesData).toDF(columns: _*)

salesDF.show()


// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder.appName("SalesDataAggregation").getOrCreate()

val salesData = Seq(
  ("2024-01-01", "North", "Product A", 10, 200.0),
  ("2024-01-01", "South", "Product B", 5, 300.0),
  ("2024-01-02", "North", "Product A", 20, 400.0),
  ("2024-01-02", "South", "Product B", 10, 600.0),
  ("2024-01-03", "East", "Product C", 15, 375.0)
)

val columns = Seq("date", "region", "product", "quantity", "revenue")
val salesDF = spark.createDataFrame(salesData).toDF(columns: _*)

// Group by region and sum quantity
salesDF.groupBy("region").agg(sum("quantity").as("total_quantity")).show()


// COMMAND ----------

salesDF.groupBy("region")
  .agg(
    sum("quantity").alias("total_quantity"),
    sum("revenue").alias("total_revenue")
  )
  .show()


// COMMAND ----------



// COMMAND ----------

import org.apache.spark.sql.functions._

salesDF.groupBy("product")
  .agg(
    (avg("revenue") / avg("quantity")).alias("average")
  )
  .show()


// COMMAND ----------

import org.apache.spark.sql.functions._

salesDF.groupBy("region")
  .agg(
    max("revenue").alias("max_revenue")
  )
  .show()


// COMMAND ----------

salesDF.groupBy("region")
  .sum("revenue")
  .orderBy($"sum(revenue)".desc)
  .select("region")
  .show(1)
