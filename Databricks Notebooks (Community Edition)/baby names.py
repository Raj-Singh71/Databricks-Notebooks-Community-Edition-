# Databricks notebook source
catalog = "catalog"
schema = "dbo"
volume = "vol1"
download_url = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv"
file_name = "rows.csv"
table_name = "babynames"
path_volume = "/Volumes/" + catalog + "/" + schema + "/" + volume
path_table = catalog + "." + schema
print(path_table) # Show the complete path
print(path_volume) # Show the complete path

# COMMAND ----------

dbutils.fs.cp(f"{download_url}", f"{path_volume}/{file_name}")

# COMMAND ----------

data = [[2021, "test", "Albany", "M", 42]]
columns = ["Year", "First_Name", "County", "Sex", "Count"]

df1 = spark.createDataFrame(data, schema="Year int, First_Name STRING, County STRING, Sex STRING, Count int")
display(df1) # The display() method is specific to Databricks notebooks and provides a richer visualization.
# df1.show() The show() method is a part of the Apache Spark DataFrame API and provides basic visualization.

# COMMAND ----------

df_csv = spark.read.csv(f"{path_volume}/{file_name}",
                        header=True,
                        inferSchema=True,
                        sep=",")
display(df_csv)


# COMMAND ----------

df_csv.printSchema()
df1.printSchema()

# COMMAND ----------

df_csv = df_csv.withColumnRenamed("First Name", "First_Name")
df_csv.printSchema

# COMMAND ----------

df = df1.union(df_csv)
display(df)
df.printSchema

# COMMAND ----------

# MAGIC %md
# MAGIC # Get baby names > 50

# COMMAND ----------

display(df.filter(df["Count"] > 50))

# COMMAND ----------

display(df.where(df["Count"] > 50))

# COMMAND ----------

# MAGIC %md
# MAGIC # Display baby first name and count in descending order

# COMMAND ----------

from pyspark.sql.functions import desc
display(df.select("First_Name", "Count").orderBy(desc("Count")))

# COMMAND ----------

# MAGIC %md
# MAGIC # Display first name, county, count for year 2009, count > 100, Gender = Female, in desceinding order of count

# COMMAND ----------

subsetDF = df.filter((df["Year"] == 2009) & (df["Count"] > 100) & (df["Sex"] == "F")).select("First_Name", "County", "Count").orderBy(desc("Count"))
display(subsetDF)





# COMMAND ----------

df.write.mode("overwrite").saveAsTable(f"{path_table}.{table_name}")

# COMMAND ----------

df.write.format("json").mode("overwrite").save("/tmp/json_data")


# COMMAND ----------

display(spark.read.format("json").json("/tmp/json_data"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Display count and county name in upper case

# COMMAND ----------

display(df.selectExpr("Count", "upper(County) as big_name"))


# COMMAND ----------

# MAGIC %md
# MAGIC # Display count and county name in lower case

# COMMAND ----------

from pyspark.sql.functions import expr
display(df.select("Count", expr("lower(County) as little_name")))


# COMMAND ----------

display(spark.sql(f"SELECT * FROM {path_table}.{table_name}"))
