# Databricks notebook source
# MAGIC %md
# MAGIC # 📝 Assignment: Python Collections
# MAGIC This notebook contains practice exercises on Python collections like List, Dictionary, Tuple, and Set.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Part 1: List Operations
# MAGIC Given the list of sales amounts:

# COMMAND ----------

sales = [250, 300, 400, 150, 500, 200]

# COMMAND ----------

# MAGIC %md
# MAGIC **Tasks:**
# MAGIC 1. Find the total sales.
# MAGIC 2. Calculate the average sale amount.
# MAGIC 3. Print sale values above 300.
# MAGIC 4. Add `350` to the list.
# MAGIC 5. Sort the list in descending order.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Part 2: Dictionary Operations
# MAGIC Create a dictionary with product names and their prices:

# COMMAND ----------

products = {
    "Laptop": 70000,
    "Mouse": 500,
    "Keyboard": 1500,
    "Monitor": 12000
}

# COMMAND ----------

# MAGIC %md
# MAGIC **Tasks:**
# MAGIC 1. Print the price of the "Monitor".
# MAGIC 2. Add a new product `"Webcam"` with price `3000`.
# MAGIC 3. Update the price of "Mouse" to `550`.
# MAGIC 4. Print all product names and prices using a loop.

# COMMAND ----------

sales = [250, 300, 400, 150, 500, 200]

# COMMAND ----------

total_sales = sum(sales)
print(total_sales)

# COMMAND ----------

average_sale = total_sales / len(sales)
print(average_sale)

# COMMAND ----------

above_300 = [s for s in sales if s > 300]
print(above_300)

# COMMAND ----------

sales.append(350)
print(sales)

# COMMAND ----------

sales_sorted_desc = sorted(sales, reverse=True)
print(sales_sorted_desc)

# COMMAND ----------

products = {
    "Laptop": 70000,
    "Mouse": 500,
    "Keyboard": 1500,
    "Monitor": 12000
}

# COMMAND ----------

print(products.keys())

# COMMAND ----------

print(products.values())

# COMMAND ----------

expensive = max(products, key=products.get)
print(expensive)

# COMMAND ----------

products["Webcam"] = 2500

# COMMAND ----------

products["Mouse"] = 550