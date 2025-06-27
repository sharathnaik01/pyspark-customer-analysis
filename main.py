from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import matplotlib.pyplot as plt

# Initialize Spark Session
spark = SparkSession.builder.appName("RetailBehaviorAnalysis").getOrCreate()

# -----------------------------
# 1️⃣ Create Customer Data
# -----------------------------
customer_data = [
    (1, "Alice", "Bangalore"),
    (2, "Bob", "Mumbai"),
    (3, "Charlie", "Delhi"),
    (4, "David", "Chennai"),
    (5, "Eva", "Hyderabad"),
]

customer_df = spark.createDataFrame(customer_data, ["customer_id", "name", "city"])

print("✅ Customers:")
customer_df.show()

# -----------------------------
# 2️⃣ Create Product Data
# -----------------------------
product_data = [
    (101, "Keyboard", 1200),
    (102, "Mouse", 700),
    (103, "Monitor", 8000),
    (104, "Laptop", 55000),
    (105, "Webcam", 2500),
]

product_df = spark.createDataFrame(product_data, ["product_id", "product_name", "price"])

print("✅ Products:")
product_df.show()

# -----------------------------
# 3️⃣ Create Transactions Data
# -----------------------------
transactions = [
    (1, 101, "2025-06-01", 1),
    (1, 102, "2025-06-01", 2),
    (2, 104, "2025-06-03", 1),
    (2, 103, "2025-06-04", 1),
    (3, 105, "2025-06-05", 2),
    (3, 101, "2025-06-06", 1),
    (4, 102, "2025-06-06", 1),
    (4, 103, "2025-06-07", 1),
    (5, 104, "2025-06-08", 1),
    (5, 105, "2025-06-09", 1),
    (5, 102, "2025-06-10", 2),
]

transactions_df = spark.createDataFrame(transactions, ["customer_id", "product_id", "date", "quantity"])

print("✅ Transactions:")
transactions_df.show()

# -----------------------------
# 4️⃣ Join DataFrames
# -----------------------------
joined_df = transactions_df \
    .join(customer_df, on="customer_id") \
    .join(product_df, on="product_id")

print("✅ Joined Data:")
joined_df.show()

# -----------------------------
# 5️⃣ Calculate Total Price per Transaction
# -----------------------------
df_with_total = joined_df.withColumn("total_price", col("quantity") * col("price"))

print("✅ With Total Price:")
df_with_total.select("customer_id", "name", "product_name", "quantity", "price", "total_price").show()

# -----------------------------
# 6️⃣ Total Spending Per Customer
# -----------------------------
spending_df = df_with_total.groupBy("customer_id", "name").agg(
    sum("total_price").alias("total_spent")
).orderBy(desc("total_spent"))

print("💰 Total Spending Per Customer:")
spending_df.show()

# -----------------------------
# 7️⃣ Top Products Sold (by quantity)
# -----------------------------
top_products = df_with_total.groupBy("product_name").agg(
    sum("quantity").alias("total_sold")
).orderBy(desc("total_sold"))

print("📦 Top Products by Quantity Sold:")
top_products.show()

# -----------------------------
# 8️⃣ Rank Customers by Spending (Window Function)
# -----------------------------
window_spec = Window.orderBy(desc("total_spent"))
ranked_customers = spending_df.withColumn("rank", dense_rank().over(window_spec))

print("🏆 Ranked Customers:")
ranked_customers.show()

# -----------------------------
# 9️⃣ Visualize Top Products
# -----------------------------
# Convert to Pandas for matplotlib
top_pd = top_products.limit(5).toPandas()

plt.figure(figsize=(8,5))
plt.bar(top_pd['product_name'], top_pd['total_sold'], color='skyblue')
plt.title("Top 5 Products by Quantity Sold")
plt.xlabel("Product")
plt.ylabel("Units Sold")
plt.tight_layout()
plt.show()

# -----------------------------
# 🔟 Visualize Spending by City
# -----------------------------
city_spending = df_with_total.groupBy("city").agg(sum("total_price").alias("city_spend")).orderBy("city_spend", ascending=False)
city_pd = city_spending.toPandas()

plt.figure(figsize=(8,5))
plt.bar(city_pd["city"], city_pd["city_spend"], color='lightgreen')
plt.title("Total Spending by City")
plt.xlabel("City")
plt.ylabel("Amount Spent")
plt.tight_layout()
plt.show()
