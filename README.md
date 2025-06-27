# pyspark-customer-analysis
# 🛒 PySpark Customer Behavior Analysis

This is a beginner-friendly PySpark project that analyzes online retail customer behavior using transactions, customer, and product data.

## 📌 Features
- DataFrame joins and filters
- Total spending per customer
- Top-selling products
- Window functions to rank customers
- Visualizations using Matplotlib

## 🧪 Sample Output

### 💰 Total Spending Per Customer
| customer_id | name    | total_spent |
|-------------|---------|-------------|
| 5           | Eva     | 60500       |
| 2           | Bob     | 63000       |
| 3           | Charlie | 4900        |
| 4           | David   | 8700        |
| 1           | Alice   | 2600        |

### 📦 Top Products by Quantity Sold
| product_name | total_sold |
|--------------|------------|
| Mouse        | 5          |
| Webcam       | 3          |
| Laptop       | 2          |
| Keyboard     | 2          |
| Monitor      | 2          |

---

### 📊 Visual Output

#### 🔹 Top 5 Products by Quantity Sold

![Top Products](images/top_products_chart.png)

#### 🔹 Total Spending by City

![City Spending](images/city_spending_chart.png)

---

## 📂 Files
- `main.py` – Full PySpark code
- `README.md` – Project description with sample output
- `images/` – Folder containing output chart screenshots

---

## ✅ How to Run
1. Use Google Colab or any PySpark environment
2. Run `main.py` in a notebook or Python terminal

---

Made with ❤️ by [Sharath Naik](https://www.linkedin.com/in/sharathnaikbc/)
