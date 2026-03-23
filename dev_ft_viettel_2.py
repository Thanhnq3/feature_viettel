from pyspark.sql.functions import col, sum as spark_sum, row_number
from pyspark.sql.window import Window

df_daily_sales = df.groupBy("tennant id", "invoice date").agg(
    spark_sum("total_sales").alias("total_sales")
)

# Window to rank days by total_sales per tennant
window_spec = Window.partitionBy("tennant id").orderBy(col("total_sales").desc())

df_ranked = df_daily_sales.withColumn("rank", row_number().over(window_spec))

# Filter top 5 days per tennant
top5_days = df_ranked.filter(col("rank") <= 5)

# Calculate the total sales of the top 5 days per tennant
total_sales_top5 = top5_days.groupBy("tennant id").agg(
    spark_sum("total_sales").alias("total_sales_top5")
)

display(total_sales_top5)

------------

from pyspark.sql.functions import month, year, max as spark_max

# Add year and month columns
df_with_month = df.withColumn("year", year(col("invoice date"))).withColumn("month", month(col("invoice date")))

# Window to get the invoice with highest total_sales per tennant per month
window_month = Window.partitionBy("tennant id", "year", "month").orderBy(col("total_sales").desc())

df_ranked_month = df_with_month.withColumn("rank", row_number().over(window_month))

# Filter to get only the invoice with highest total_sales per tennant per month
top_invoice_per_month = df_ranked_month.filter(col("rank") == 1)

# Calculate total sales of the invoice with highest value for each month of each tennant
result = top_invoice_per_month.select(
    col("tennant id"),
    col("year"),
    col("month"),
    col("total_sales").alias("max_invoice_total_sales")
)

display(result)

---------
from pyspark.sql.functions import sum as spark_sum

# Add year and month columns if not already present
df_with_month = df.withColumn("year", year(col("invoice date"))).withColumn("month", month(col("invoice date")))

# Calculate total sales per month
monthly_sales = df_with_month.groupBy("year", "month").agg(
    spark_sum("total_sales").alias("total_sales")
)

# Find the maximum total sales value
max_total_sales = monthly_sales.agg(spark_max("total_sales").alias("max_total_sales")).collect()[0]["max_total_sales"]

# Filter to get the month(s) with the highest total sales
month_with_max_sales = monthly_sales.filter(col("total_sales") == max_total_sales)

display(month_with_max_sales)

-----------------
from pyspark.sql.functions import countDistinct

# Add year and month columns if not already present
df_with_month = df.withColumn("year", year(col("invoice date"))).withColumn("month", month(col("invoice date")))

# Count distinct buyers per month
buyers_per_month = df_with_month.groupBy("year", "month").agg(
    countDistinct("buyer id").alias("num_buyers")
)

# Find the month with the highest number of buyers
max_buyers = buyers_per_month.agg(spark_max("num_buyers").alias("max_num_buyers")).collect()[0]["max_num_buyers"]

# Filter to get the month(s) with the highest number of buyers
month_with_max_buyers = buyers_per_month.filter(col("num_buyers") == max_buyers)

display(month_with_max_buyers)
----------------


from pyspark.sql.functions import count

# Count transactions per buyer
transactions_per_buyer = df.groupBy("buyer id").agg(count("*").alias("num_transactions"))

# Find the maximum number of transactions by any buyer
max_transactions = transactions_per_buyer.agg(spark_max("num_transactions").alias("max_num_transactions")).collect()[0]["max_num_transactions"]

# Filter buyers with the maximum number of transactions
most_frequent_buyers = transactions_per_buyer.filter(col("num_transactions") == max_transactions)

display(most_frequent_buyers)

-----------------
# Find the minimum number of transactions by any buyer
min_transactions = transactions_per_buyer.agg(spark_min("num_transactions").alias("min_num_transactions")).collect()[0]["min_num_transactions"]

# Filter buyers with the minimum number of transactions
least_frequent_buyers = transactions_per_buyer.filter(col("num_transactions") == min_transactions)

# Calculate the total number of transactions with the least frequent buyers
transactions_with_least_frequent_buyers = df.join(
    least_frequent_buyers.select("buyer id"), on="buyer id", how="inner"
).count()

print("Number of transactions with the least frequent buyers:", transactions_with_least_frequent_buyers)

---------------
from pyspark.sql.functions import sum as spark_sum

# Calculate total discount for the most frequent buyers
total_discount_most_frequent_buyers = df.join(
    most_frequent_buyers.select("buyer id"), on="buyer id", how="inner"
).groupBy("buyer id").agg(
    spark_sum("discount").alias("total_discount")
)

display(total_discount_most_frequent_buyers)

----------------from pyspark.sql.functions import sum as spark_sum

# Calculate total sales per buyer
sales_per_buyer = df.groupBy("buyer id").agg(spark_sum("total_sales").alias("total_sales"))

# Get the top 3 buyers by total sales
top3_buyers = sales_per_buyer.orderBy(col("total_sales").desc()).limit(3)

# Calculate total sales with the top 3 buyers
total_sales_top3_buyers = df.join(
    top3_buyers.select("buyer id"), on="buyer id", how="inner"
).agg(spark_sum("total_sales").alias("total_sales_with_top3_buyers"))

display(total_sales_top3_buyers)

-------------------
from pyspark.sql.functions import countDistinct, lit, date_trunc, sequence, explode, to_date, min as spark_min, max as spark_max

# Get min and max invoice date
date_range = df.agg(
    spark_min("invoice date").alias("min_date"),
    spark_max("invoice date").alias("max_date")
).collect()[0]
min_date = date_range["min_date"]
max_date = date_range["max_date"]

# Generate all dates in the range
all_dates_df = spark.createDataFrame([(min_date, max_date)], ["min_date", "max_date"]) \
    .withColumn("date_seq", sequence(date_trunc("DAY", col("min_date")), date_trunc("DAY", col("max_date")))) \
    .select(explode(col("date_seq")).alias("date"))

# Get distinct selling dates
selling_dates_df = df.select(to_date(col("invoice date")).alias("date")).distinct()

# Find days without selling transactions
days_without_sales = all_dates_df.join(selling_dates_df, on="date", how="left_anti")

# Count number of days without selling transactions
num_days_without_sales = days_without_sales.count()

print("No. days without selling transactions:", num_days_without_sales)


----------------------------

from pyspark.sql.functions import to_date, lag, datediff

# Get distinct selling dates sorted
selling_dates_sorted = df.select(to_date(col("invoice date")).alias("date")).distinct().orderBy("date")

# Calculate the gap in days between consecutive selling dates
selling_dates_with_gap = selling_dates_sorted.withColumn(
    "prev_date", lag("date").over(Window.orderBy("date"))
).withColumn(
    "days_gap", datediff(col("date"), col("prev_date"))
)

# Exclude the first row (where prev_date is null) and get the maximum gap
max_days_gap = selling_dates_with_gap.agg({"days_gap": "max"}).collect()[0]["max(days_gap)"]

print("No. days gap between selling invoices:", max_days_gap)


-------------------------
from pyspark.sql.functions import to_date, lag, datediff

# Get distinct selling dates per buyer sorted
selling_dates_per_buyer = df.select(
    col("buyer id"),
    to_date(col("invoice date")).alias("date")
).distinct().orderBy("buyer id", "date")

# Calculate the gap in days between consecutive selling dates for each buyer
window_buyer = Window.partitionBy("buyer id").orderBy("date")
selling_dates_with_gap = selling_dates_per_buyer.withColumn(
    "prev_date", lag("date").over(window_buyer)
).withColumn(
    "days_gap", datediff(col("date"), col("prev_date"))
)

# Exclude the first row (where prev_date is null) and get the maximum gap per buyer
max_days_gap_per_buyer = selling_dates_with_gap.groupBy("buyer id").agg(
    {"days_gap": "max"}
).withColumnRenamed("max(days_gap)", "max_days_gap")

display(max_days_gap_per_buyer)