import time

import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize findspark and SparkSession
findspark.init()

spark = SparkSession.builder.master("local[12]")\
        .appName("testing")\
        .getOrCreate()

# Customer Data
customer_data = [
    (1, 'manish', 'patna', "30-05-2022"),
    (2, 'vikash', 'kolkata', "12-03-2023"),
    (3, 'nikita', 'delhi', "25-06-2023"),
    (4, 'rahul', 'ranchi', "24-03-2023"),
    (5, 'mahesh', 'jaipur', "22-03-2023"),
    (6, 'prantosh', 'kolkata', "18-10-2022"),
    (7, 'raman', 'patna', "30-12-2022"),
    (8, 'prakash', 'ranchi', "24-02-2023"),
    (9, 'ragini', 'kolkata', "03-03-2023"),
    (10, 'raushan', 'jaipur', "05-02-2023")
]

# Define Customer Schema
customer_schema = StructType([
    StructField('customer_id', IntegerType(), True),
    StructField('customer_name', StringType(), True),
    StructField('customer_address', StringType(), True),
    StructField('DateOfJoining', StringType(), True),
])

# Create Customer DataFrame
customer_df = spark.createDataFrame(customer_data, customer_schema)

# Convert DateOfJoining to DateType
# customer_df = customer_df.withColumn("DateOfJoining", to_date(col("DateOfJoining"), "dd-MM-yyyy"))

# Show Customer DataFrame
print("Customer DataFrame:")
customer_df.show()

# Sales Data
sales_data = [
    (1, 22, 10, "01-06-2022"),
    (1, 27, 5, "03-02-2023"),
    (2, 5, 3, "01-06-2023"),
    (5, 22, 1, "22-03-2023"),
    (7, 22, 4, "03-02-2023"),
    (9, 5, 6, "03-03-2023"),
    (2, 1, 12, "15-06-2023"),
    (1, 56, 2, "25-06-2023"),
    (5, 12, 5, "15-04-2023"),
    (11, 12, 76, "12-03-2023")
]

# Define Sales Schema
sales_schema = StructType([
    StructField('customer_id', IntegerType(), True),
    StructField('product_id', IntegerType(), True),
    StructField('quantity', IntegerType(), True),
    StructField('sale_date', StringType(), True),
])

# Create Sales DataFrame
sales_df = spark.createDataFrame(sales_data, sales_schema)

# Convert sale_date to DateType
# sales_df = sales_df.withColumn("sale_date", to_date(col("sale_date"), "dd-MM-yyyy"))

# Show Sales DataFrame
print("Sales DataFrame:")
sales_df.show()


# sortmergedf = customer_df.join(sales_df, customer_df.customer_id == sales_df.customer_id, "inner")
#
# sortmergedf.show()
#
# # sortmergedf.printSchema()
#
# sortmergedf.explain()


broadcast_df = customer_df.join(broadcast(sales_df), customer_df.customer_id == sales_df.customer_id, "inner")

broadcast_df.show()

broadcast_df.explain()


input("Press enter to terminate")



















