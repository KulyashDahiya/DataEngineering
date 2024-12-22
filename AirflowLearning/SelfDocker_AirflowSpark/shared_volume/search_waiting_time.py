from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("SearchWaitingTime").getOrCreate()
df = spark.read.format("delta").load("/shared_volume/flight_search")
df = df.withColumn("waiting_time", col("responsed_at").cast("long") - col("searched_at").cast("long"))
df.show()
