from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("NBSearch").getOrCreate()
df = spark.read.format("delta").load("/shared_volume/flight_search")
counts = df.groupBy("channel", "origin", "destination").count()
counts.show()
