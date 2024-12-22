from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.appName("FlightSearchIngestion").getOrCreate()

# Simulating Kafka ingestion
data = [
    (current_timestamp(), current_timestamp(), "web", "NYC", "LAX", "2024-12-25", "2024-12"),
]
schema = [
    "searched_at", "responsed_at", "channel", "origin", "destination", "departure_date", "year_month"
]
df = spark.createDataFrame(data, schema)
df.write.format("delta").mode("append").save("/shared_volume/flight_search")
