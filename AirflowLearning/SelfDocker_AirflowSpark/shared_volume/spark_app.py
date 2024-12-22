from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ExampleSparkApp").getOrCreate()

    data = [(1, "Alice"), (2, "Bob"), (3, "Cathy")]
    df = spark.createDataFrame(data, ["ID", "Name"])
    df.show()

    spark.stop()
