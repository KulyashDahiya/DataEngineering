from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("MySparkApp").getOrCreate()
    data = [("Alice", 34), ("Bob", 36), ("Cathy", 30)]
    df = spark.createDataFrame(data, ["Name", "Age"])
    df.show()

if __name__ == "__main__":
    main()
