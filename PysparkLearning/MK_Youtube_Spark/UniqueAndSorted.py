import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[4]")\
        .appName("UniqueSorted")\
        .getOrCreate()

data= [ (10 ,'Anil',50000, 18),
        (11 ,'Vikas',75000,  16),
        (12 ,'Nisha',40000,  18),
        (13 ,'Nidhi',60000,  17),
        (14 ,'Priya',80000,  18),
        (15 ,'Mohit',45000,  18),
        (16 ,'Rajesh',90000, 10),
        (17 ,'Raman',55000, 16),
        (18 ,'Sam',65000,   17),
        (15 ,'Mohit',45000,  18),
        (13 ,'Nidhi',60000,  17),
        (14 ,'Priya',90000,  18),
        (18 ,'Sam',65000,   17)
        ]

schema = StructType([StructField('id', IntegerType(), True),
                     StructField('name', StringType(), True),
                     StructField('salary', IntegerType(), True),
                     StructField('mngr_id', IntegerType(), True)])

manager_df = spark.createDataFrame(data = data, schema = schema)


#DF and Count
manager_df.show()
print(manager_df.count())
print()

#Distinct DF and Count
manager_df.distinct().show()
print(manager_df.distinct().count())
print()

#Distinct on Two Columns
manager_df.select("id", "name").distinct().show()


# Drop Duplicates
manager_df.drop_duplicates().show()

# New DF with Unique Records
dropped_manager_df = manager_df.drop_duplicates().show()



## SORTING  ##

# manager_df.sort("salary").show()
manager_df.sort(col("salary")).show()
manager_df.sort(col("salary").desc()).show()

manager_df.sort(col("salary").desc(), col("name").desc()).show()

















# input("Press Enter to Exit")



