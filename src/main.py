from pyspark.sql import SparkSession # type: ignore

from data_loader import *

spark = SparkSession.builder.appName("DataLoading").getOrCreate()

df = load_title_ratings(spark)

print(df.count())
print(df.columns)

df.show(50)
df.printSchema()
