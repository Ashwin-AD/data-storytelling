from pyspark.sql import SparkSession
from pyspark.sql.functions import length 

spark = SparkSession.builder \
        .master("local[1]") \
        .appName("imdb-dataset-preprocessing") \
        .getOrCreate()

sc = spark.sparkContext

## Load the Dataset
imdb_df = spark.read.options(header='true', inferSchema = 'true') \
        .csv("imdb_dataset.csv")

## Explore the Data 
print(type(imdb_df)) # <class 'pyspark.sql.classic.dataframe.DataFrame'>

imdb_df.printSchema()
# root
# |-- review: string (nullable = true)
# |-- sentiment: string (nullable = true)

imdb_df.show(5)

print(f"Number of rows = {imdb_df.count()}") # 50000

imdb_df = imdb_df.na.drop() # Drop all rows with na

print(f"Number of rows = {imdb_df.count()}") # 49993

## Analyze the Data
print(f"Number of rows = {imdb_df.count()}") # 49993
print(f"Positive reviews = {imdb_df.where(imdb_df.sentiment == 'positive').count()} and Negative reviews = {imdb_df.where(imdb_df.sentiment == 'negative').count()}") # Positive = 14897 and Negative = 13792

## Transform the Data

imdb_df = imdb_df.withColumn("review_length", length(imdb_df.review))
review_more_than_500_chars = imdb_df.filter(imdb_df.review_length > 500)
print(f"Number of rows = {review_more_than_500_chars.count()}") # 33149 

## Save your results
review_more_than_500_chars.write.option("header", True).csv("cleaned_filtered_imdb_dataset.csv") # Makes a new dir called cleaned_filtered_imdb_dataset.csv and inside that is the csv files with a SUCCESS file
