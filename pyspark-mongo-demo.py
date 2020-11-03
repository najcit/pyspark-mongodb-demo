from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri",
            "mongodb://192.168.10.65:27017/spark.personal_ratings") \
    .config("spark.mongodb.output.uri",
            "mongodb://192.168.10.65:27017/spark.test") \
    .config('spark.jars.packages',
            'org.mongodb.spark:mongo-spark-connector_2.12:3.0.0')\
    .getOrCreate()


# Read from MongoDB
df = spark.read.format("mongo").load()
df.show()

# Use DataFrame
# df.filter(df['rating'] >= 3).write.format("mongo").mode("append").save()
some_fruit = df.filter(df['rating'] >= 3)
some_fruit.show()

# Use SQL
df.createOrReplaceTempView("temp")
some_fruit = spark.sql("SELECT * FROM temp WHERE rating >= 3")
some_fruit.show()
