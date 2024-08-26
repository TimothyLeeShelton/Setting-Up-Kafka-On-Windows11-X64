from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create Spark session
spark = SparkSession.builder \
    .appName("WikipediaChangesConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Define the schema for the incoming JSON data
schema = StructType([
    StructField("title", StringType(), True),
    StructField("type", StringType(), True),
    StructField("user", StringType(), True),
    StructField("bot", StringType(), True),
    StructField("id", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "wikipedia_changes") \
    .load()

# Parse the JSON data
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Process the data (count changes per page)
result_df = parsed_df.groupBy("title").count().orderBy(col("count").desc())

# Write to PostgreSQL
def write_to_postgres(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/wikipedia_changes") \
        .option("dbtable", "page_changes") \
        .option("user", "your_username") \
        .option("password", "your_password") \
        .mode("append") \
        .save()

# Start the streaming query
query = result_df \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_postgres) \
    .start()

query.awaitTermination()