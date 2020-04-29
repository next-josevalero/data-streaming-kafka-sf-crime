import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# Schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType()),
    StructField("original_crime_type_name", StringType()),
    StructField("report_date", StringType()),
    StructField("call_date", StringType()),
    StructField("offense_date", StringType()),
    StructField("call_time", StringType()),
    StructField("call_date_time", StringType()),
    StructField("disposition", StringType()),
    StructField("address", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("agency_id", StringType()),
    StructField("address_type", StringType()),
    StructField("common_location", StringType())
])

def run_spark_job(spark):

    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "com.udacity.sfcrime.calls") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("maxRatePerPartition", 10) \
        .option("spark.default.parallelism", 600) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("cast(value as string)")
    
    kafka_df.printSchema()

    service_table = kafka_df\
        .select(psf.from_json(psf.col("value"), schema).alias("DF"))\
        .select("DF.*")
    
    service_table.printSchema()

    # Select original_crime_type_name and disposition
    distinct_table = service_table \
        .select(["original_crime_type_name", "disposition"])
    
    distinct_table.printSchema()
    

    # Count the number of original crime type
    agg_df = distinct_table \
        .groupBy("original_crime_type_name") \
        .count()
    
    # Q1. Submit a screen shot of a batch ingestion of the aggregation
    # and write output stream
    query = agg_df \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    # Attach a ProgressReporter
    query.awaitTermination()

    # Get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # Clean up your data so that the column names match on radio_code_df and agg_df
    # We will want to join on the disposition code

    # Rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # Join on disposition column
    join_query = agg_df \
        .writeStream \
        .join(radio_code_df, "disposition") \
        .format("console") \
        .start()

    join_query.awaitTermination()

    
if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
