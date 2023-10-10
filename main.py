import argparse

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import LongType, StructField, StructType, StringType, DoubleType, FloatType

parser = argparse.ArgumentParser()
parser.add_argument("--lake_bucket", help="bucket where lake is located.")
parser.add_argument("--psub_subscript_id", help="PUB/SUB lite subscription id")
parser.add_argument("--checkpoint_location", help="Checkpoint location.")

args = parser.parse_args()
lake_bucket = args.lake_bucket
psub_subscript_id = args.psub_subscript_id
checkpoint_location = args.checkpoint_location
print("--------------------------------------------------------------------------")
print("spark.sql.warehouse.dir = {} ".format(lake_bucket))
print("psub_subscript_id = {}".format(psub_subscript_id))
print("checkpoint location = {} ".format(checkpoint_location))

conf = (
    SparkConf()
    .setAppName('injest-trips-iceberg-table')
    .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .set('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')
    .set('spark.sql.catalog.spark_catalog.type', 'hive')
    .set(f'spark.sql.catalog.dev', 'org.apache.iceberg.spark.SparkCatalog')
    .set(f'spark.sql.catalog.dev.type', 'hive')
    .set(f'spark.sql.warehouse.dir', lake_bucket)
)
spark = SparkSession.builder.enableHiveSupport().config(conf=conf).getOrCreate()

# Create table if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS dev.lakehouse")

schema = StructType([
    StructField("vendor_id", LongType(), True),
    StructField("trip_id", LongType(), True),
    StructField("trip_distance", FloatType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True)
])

# Converting Pubsub DF to a iceberg DF
df = spark.createDataFrame([], schema)
df.writeTo("dev.lakehouse.trips").partitionedBy("vendor_id").createOrReplace()

# Subscribe to GCP PubSub lite.
sdf = (
    spark.readStream.format("pubsublite")
    .option(
        "pubsublite.subscription",
        psub_subscript_id,
    )
    .load()
)

# Converting Column data
sdf = sdf.withColumn("data", sdf.data.cast(StringType()))
sdf = sdf.withColumn("data", from_json("data", schema)).select(col('data.*'))


query = (sdf
         .writeStream
         .format("iceberg")
         .trigger(processingTime="30 seconds")
         .option("path", "dev.lakehouse.trips")
         .option("checkpointLocation", checkpoint_location)
         .start()
         )

query.awaitTermination()
query.stop()
