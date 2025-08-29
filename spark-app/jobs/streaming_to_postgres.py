import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, to_timestamp, coalesce

# Spark en timezone UTC (cohérent avec Postgres et le 'Z')
spark = (SparkSession.builder
         .appName("orders_stream_to_postgres")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())

bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
topic     = os.getenv("TOPIC", "orders")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", bootstrap)
       .option("subscribe", topic)
       .option("startingOffsets", "earliest")
       .option("failOnDataLoss", "false")
       .load())

schema = StructType([
    StructField("event_id",   StringType(), True),   # on la lira mais on ne l’écrira pas
    StructField("user_id",    IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("price",      DoubleType(), True),
    StructField("ts",         StringType(), True)    # ISO-8601 (string d’abord)
])

# Parse JSON puis conversion TS:
# - format principal: microsecondes + 'Z'  -> SSSSSS + X
# - fallback: sans fraction -> pas de S
json_df = (
    raw.selectExpr("CAST(value AS STRING) AS json")
       .select(from_json(col("json"), schema).alias("data"))
       .select("data.*")
       .withColumn(
           "ts",
           coalesce(
               to_timestamp(col("ts"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX"),
               to_timestamp(col("ts"), "yyyy-MM-dd'T'HH:mm:ssX")
           )
       )
       .filter(col("ts").isNotNull())      # on écarte les timestamps non parsables
)

jdbc_url  = os.getenv("JDBC_URL")
pg_user   = os.getenv("PGUSER")
pg_pass   = os.getenv("PGPASSWORD")

def write_batch(df, batch_id):
    (df
     .drop("event_id")  # IMPORTANT : on laisse Postgres générer l'UUID par défaut
     .write
     .format("jdbc")
     .mode("append")
     .option("url", jdbc_url)
     .option("dbtable", "public.events")
     .option("user", pg_user)
     .option("password", pg_pass)
     .option("driver", "org.postgresql.Driver")
     .save())

query = (json_df
         .writeStream
         .outputMode("append")
         .foreachBatch(write_batch)
         .option("checkpointLocation", "/tmp/checkpoints/orders_to_pg")
         .start())

spark.streams.awaitAnyTermination()
