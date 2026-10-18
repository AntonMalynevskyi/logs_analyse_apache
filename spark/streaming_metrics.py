from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, from_json, to_timestamp
from schema import event_schema

KAFKA_BOOTSTRAP_SERVERS = "broker:29092"
TOPIC = "logs"
CHECKPOINT_ROOT = "file:///tmp/spark-checkpoints"


def show_top(batch_df, batch_id: int, label: str) -> None:
    print(f"\n=== Live {label} counts (batch {batch_id}) ===")
    batch_df.orderBy(desc("count")).limit(10).show(truncate=False)


spark = SparkSession.builder.appName("live-log-metrics").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

kafka_events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", "10000")
    .load()
)

events = (
    kafka_events.select(from_json(col("value").cast("string"), event_schema).alias("event"))
    .select("event.*")
    .withColumn("event_time", to_timestamp("event_time"))
    .filter(col("user_id").isNotNull() & col("website").isNotNull())
)

website_counts = events.groupBy("website").count()
user_counts = events.groupBy("user_id").count()

website_query = (
    website_counts.writeStream.outputMode("complete")
    .foreachBatch(lambda df, batch_id: show_top(df, batch_id, "website"))
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/websites")
    .trigger(processingTime="10 seconds")
    .start()
)

user_query = (
    user_counts.writeStream.outputMode("complete")
    .foreachBatch(lambda df, batch_id: show_top(df, batch_id, "user"))
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/users")
    .trigger(processingTime="10 seconds")
    .start()
)

spark.streams.awaitAnyTermination()
