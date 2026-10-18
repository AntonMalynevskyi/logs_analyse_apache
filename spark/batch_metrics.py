from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import avg, col, desc, lag, to_timestamp

from schema import event_schema

HDFS_LOGS_PATH = "hdfs://namenode:9000/logs"


def completed_log_files(root_path: str) -> list[str]:
    """Return stable HDFS files, excluding Flume's actively written .tmp file."""
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(root_path)
    file_system = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())

    if not file_system.exists(path):
        raise FileNotFoundError(f"HDFS log path does not exist: {root_path}")

    files: list[str] = []

    def visit(directory) -> None:
        for status in file_system.listStatus(directory):
            child = status.getPath()
            if status.isDirectory():
                visit(child)
            elif status.isFile() and not child.getName().endswith(".tmp"):
                files.append(child.toString())

    visit(path)
    if not files:
        raise FileNotFoundError(
            f"No completed HDFS log files found under {root_path}; wait for Flume to roll a file."
        )
    return sorted(files)

spark = SparkSession.builder.appName("all-time-log-metrics").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Ask HDFS for completed file paths before Spark builds its scan. Filtering
# after a directory scan can race with Flume renaming the active .tmp file.
log_files = completed_log_files(HDFS_LOGS_PATH)
events = (
    spark.read.schema(event_schema).json(log_files)
    .filter(col("user_id").isNotNull() & col("website").isNotNull())
    .withColumn("event_ts", to_timestamp("event_time"))
    .filter(col("event_ts").isNotNull())
)

print("\n=== All-time requests by website ===")
events.groupBy("website").count().orderBy(desc("count")).show(truncate=False)

print("\n=== All-time requests by user ===")
events.groupBy("user_id").count().orderBy(desc("count")).show(20, truncate=False)

user_event_order = Window.partitionBy("user_id").orderBy("event_ts")
gaps = (
    events.withColumn("previous_event_ts", lag("event_ts").over(user_event_order))
    .withColumn(
        "gap_seconds",
        col("event_ts").cast("long") - col("previous_event_ts").cast("long"),
    )
    .filter(col("gap_seconds").isNotNull() & (col("gap_seconds") >= 0))
)

print("\n=== Average time between actions by user (seconds) ===")
gaps.groupBy("user_id").agg(avg("gap_seconds").alias("average_gap_seconds")) \
    .orderBy("user_id").show(20, truncate=False)

print("\n=== Overall average time between actions (seconds) ===")
gaps.agg(avg("gap_seconds").alias("average_gap_seconds")).show(truncate=False)

spark.stop()
