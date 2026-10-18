from pyspark.sql.types import StructType, StringType, IntegerType, StructField

event_schema = StructType(
    [
        StructField("event_time", StringType(), True),
        StructField("generator_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("website", StringType(), True),
        StructField("method", StringType(), True),
        StructField("path", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("response_time_ms", IntegerType(), True),
        StructField("bytes_sent", IntegerType(), True),
        StructField("referrer", StringType(), True),
        StructField("user_agent", StringType(), True),
    ]
)
