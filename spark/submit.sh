#!/bin/sh
set -eu

if [ "$#" -lt 1 ]; then
    echo "Usage: submit.sh <script.py> [script arguments...]" >&2
    exit 2
fi

script="$1"
shift

case "$script" in
    /*) app="$script" ;;
    *) app="/opt/spark-apps/$script" ;;
esac

if [ ! -f "$app" ]; then
    echo "Spark application not found: $app" >&2
    echo "Put the script in the project's spark/ directory, or pass its container path." >&2
    exit 2
fi

case "${SPARK_MODE:-}" in
    streaming)
        exec /opt/spark/bin/spark-submit \
            --master 'local[*]' \
            --conf spark.jars.ivy=/tmp/spark-ivy \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
            "$app" "$@"
        ;;
    batch)
        exec /opt/spark/bin/spark-submit \
            --master 'local[2]' \
            --driver-memory 2g \
            "$app" "$@"
        ;;
    *)
        echo "SPARK_MODE must be set to 'streaming' or 'batch'." >&2
        exit 2
        ;;
esac
