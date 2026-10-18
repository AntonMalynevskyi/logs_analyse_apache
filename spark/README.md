# Running Spark jobs

Spark applications in this directory are mounted into both Spark services at
`/opt/spark-apps`. Each service accepts an application filename followed by
arguments for that application.

Run the default continuous Kafka analysis:

```sh
docker compose -f docker/compose.yml up -d spark-streaming
```

This starts the Kafka-to-Spark live analysis and its dependencies. It does not
start Flume, so it does not archive events to HDFS. For the complete
generator-to-HDFS pipeline, including the host-side log generator, follow the
[project quick start](../README.md#run-the-full-pipeline).

Run a different streaming script (and pass it arguments):

```sh
docker compose -f docker/compose.yml run --rm spark-streaming my_stream_job.py --topic logs
```

The streaming service is configured to restart its default application. Stop
that default service first if you want to run a custom streaming job without
both jobs consuming Kafka at once:

```sh
docker compose -f docker/compose.yml stop spark-streaming
docker compose -f docker/compose.yml run --rm spark-streaming my_stream_job.py --topic logs
```

Run the default one-time HDFS analysis:

```sh
docker compose -f docker/compose.yml --profile batch run --rm spark-batch
```

Flume rolls completed HDFS files every 60 seconds in this local demo. The batch
job skips the active `.tmp` file, so run it after at least one file has rolled.

Run a different batch script and pass arguments:

```sh
docker compose -f docker/compose.yml --profile batch run --rm spark-batch my_batch_job.py --input /logs
```

Create custom applications in this directory. The wrapper passes everything
after the script filename through to the Python application. Streaming jobs
receive the Kafka Spark connector; batch jobs use the configured local Spark
master and driver memory. Use an absolute path inside the container only when
the application is stored outside `/opt/spark-apps`.
