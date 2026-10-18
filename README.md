# logs-analyse

A local log-processing demo. A Python generator sends events to Kafka. Flume archives them in HDFS, while Spark can summarize Kafka events live or analyze completed HDFS files in a batch job.

## Get the project

Clone the repository and move into its directory:

```sh
git clone https://github.com/AntonMalynevskyi/logs_analyse_apache.git
cd logs_analyse_apache
```

## Requirements

- Docker Engine with the Docker Compose plugin
- Python 3.12 or newer and [uv](https://docs.astral.sh/uv/)
- Several gigabytes of free memory for the Hadoop, Kafka, Flume, and Spark containers

The generator runs on the host and connects to Kafka at `localhost:9092`. Spark and Flume run in Docker.

## Run the full pipeline

Run these commands from the project root.

1. Install the generator dependencies:

   ```sh
   uv sync
   ```

2. Start the services used by the end-to-end pipeline:

   ```sh
   docker compose -f docker/compose.yml up -d namenode datanode broker flume spark-streaming
   ```

   Compose waits until the NameNode responds to HDFS and the DataNode and Kafka ports are open before starting Flume and Spark. The YARN services are not needed by these Spark jobs.

3. In a second terminal, change to the project root if needed, then start generating events:

   ```sh
   uv run python generate_logs.py
   ```

   Give Spark a few seconds to initialize before starting the generator. The live query starts at Kafka's latest offset, so events produced before it initializes will not appear in live summaries. The generator runs until you press `Ctrl+C` and sends a small batch about every 20 seconds by default. To send a fixed number per batch, set `BATCH_SIZE`, for example:

   ```sh
   GENERATOR_ID=web-01 BATCH_SIZE=200 uv run python generate_logs.py
   ```

   `GENERATOR_ID` labels each event and is included in generated user IDs. It defaults to `gen-1`; use a different value for each generator if you run more than one. `BATCH_SIZE` defaults to a random batch size between 10 and 1,000 records.

4. In a third terminal, change to the project root if needed and follow live Spark summaries and Flume status:

   ```sh
   docker compose -f docker/compose.yml logs -f spark-streaming flume
   ```

   Spark summaries appear in the `spark-streaming` container logs. The HDFS NameNode page is available at [http://localhost:9870](http://localhost:9870).

   The log-follow command stays attached until you press `Ctrl+C`. This only stops following logs; it does not stop the containers. You can reuse this terminal for the batch command below.

5. Keep the generator running until Flume has rolled at least one HDFS file, then run the batch analysis in the log-view terminal (after pressing `Ctrl+C` there):

   ```sh
   docker compose -f docker/compose.yml --profile batch run --rm spark-batch
   ```

   The local Flume rollover interval is 60 seconds. The batch job skips the active `.tmp` file, so wait for a completed file to appear before running it.

6. Stop the generator with `Ctrl+C` in its terminal, then stop the containers from any terminal in the project root:

   ```sh
   docker compose -f docker/compose.yml down
   ```

   This keeps the named HDFS data volumes. To delete the HDFS data as well, use `docker compose -f docker/compose.yml down -v`. Kafka data is not stored in a named volume and is reset when its container is removed.

## Run Spark without the full pipeline

To run only live Kafka analysis, the Spark service can start with its Kafka and HDFS dependencies:

```sh
docker compose -f docker/compose.yml up -d spark-streaming
```

This does not start Flume, so it does not archive events to HDFS. For batch analysis, start Flume as shown in the full-pipeline steps above. See [spark/README.md](spark/README.md) for custom Spark jobs.

The ResourceManager and NodeManager are optional because the supplied Spark jobs use local Spark masters. Start them only when exploring the YARN services:

```sh
docker compose -f docker/compose.yml --profile yarn up -d resourcemanager nodemanager
```
