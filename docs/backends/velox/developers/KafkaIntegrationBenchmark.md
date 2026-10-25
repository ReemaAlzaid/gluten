---
layout: page
title: Kafka Integration Benchmark
nav_order: 4
parent: Developer Overview
---

# Kafka integration benchmark

`org.apache.spark.sql.execution.benchmark.KafkaIntegrationBenchmark` compares Spark
and Gluten across Kafka micro-batch reads, projection, headers fallback, checkpoint
restart, and a finite producer-to-Parquet pipeline. It uses Spark's `Benchmark` helper
and an existing test broker that permits topic creation and deletion.

Use the [Kafka getting started guide in PR #13413](https://github.com/apache/gluten/pull/13413)
for native build flags, broker setup, runtime dependencies, and the streaming example.
The benchmark requires that Kafka integration and a compatible native Velox build with
`ENABLE_KAFKA=ON` are already available. Use Spark 3.5, Scala 2.12, and JDK 17;
set `JAVA_HOME` to that JDK. Install the matching JVM modules before running:

```bash
./build/mvn -B -ntp install \
  -Pbackends-velox -Pspark-3.5 -Pscala-2.12 \
  -Pjava-17 -Pkafka -Pspark-ut -Pfast-build -DskipTests
```

After changing Maven profiles, clean the affected JVM module before compiling to remove
stale component resources. Keep the native library and JVM modules compatible.

## Scenarios

| Scenario | Workload and checks | Timed interval |
| --- | --- | --- |
| `scan` | Read all supported Kafka columns: `key`, `value`, `topic`, `partition`, `offset`, `timestamp`, and `timestampType`. Fixtures include null, empty, and binary keys and values. | Consume previously produced data to `noop`. |
| `projection` | Read pruned and reordered fields, start at offset 1 in each partition, and request `minPartitions=2*partitions`. | Consume the selected offsets to `noop`. |
| `headers` | Produce real headers and retain the `headers` column. Require a Spark Kafka scan even when Gluten is enabled. | Consume to `noop` through the expected fallback. |
| `checkpoint` | Consume `rows` records to Parquet, append another `rows`, and restart with the same checkpoint and output. Verify the complete `2*rows` result. | Only the second consumption, including restart and Parquet commits. |
| `pipeline` | Produce `rows` records with Spark's batch Kafka writer, then consume with `AvailableNow` into Parquet. | Production followed by consumption and Parquet commits. |

The `scan`, `projection`, and `headers` scenarios also run a small, untimed Parquet
replay and compare its records exactly with a Spark batch-read reference. Topic
creation, correctness checks, and cleanup are outside every timed interval. Production
is outside timing except in `pipeline`.

Read scenarios reuse an immutable topic across engines and iterations, with fresh
checkpoints for each run. Pipeline and checkpoint iterations each create their own
topic and output. The checkpoint scenario preserves its checkpoint and output only
between its two runs.
It checks that the query ID persists, the run ID changes, and batch IDs continue from
the previous run. No pre-existing topic data is deleted.

## Run

Run from the repository root. Collect the same JDK access flags used by Gluten's tests:

```bash
KAFKA_BENCHMARK_JAVA_OPTS=$(python3 - <<'PY'
import xml.etree.ElementTree as ET
ns = {"m": "http://maven.apache.org/POM/4.0.0"}
flags = ET.parse("pom.xml").getroot().find("m:properties/m:extraJavaTestArgs", ns)
print(" ".join(" ".join(flags.itertext()).split()))
PY
)
```

Launch a separate JVM through `exec:exec`, so Spark and Kafka threads use the benchmark
JVM rather than Maven's classloader. Put every `-Dspark.*` option inside `exec.args`:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export SPARK_LOCAL_IP=127.0.0.1
set -o pipefail

./build/mvn -B -ntp test-compile exec:exec -pl backends-velox \
  -Pbackends-velox -Pspark-3.5 -Pscala-2.12 \
  -Pjava-17 -Pkafka -Pfast-build \
  -Dexec.executable="$JAVA_HOME/bin/java" -Dexec.classpathScope=test \
  "-Dexec.args=$KAFKA_BENCHMARK_JAVA_OPTS -Xmx4g \
    -Dspark.gluten.benchmark.kafka.scenarios=scan,projection,headers,checkpoint,pipeline \
    -Dspark.gluten.benchmark.kafka.engine=both \
    -Dspark.gluten.benchmark.kafka.rows=100000 \
    -Dspark.gluten.benchmark.kafka.payloadBytes=128 \
    -Dspark.gluten.benchmark.kafka.maxOffsetsPerTrigger=10000 \
    -Dspark.gluten.benchmark.kafka.iterations=3 \
    -cp %classpath org.apache.spark.sql.execution.benchmark.KafkaIntegrationBenchmark" \
  2>&1 | tee /tmp/gluten-kafka-integration-benchmark.log
```

This is a benchmark main class; invoke it with `exec:exec`, not ScalaTest's `-Dsuites`.
For a quick smoke run, use 1,000 rows, 250 offsets per trigger, and one iteration.
Select individual scenarios with a comma-separated list, such as `scan,checkpoint`.

All configuration names below use the prefix `spark.gluten.benchmark.kafka.`:

| Suffix | Default | Meaning |
| --- | --- | --- |
| `bootstrapServers` | `KAFKA_BOOTSTRAP_SERVERS`, or `localhost:9092` | Broker address |
| `scenarios` | `scan,projection,headers,checkpoint,pipeline` | Scenarios to run |
| `engine` | `both` | `spark`, `gluten`, or both cases |
| `rows` | `100000` | Records per dataset; checkpoint produces two datasets |
| `partitions` | `2` | Kafka topic partitions |
| `payloadBytes` | `128` | Size of ordinary value payloads; edge-case fixtures also include null and empty values |
| `maxOffsetsPerTrigger` | `10000` | Kafka offsets per micro-batch |
| `iterations` | `3` | Measured iterations per case, excluding warm-up |
| `timeoutMs` | `120000` | Query completion timeout |

`spark.master` defaults to `local[2]`. The `spark` engine does not automatically load
Gluten. The `both` and `gluten` modes load Gluten and default to 1 GiB off-heap memory.
The query timeout does not impose a deadline on the preceding batch producer job.

## Results and scope

The benchmark checks executed scan plans before accepting timings. Supported Gluten
reads require `MicroBatchScanExecTransformer` and no Spark `MicroBatchScanExec`.
The Spark baseline and headers scenario require the opposite. Checkpoint runs check
both executions. These checks identify the final executed micro-batch of each run.

The `noop` scenarios use committed sink row counts and check batch progress continuity.
Spark's `numInputRows` can be zero for native scans, and the Parquet V1 sink reports
`numOutputRows=-1`. Parquet scenarios therefore validate persisted records outside the
timer. Restart validation checks the exact contents of both datasets, including
duplicate and missing records.

Timings include query startup, scheduling, scanning, row conversion, and checkpoint
commits. Parquet scenarios also include sink work. The pipeline reports throughput for
sequential finite production and consumption; it does not measure live event latency.
Compare repeated runs with the same data size, worker count, broker, and storage.

Native Kafka acceleration covers micro-batch reads. Spark still manages Kafka writes,
offsets, checkpoints, and sink coordination. Native Kafka batch reads and continuous
processing are unsupported. Authentication, broker failures, retention loss, and recovery
under faults need separate correctness tests; this benchmark does not establish those
guarantees. Kafka producer retries can introduce duplicates, which fail exact validation.
