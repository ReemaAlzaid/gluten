# Gluten Substrait GPU Executor

Run **Spark-Connect clients on Gluten + Velox (GPU via cuDF)** by combining two pieces:

```
PySpark / Spark-Connect client
        │  Spark Connect protocol
        ▼
 spark-substrait-gateway        (front end: Spark Connect → Substrait IR)
        │  Substrait plan over ADBC Flight SQL  (grpc://localhost:50052)
        ▼
 THIS SERVER  (Arrow Flight SQL)  ── Substrait → Spark DataFrame ──▶ Spark 3.5 + Gluten + Velox-cuDF
        │  Arrow results
        ▼
 gateway → client
```

It's the "Substrait + Gluten + Velox" stack: the gateway speaks Substrait, this server turns that into a
Spark plan, and Gluten offloads it to **Velox-cuDF on the GPU**.

## Repo layout (this module)
```
tools/substrait-gpu-executor/
├── pom.xml                         # builds the fat jar (Spark provided; relocates guava/arrow/...)
├── README.md                       # this file
├── src/main/scala/.../gpu/
│   ├── SubstraitGpuExecutor.scala   # Flight SQL server: Substrait bytes → DataFrame → Arrow
│   ├── SubstraitNativeTranslator.scala  # native Substrait→DataFrame for file-backed plans
│   └── ArrowSupport.scala               # Spark schema/rows → Arrow
├── src/main/scala/org/apache/spark/sql/
│   └── GlutenLogicalPlanBridge.scala    # reaches package-private Dataset.ofRows
├── scripts/
│   └── run.sh                      # ONE entry point: build | server | gateway | demo | gpu-proof | stop
└── gateway/
    ├── gluten_flightsql_backend.py # the new gateway backend (copy into the gateway repo)
    └── GATEWAY_CHANGES.md           # all edits to make in the spark-substrait-gateway repo
```

## Before / after (one script)
The PoC used to be **six loose files**; it's now **one `run.sh`** with subcommands.

```
  BEFORE  (scripts/)                          AFTER  (scripts/)
  ──────────────────────────────             ──────────────────────────
  build.sh ───────────────────┐
  run-server.sh ──────────────┤
  run-gateway.sh ─────────────┼───────────▶   run.sh  <command>
  client-demo.py ─────────────┤
  gpu-standalone-demo.sh ─────┘
  gen-testdata.py ──────────── (deleted; use /home/vpcuser/benchmarks)
```

| Before (file) | After (command) |
|---|---|
| `build.sh` | `./run.sh build` |
| `run-server.sh` | `./run.sh server` |
| `run-gateway.sh` | `./run.sh gateway` |
| `client-demo.py` | `./run.sh demo` |
| `gpu-standalone-demo.sh` | `./run.sh gpu-proof` |
| `gen-testdata.py` | *(deleted — uses real TPC-H data in `/home/vpcuser/benchmarks`)* |
| — | `./run.sh stop` *(new — kills server + gateway safely)* |

Same flags, same behavior — just one file and one verb each. All paths/ports stay overridable by env.

## Prerequisites
- A **Gluten Velox bundle built with the GPU/cuDF backend**: `package/target/gluten-velox-bundle-spark3.5_2.12-linux_amd64-1.7.0-SNAPSHOT.jar` (built from the `gala/main` velox at `/home/vpcuser/velox`).
- **Spark 3.5.5** dist (`$SPARK_HOME`, default `/home/vpcuser/spark-3.5.5-bin-hadoop3`).
- NVIDIA GPU + CUDA 12.x; cuDF/RMM libs in the velox build tree (`_build/release/_deps/`).
- The gateway repo + a Python 3.11 venv — see [gateway/GATEWAY_CHANGES.md](gateway/GATEWAY_CHANGES.md).

## How to run (end-to-end)
Everything is one script — `scripts/run.sh <command>`:

```bash
cd <gluten>/tools/substrait-gpu-executor/scripts

./run.sh build          # 1. build the server jar (once)
./run.sh server         # 2. terminal 1 — GPU server; wait for "Gluten plugin active: true"
./run.sh gateway        # 3. terminal 2 — the Spark-Connect gateway
./run.sh demo           # 4. terminal 3 — PySpark client, end-to-end
```
Data defaults to the real **TPC-H SF10** set at `/home/vpcuser/benchmarks/tpch/data/lineitem`
(~60M rows). Override anything via env: `DATA=/path CUDF=false ./run.sh gpu-proof`.

> Restart the gateway whenever you restart the server — the gateway caches one backend connection.
> Stop both with `./run.sh stop` (kills by Java main-class + `gateway.server`; never by a script
> name, which would match — and kill — the launcher itself).

## Standalone GPU proof (no gateway)
To see Gluten actually offload to the GPU today:
```bash
nvidia-smi dmon -s um        # terminal A: watch the GPU
./run.sh gpu-proof           # terminal B: TPC-H groupBy on the GPU, prints the plan
```
Expect `CudfColumnarExchange [shuffle_writer_type=gpu_hash]` in the printed plan and `sm%` moving.
(Verified 2026-06-13: TPC-H SF10 `lineitem`, 60M-row filter+groupBy.count, `sm≈27%`, correct
results, on 2× L40S — plan: `FileSourceScanExecTransformer → FilterExecTransformer →
HashAggregateTransformer → CudfColumnarExchange[gpu_hash] → SortExecTransformer`.)

## Status
| Path | State |
|---|---|
| Worker: Gluten + Velox-cuDF GPU offload (standalone) | ✅ verified (`CudfColumnarExchange`, sm≈20%) |
| Gateway → server → Gluten, **inline data** | ✅ works (CPU Velox) |
| Gateway → server, **parquet read** | ✅ works (native translator) |
| Gateway → server → GPU, **parquet filter/aggregate** | ⚠️ blocked — see below |

**Known blocker:** a Parquet filter/aggregate routed *through the gateway* fails with `unread block data`
at Spark's `TaskResultGetter` (task-result deserialization) in this embedded-Flight-server setup. The same
query built natively (and the standalone GPU proof) runs fine — so it's the "embed Spark behind a Flight
server and collect file-scan task results" mechanism, not Gluten/Velox/cuDF. `SubstraitNativeTranslator`
bypasses `io.substrait:spark` (which mis-builds the FileScan) but the deserialization issue persists.

**Two clean ways forward (pick a lane):**
1. **Spark Connect + Gluten directly** — drop this separate server; clients hit a Spark Connect server with
   the Gluten plugin; Substrait stays internal to Gluten; GPU is the proven standalone path. Lowest risk.
2. **Substrait-native GPU engine** behind the gateway (e.g. Sirius) — the differentiated "Spark-less" bet;
   avoids bouncing Substrait back through Spark entirely.

## Build notes (why the pom relocates things)
The fat jar relocates `com.google.common` (Spark ships old Guava), `org.apache.arrow` / `com.fasterxml.jackson`
/ flatbuffers / eclipse-collections / snakeyaml (clash with Spark/Hadoop on the classpath), and
`io.substrait` + `com.google.protobuf` (Gluten ships its own `io.substrait.proto` for Velox). `io.netty` is
**not** relocated (breaks the Flight gRPC transport). Java 17 needs `-Dio.netty.tryReflectionSetAccessible=true`
+ `--add-opens java.base/java.nio` for Gluten's columnar shuffle (in `run.sh`).
