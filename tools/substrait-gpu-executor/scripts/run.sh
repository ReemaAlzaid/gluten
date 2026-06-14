#!/usr/bin/env bash
# ============================================================================
#  Gluten Substrait GPU Executor — single entry point for the whole PoC.
#
#  One script, a few subcommands (was: build.sh + run-server.sh + run-gateway.sh
#  + gpu-standalone-demo.sh + client-demo.py + gen-testdata.py).
#
#    ./run.sh build       compile the server fat jar
#    ./run.sh server      start the Flight SQL server (:50052)  [Gluten + Velox-cuDF GPU]
#    ./run.sh gateway     start the spark-substrait-gateway (:50051, GLUTEN backend)
#    ./run.sh demo        end-to-end PySpark client demo (needs server + gateway up)
#    ./run.sh gpu-proof   standalone GPU proof (no gateway) — prints the Cudf* plan
#    ./run.sh tpch        standalone TPC-H on Gluten+Velox-cuDF; QUERIES="1 6"|"all", CUDF=true|false
#    ./run.sh stop        kill the server and the gateway
#
#  Typical run (three terminals):
#    ./run.sh build                       # once
#    ./run.sh server                      # terminal 1  (wait for "Gluten plugin active: true")
#    ./run.sh gateway                     # terminal 2
#    ./run.sh demo                        # terminal 3
#  Or just prove the GPU offload with no gateway:  ./run.sh gpu-proof
#
#  Env overrides (all have sane defaults):
#    SPARK_HOME  VELOX_HOME  GLUTEN_HOME  DATA  CUDF  GLUTEN
#    SERVER_PORT  GATEWAY_PORT  GATEWAY_DIR  VENV
# ============================================================================
set -euo pipefail

# --- locate the repo from this script: <gluten>/tools/substrait-gpu-executor/scripts/ ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
GLUTEN_HOME="${GLUTEN_HOME:-$(cd "$MODULE_DIR/../.." && pwd)}"

# --- config (override via env) ---
export SPARK_HOME="${SPARK_HOME:-/home/vpcuser/spark-3.5.5-bin-hadoop3}"
VELOX_HOME="${VELOX_HOME:-/home/vpcuser/velox}"
DATA="${DATA:-/home/vpcuser/benchmarks/tpch/data/lineitem}"   # demo/gpu-proof: one table (~60M rows)
TPCH_DATA="${TPCH_DATA:-/home/vpcuser/benchmarks/tpch/data}"  # tpch: all 8 tables (SF10)
TPCH_QUERIES="${TPCH_QUERIES:-/home/vpcuser/benchmarks/velox-testing/common/testing/queries/tpch/queries.json}"
CUDF="${CUDF:-true}"          # GPU on/off (Velox-cuDF)
GLUTEN="${GLUTEN:-true}"      # plugin on/off (false = plain Spark, for debugging)
SERVER_PORT="${SERVER_PORT:-50052}"
GATEWAY_PORT="${GATEWAY_PORT:-50051}"
GATEWAY_DIR="${GATEWAY_DIR:-/home/vpcuser/spark-substrait-gateway}"
VENV="${VENV:-/home/vpcuser/ssg-venv}"

BUNDLE_JAR="$GLUTEN_HOME/package/target/gluten-velox-bundle-spark3.5_2.12-linux_amd64-1.7.0-SNAPSHOT.jar"
SERVER_JAR="$MODULE_DIR/target/substrait-gpu-executor-1.0.0-SNAPSHOT.jar"
MAIN_CLASS="org.apache.gluten.substrait.gpu.SubstraitGpuExecutor"

# GPU shared libs that libvelox.so (cuDF backend) needs at load/runtime.
gpu_libs() {
  export LD_LIBRARY_PATH="\
$VELOX_HOME/_build/release/_deps/cudf-build:\
$VELOX_HOME/_build/release/_deps/nvcomp_proprietary_binary-src/lib64:\
/usr/local/cuda/targets/x86_64-linux/lib:\
/usr/local/lib:/usr/local/lib64:${LD_LIBRARY_PATH:-}"
}

# Java 17 needs these for Gluten's columnar shuffle (off-heap DirectByteBuffer).
JAVAOPTS="-Dio.netty.tryReflectionSetAccessible=true \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/java.lang=ALL-UNNAMED"

# Gluten + Velox-cuDF Spark confs (empty if GLUTEN=false). Echoed one-per-line.
gluten_conf() {
  [ "$GLUTEN" = "true" ] || return 0
  printf '%s\n' \
    --conf "spark.plugins=org.apache.gluten.GlutenPlugin" \
    --conf "spark.gluten.sql.columnar.backend.lib=velox" \
    --conf "spark.gluten.sql.columnar.cudf=$CUDF" \
    --conf "spark.gluten.sql.columnar.backend.velox.cudf.enableTableScan=$CUDF" \
    --conf "spark.memory.offHeap.enabled=true" \
    --conf "spark.memory.offHeap.size=8g" \
    --conf "spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager"
}

# ---------------------------------------------------------------------------
cmd_build() {
  cd "$GLUTEN_HOME"
  ./build/mvn -f tools/substrait-gpu-executor/pom.xml clean package -DskipTests
  echo "Built: $SERVER_JAR"
}

cmd_server() {
  [ -f "$BUNDLE_JAR" ] || { echo "Missing Gluten bundle: $BUNDLE_JAR (build Gluten first)"; exit 1; }
  [ -f "$SERVER_JAR" ] || { echo "Missing server jar: $SERVER_JAR (run: $0 build)"; exit 1; }
  gpu_libs
  echo "GLUTEN_HOME=$GLUTEN_HOME | SPARK_HOME=$SPARK_HOME"
  echo "Gluten=$GLUTEN | cuDF(GPU)=$CUDF | port=$SERVER_PORT"
  local conf=()
  mapfile -t conf < <(gluten_conf)
  exec "$SPARK_HOME/bin/spark-submit" \
    --master "local[*]" \
    --class "$MAIN_CLASS" \
    --name GlutenSubstraitGpuExecutor \
    --jars "$BUNDLE_JAR" \
    --driver-library-path "$LD_LIBRARY_PATH" \
    --conf "spark.driver.memory=${SERVER_MEM:-8g}" \
    --conf "spark.driver.extraJavaOptions=-Dflightsql.port=$SERVER_PORT $JAVAOPTS" \
    --conf "spark.executor.extraJavaOptions=$JAVAOPTS" \
    --conf "spark.sql.shuffle.partitions=1" \
    --conf "spark.default.parallelism=1" \
    "${conf[@]}" \
    "$SERVER_JAR"
}

cmd_gateway() {
  [ -d "$GATEWAY_DIR" ] || { echo "Missing gateway repo: $GATEWAY_DIR (see gateway/GATEWAY_CHANGES.md)"; exit 1; }
  cd "$GATEWAY_DIR"
  echo "gateway :$GATEWAY_PORT  ->  server grpc://localhost:$SERVER_PORT"
  GATEWAY_BACKEND=gluten PYTHONPATH=src GLUTEN_FLIGHTSQL_URI="grpc://localhost:$SERVER_PORT" \
    exec "$VENV/bin/python" -m gateway.server --port "$GATEWAY_PORT"
}

cmd_demo() {
  echo ">> end-to-end demo against $DATA  (server :$SERVER_PORT, gateway :$GATEWAY_PORT must be up)"
  DEMO_DATA="$DATA" GATEWAY_PORT="$GATEWAY_PORT" "$VENV/bin/python" - <<'PY'
import os
from pyspark.sql import SparkSession

data = os.environ["DEMO_DATA"]
port = os.environ["GATEWAY_PORT"]
spark = (
    SparkSession.builder.remote(f"sc://localhost:{port}")
    # keep small inline data inline (gateway lacks cached_local_relation)
    .config("spark.sql.session.localRelationCacheThreshold", str(1 << 30))
    .getOrCreate()
)

def show(label, fn):
    try:
        print(f"[PASS] {label}: {fn()}")
    except Exception as e:
        print(f"[FAIL] {label}: {str(e).splitlines()[-1][:160]}")

print(">> inline data (runs on Gluten CPU Velox):")
df = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["a", "b"])
show("inline select", lambda: df.select("a").collect())

print(f">> parquet read (Gluten Velox scan) — {data}:")
show("parquet read", lambda: spark.read.parquet(data).limit(3).collect())

print(">> parquet filter + aggregate (target: Gluten/Velox-cuDF GPU):")
d = spark.read.parquet(data)
show("parquet filter", lambda: d.filter(d.l_orderkey > 100).limit(3).collect())
d2 = spark.read.parquet(data)
show("parquet groupBy.count",
     lambda: d2.filter(d2.l_orderkey > 100).groupBy("l_linenumber").count()
               .orderBy("l_linenumber").collect())
PY
}

cmd_gpu_proof() {
  [ -f "$BUNDLE_JAR" ] || { echo "Missing Gluten bundle: $BUNDLE_JAR"; exit 1; }
  gpu_libs
  echo ">> standalone GPU proof on $DATA  (watch 'nvidia-smi dmon -s um' in another shell)"
  local py=/tmp/_gpu_proof.py
  cat > "$py" <<PY
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
spark = SparkSession.builder.appName("gluten-gpu-proof").getOrCreate()
df = spark.read.parquet("$DATA")
# filter + group-by-count: the shape we verified offloads to the GPU
q = (df.filter(df.l_orderkey > 100)
       .groupBy("l_linenumber").agg(count("*").alias("c"))
       .orderBy("l_linenumber"))
print("RESULT:", q.collect())
print("=== EXECUTED PLAN (look for Cudf* / shuffle_writer_type=gpu_hash) ===")
print(q._jdf.queryExecution().executedPlan().toString())
spark.stop()
PY
  local conf=()
  mapfile -t conf < <(gluten_conf)
  exec "$SPARK_HOME/bin/spark-submit" \
    --master "local[*]" \
    --jars "$BUNDLE_JAR" \
    --driver-library-path "$LD_LIBRARY_PATH" \
    --conf "spark.driver.extraJavaOptions=$JAVAOPTS" \
    --conf "spark.executor.extraJavaOptions=$JAVAOPTS" \
    --conf "spark.sql.shuffle.partitions=1" \
    "${conf[@]}" \
    "$py"
}

cmd_tpch() {
  # Standalone TPC-H on Spark + Gluten + Velox-cuDF (NOT through the gateway/executor —
  # the gateway can't carry aggregates yet). Answers "CPU vs GPU, is there a payoff?".
  #   QUERIES="1 6 3"  (default "1 6"; "all" = Q1..Q22)   CUDF=true|false   EXPLAIN=1
  [ -f "$BUNDLE_JAR" ] || { echo "Missing Gluten bundle: $BUNDLE_JAR (build Gluten first)"; exit 1; }
  gpu_libs
  local queries="${QUERIES:-1 6}"
  echo ">> TPC-H on Spark+Gluten+Velox  (cuDF/GPU=$CUDF)  queries: [$queries]  data=$TPCH_DATA"
  local py=/tmp/_tpch.py
  cat > "$py" <<'PY'
import json, os, time
from pyspark.sql import SparkSession

data, qjson = os.environ["TPCH_DATA"], os.environ["TPCH_QUERIES"]
pick = os.environ.get("QUERIES", "1 6").split()
explain = bool(os.environ.get("EXPLAIN"))

spark = SparkSession.builder.appName("gluten-tpch").getOrCreate()
print("spark.plugins =", spark.conf.get("spark.plugins", "<none>"),
      "| cudf =", spark.conf.get("spark.gluten.sql.columnar.cudf", "<none>"))

for t in ["lineitem","orders","customer","part","partsupp","supplier","nation","region"]:
    spark.read.parquet(f"{data}/{t}").createOrReplaceTempView(t)

queries = json.load(open(qjson))
getq = lambda n: queries.get(f"Q{n}") or queries.get(str(n))
if pick == ["all"]:
    pick = [str(i) for i in range(1, 23)]

out = []
for n in pick:
    sql = getq(n)
    if not sql:
        print(f"Q{n}: (not in query set)"); continue
    try:
        t0 = time.time()
        df = spark.sql(sql)
        res = df.collect()
        dt = time.time() - t0
        gpu = df._jdf.queryExecution().executedPlan().toString().count("Cudf")
        out.append((n, dt, len(res), gpu, None))
        print(f"Q{n}: {dt:6.2f}s  rows={len(res):<4} cudf_ops={gpu}", flush=True)
        if explain:
            print(df._jdf.queryExecution().executedPlan().toString()[:2500])
    except Exception as e:
        msg = str(e).splitlines()[-1][:140]
        out.append((n, None, None, None, msg))
        print(f"Q{n}: FAIL  {msg}", flush=True)

print("\n=== SUMMARY (cudf/GPU =", spark.conf.get("spark.gluten.sql.columnar.cudf","?"), ") ===")
print(f"{'query':<6}{'time':>9}{'rows':>7}{'cudf':>6}  note")
for n, dt, rc, gpu, err in out:
    t   = ("%.2fs" % dt) if dt is not None else "FAIL"
    rcs = "" if rc  is None else str(rc)
    gps = "" if gpu is None else str(gpu)
    note = err or ("GPU" if gpu else "CPU-fallback")
    print(f"Q{n:<5}{t:>9}{rcs:>7}{gps:>6}  {note}")
spark.stop()
PY
  local conf=()
  mapfile -t conf < <(gluten_conf)
  export TPCH_DATA TPCH_QUERIES QUERIES="$queries" EXPLAIN="${EXPLAIN:-}"
  exec "$SPARK_HOME/bin/spark-submit" \
    --master "local[*]" \
    --jars "$BUNDLE_JAR" \
    --driver-library-path "$LD_LIBRARY_PATH" \
    --conf "spark.driver.extraJavaOptions=$JAVAOPTS" \
    --conf "spark.executor.extraJavaOptions=$JAVAOPTS" \
    --conf "spark.driver.memory=24g" \
    --conf "spark.sql.shuffle.partitions=${SHUFFLE:-16}" \
    "${conf[@]}" \
    "$py"
}

cmd_stop() {
  pkill -9 -f "$MAIN_CLASS"   2>/dev/null && echo "killed server" || echo "no server running"
  pkill -9 -f gateway.server  2>/dev/null && echo "killed gateway" || echo "no gateway running"
}

usage() { awk 'NR==1{next} /^#/{sub(/^# ?/,"");print;next} {exit}' "${BASH_SOURCE[0]}"; }

case "${1:-help}" in
  build)     cmd_build ;;
  server)    cmd_server ;;
  gateway)   cmd_gateway ;;
  demo)      cmd_demo ;;
  gpu-proof) cmd_gpu_proof ;;
  tpch)      cmd_tpch ;;
  stop)      cmd_stop ;;
  help|-h|--help) usage ;;
  *) echo "unknown command: $1"; echo; usage; exit 1 ;;
esac
