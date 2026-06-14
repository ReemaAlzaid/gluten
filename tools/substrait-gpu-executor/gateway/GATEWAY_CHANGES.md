# Gateway changes (voltrondata/spark-substrait-gateway)

The front end is the [spark-substrait-gateway](https://github.com/voltrondata/spark-substrait-gateway).
It needs a small set of edits to add a **`GLUTEN`** backend that ships the Substrait plan over
ADBC Flight SQL to our server. The gateway lives in its own repo; these are the changes to make there.

## Setup
```bash
# tarball (git clone needs auth in some envs)
curl -fSL -o ssg.tar.gz https://codeload.github.com/voltrondata/spark-substrait-gateway/tar.gz/refs/heads/main
tar xzf ssg.tar.gz && mv spark-substrait-gateway-main spark-substrait-gateway

# gateway needs Python 3.10+ (uses match / X|Y); use uv to make a 3.11 venv
uv python install 3.11
uv venv /home/vpcuser/ssg-venv --python 3.11
SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0 uv pip install --python /home/vpcuser/ssg-venv/bin/python \
  pyspark==3.5.5 grpcio grpcio-status grpcio-channelz grpcio-reflection pandas "pyarrow>=17" \
  adbc_driver_flightsql adbc_driver_manager "datafusion==41.0.*" "duckdb==1.1.0" \
  "substrait==0.21.0" "protobuf>=3.20" click cloudpickle PyJWT cryptography
```

## Edits

### 1. `src/backends/backend_options.py` — add the enum + endpoint
```python
class BackendEngine(Enum):
    ARROW = 1
    DATAFUSION = 2
    DUCKDB = 3
    GLUTEN = 4          # <-- add

# in BackendOptions.__init__, after the other flags:
        self.flightsql_uri = "grpc://localhost:50052"   # <-- add
```

### 2. `src/backends/gluten_flightsql_backend.py` — NEW FILE
Copy `gluten_flightsql_backend.py` from this folder into `src/backends/`. It:
- connects via `adbc_driver_flightsql` to `flightsql_uri`,
- `_execute_plan` sends the Substrait plan (`set_substrait_plan` + `execute_query`),
- `adjust_plan` applies `RenameFunctionsForGluten`,
- `describe_files` reads the parquet schema via pyarrow (so `spark.read.parquet` works).

### 3. `src/backends/backend_selector.py` — route GLUTEN
```python
from backends.gluten_flightsql_backend import GlutenFlightSqlBackend   # add import
        case BackendEngine.GLUTEN:                                      # add case
            return GlutenFlightSqlBackend(options)
```

### 4. `src/gateway/converter/conversion_options.py` — add the preset
```python
import os
def gluten():
    options = ConversionOptions(backend=BackendOptions(BackendEngine.GLUTEN))
    options.backend.flightsql_uri = os.environ.get("GLUTEN_FLIGHTSQL_URI", "grpc://localhost:50052")
    options.needs_scheme_in_path_uris = True   # emit file: URIs for parquet LocalFiles
    return options
```

### 5. `src/gateway/server.py` — select the backend by env
```python
from gateway.converter.conversion_options import arrow, datafusion, duck_db, gluten   # add gluten
# in SparkConnectService.__init__, replace `self._options = duck_db()` with:
        _presets = {"arrow": arrow, "datafusion": datafusion, "duckdb": duck_db, "gluten": gluten}
        self._options = _presets.get(os.getenv("GATEWAY_BACKEND", "duckdb").lower(), duck_db)()
```

### 6. `src/transforms/rename_functions.py` — add `RenameFunctionsForGluten`
A visitor that rewrites short Substrait function names to the compound keys substrait-spark
expects (`gt` -> `gt:any_any`, `count` -> `count:any`, plus the other comparisons). See the class
in our patched gateway; it mirrors `RenameFunctionsForDuckDB` but appends the `:any_any` signatures.

## Run
```bash
GATEWAY_BACKEND=gluten GLUTEN_FLIGHTSQL_URI=grpc://localhost:50052 PYTHONPATH=src \
  /home/vpcuser/ssg-venv/bin/python -m gateway.server --port 50051
```
(or `scripts/run.sh gateway`)
