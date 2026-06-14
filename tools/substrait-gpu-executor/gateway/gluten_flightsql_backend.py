# SPDX-License-Identifier: Apache-2.0
"""Backend that sends Substrait plans to a remote Gluten Flight SQL server.

This is the seam that lets the gateway use Gluten/Velox (Spark, optionally on
GPU via Velox-cuDF) as its execution engine. It connects to the Flight SQL
server implemented in gluten/tools/substrait-flightsql-server over ADBC and
hands it the Substrait plan via CommandStatementSubstraitPlan.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pyarrow as pa
from adbc_driver_flightsql import dbapi as flight_sql
from substrait.gen.proto import plan_pb2

from backends.backend import Backend
from backends.backend_options import BackendOptions
from transforms.rename_functions import RenameFunctionsForGluten


def _import(handle) -> pa.RecordBatchReader:
    """Import an ADBC ArrowArrayStream C handle as a RecordBatchReader."""
    return pa.RecordBatchReader._import_from_c(handle.address)


class GlutenFlightSqlBackend(Backend):
    """Sends Substrait plans to the Gluten Flight SQL server and reads Arrow back."""

    def __init__(self, options: BackendOptions):
        """Initialize the Gluten Flight SQL backend."""
        self._connection = None
        self._options = options
        super().__init__(options)
        self.create_connection()

    def create_connection(self) -> None:
        """Open an ADBC Flight SQL connection to the Gluten server."""
        uri = getattr(self._options, "flightsql_uri", "grpc://localhost:50052")
        self._connection = flight_sql.connect(uri)

    def reset_connection(self) -> None:
        """Close and reopen the connection."""
        if self._connection is not None:
            self._connection.close()
        self._connection = None
        self.create_connection()

    @contextmanager
    def adjust_plan(self, plan: plan_pb2.Plan) -> Iterator[plan_pb2.Plan]:
        """Rewrite function names to the compound signatures substrait-java needs."""
        RenameFunctionsForGluten().visit_plan(plan)
        yield plan

    def _execute_plan(self, plan: plan_pb2.Plan) -> pa.lib.Table:
        """Execute the given Substrait plan on the remote Gluten server."""
        import os
        if os.environ.get("GLUTEN_DEBUG_PLAN"):
            from google.protobuf import json_format
            with open("/home/vpcuser/last_plan.json", "w") as fh:
                fh.write(json_format.MessageToJson(plan))
        with self._connection.cursor() as cur:
            cur.adbc_statement.set_substrait_plan(plan.SerializeToString())
            handle, _ = cur.adbc_statement.execute_query()
            return _import(handle).read_all()

    # --- Table registration -------------------------------------------------
    # MVP: inline data (createDataFrame) travels inside the Substrait plan as a
    # virtual table, so no registration round-trip is needed. Named-table and
    # file registration on the remote Spark session are future work (would push
    # a temp-view creation to the server via a custom Flight action).
    def register_table(
        self,
        name: str,
        path: Path,
        file_format: str = "parquet",
        temporary: bool = False,
        replace: bool = False,
    ) -> None:
        """Register a table on the remote Spark session (not yet implemented)."""
        raise NotImplementedError(
            "GlutenFlightSqlBackend does not yet support named-table registration; "
            "use inline DataFrames or file-path (LocalFiles) reads for now."
        )

    def register_table_with_arrow_data(
        self, name: str, data: bytes, temporary: bool = False, replace: bool = False
    ) -> None:
        """Register Arrow data as a table (not yet implemented)."""
        raise NotImplementedError(
            "GlutenFlightSqlBackend does not yet support Arrow-data table registration."
        )

    def describe_files(self, paths: list[str]):
        """Return the Arrow schema of the given parquet file(s).

        The gateway needs this to build the Substrait LocalFiles base schema for a
        `spark.read.parquet(...)`. We read it directly with pyarrow (no round-trip to
        the server needed); the file paths are shared with the JVM server (same host).
        """
        import pyarrow.parquet as pq

        files = paths
        if len(paths) == 1:
            files = Backend._expand_location(paths[0])
        return pq.ParquetFile(files[0]).schema_arrow
