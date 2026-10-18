# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = (
    Path(__file__).resolve().parents[4]
    / ".github/workflows/util/install-spark-resources.sh"
)


class SparkResourceInstallTest(unittest.TestCase):
    def test_spark4_resources_use_custom_install_directory(self):
        for version in ("4.0", "4.1", "4.2"):
            with self.subTest(
                version=version
            ), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                fixtures = root / "fixtures.sh"
                fixtures.write_text(
                    """
wget() { :; }
shasum() { :; }
tar() {
  case "$*" in
    *-bin-*) mkdir -p jars python bin; echo fixture > jars/test.jar ;;
    *) mkdir -p sql/core/src/test/resources sql/hive/src/test/resources ;;
  esac
}
mv() {
  for path in "$@"; do
    case "$path" in
      /opt|/opt/*) echo "Unexpected path: $path" >&2; return 1 ;;
    esac
  done
  command mv "$@"
}
"""
                )
                install_dir = root / "resources"
                result = subprocess.run(
                    ["bash", str(SCRIPT), version, str(install_dir)],
                    cwd=root,
                    env={**os.environ, "BASH_ENV": str(fixtures)},
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                spark_home = (
                    install_dir
                    / "shims"
                    / f"spark{version.replace('.', '')}"
                    / "spark_home"
                )
                target = spark_home / "assembly/target"
                self.assertEqual(
                    "fixture\n", (target / "scala-2.13/jars/test.jar").read_text()
                )
                self.assertFalse((target / "scala-2.12").exists())
                for resource in ("python", "bin", "sql/core/src/test/resources"):
                    self.assertTrue((spark_home / resource).is_dir(), resource)


if __name__ == "__main__":
    unittest.main()
