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

import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class BloopSetupTest(unittest.TestCase):
    def test_injects_jvm_options_into_all_configurations(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            (root / "dev").mkdir()
            (root / "build").mkdir()
            shutil.copyfile(
                Path(__file__).resolve().parents[4] / "dev/bloop-setup.sh",
                root / "dev/bloop-setup.sh",
            )
            for name in ("first", "second"):
                config = {
                    "project": {
                        "name": name,
                        "platform": {"name": "jvm", "config": {"options": []}},
                    }
                }
                (root / "build" / f"{name}.json").write_text(json.dumps(config))
            maven = root / "build/mvn"
            maven.write_text(
                "#!/bin/sh\nset -e\nmkdir .bloop\ncp build/*.json .bloop/\n"
            )
            maven.chmod(0o755)

            result = subprocess.run(
                ["bash", "dev/bloop-setup.sh", "-Pspark-3.5,backends-velox"],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=30,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            for name in ("first", "second"):
                config = json.loads((root / ".bloop" / f"{name}.json").read_text())
                self.assertIn(
                    "--add-opens=java.base/java.lang=ALL-UNNAMED",
                    config["project"]["platform"]["config"]["options"],
                    name,
                )


if __name__ == "__main__":
    unittest.main()
