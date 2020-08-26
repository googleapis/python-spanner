# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This script is used to synthesize generated parts of this library."""
import synthtool as s
from synthtool import gcp
from synthtool.languages import python

gapic = gcp.GAPICBazel()
common = gcp.CommonTemplates()

# ----------------------------------------------------------------------------
# Generate spanner GAPIC layer
# ----------------------------------------------------------------------------
library = gapic.py_library(
    service="spanner",
    version="v1",
    bazel_target="//google/spanner/v1:spanner-v1-py",
    include_protos=True,
)

s.move(library, excludes=["google/spanner/**", "google/spanner_v1/**", "*.*", "docs/index.rst"])
s.move(library / "google/spanner_v1", "google/cloud/spanner_v1")

# Fix up generated imports
s.replace(
    ["google/**/*.py", "tests/**/*.py"],
    "google\.spanner_v1",
    "google.cloud.spanner_v1",
)

# Fix invalid imports
s.replace(
    "google/**/*.py",
    "gs_type",
    "type"
)

# ----------------------------------------------------------------------------
# Generate instance admin client
# ----------------------------------------------------------------------------
library = gapic.py_library(
    service="spanner_admin_instance",
    version="v1",
    bazel_target="//google/spanner/admin/instance/v1:admin-instance-v1-py",
    include_protos=True,
)

s.move(library, excludes=["google/spanner/**", "*.*", "docs/index.rst"])
s.move(library / "google/spanner/admin/instance_v1", "google/cloud/spanner_admin_instance_v1")
s.move(library / "google/spanner/admin/instance", "google/cloud/spanner_admin_instance_v1")

# Fix up generated imports
s.replace(
    ["google/**/*.py", "tests/**/*.py"],
    "google\.spanner\.admin\.instance_v1",
    "google.cloud.spanner_admin_instance_v1",
)

# ----------------------------------------------------------------------------
# Generate database admin client
# ----------------------------------------------------------------------------
library = gapic.py_library(
    service="spanner_admin_database",
    version="v1",
    bazel_target="//google/spanner/admin/database/v1:admin-database-v1-py",
    include_protos=True,
)

s.move(library, excludes=["google/spanner/**", "*.*", "docs/index.rst"])
s.move(library / "google/spanner/admin/database_v1", "google/cloud/spanner_admin_database_v1")
s.move(library / "google/spanner/admin/database", "google/cloud/spanner_admin_database_v1")

# Fix up generated imports
s.replace(
    ["google/**/*.py", "tests/**/*.py"],
    "google\.spanner\.admin\.database_v1",
    "google.cloud.spanner_admin_database_v1",
)

# ----------------------------------------------------------------------------
# Add templated files
# ----------------------------------------------------------------------------
templated_files = common.py_library(microgenerator=True, samples=True)
s.move(templated_files, excludes=[".coveragerc"])

# Template's MANIFEST.in does not include the needed GAPIC config file.
# See PR #6928.
s.replace(
    "MANIFEST.in",
    "include README.rst LICENSE\n",
    "include README.rst LICENSE\n"
    "include google/cloud/spanner_v1/gapic/transports/spanner.grpc.config\n",
)

# Ensure CI runs on a new instance each time
s.replace(
    ".kokoro/build.sh",
    "# Remove old nox",
    "# Set up creating a new instance for each system test run\n"
    "export GOOGLE_CLOUD_TESTS_CREATE_SPANNER_INSTANCE=true\n"
    "\n\g<0>",
)

# Ensure tracing dependencies are installed
s.replace(
    "noxfile.py",
    f"session.install\(.-e., .\..\)",
    "session.install(\"-e\", \".[tracing]\")",
)

# Update check to allow for emulator use
s.replace(
    "noxfile.py",
    "if not os.environ.get\(.GOOGLE_APPLICATION_CREDENTIALS., ..\):",
    "if not os.environ.get(\"GOOGLE_APPLICATION_CREDENTIALS\", \"\") and "
    "\tnot os.environ.get(\"SPANNER_EMULATOR_HOST\", \"\"):",
)

# Update error message to indicate emulator usage
s.replace(
    "noxfile.py",
    "Credentials must be set via environment variable",
    "Credentials or emulator host must be set via environment variable"
)

# ----------------------------------------------------------------------------
# Samples templates
# ----------------------------------------------------------------------------

python.py_samples()

s.shell.run(["nox", "-s", "blacken"], hide_output=False)
