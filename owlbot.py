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

import pathlib

import synthtool as s
from synthtool import gcp
from synthtool.languages import python

common = gcp.CommonTemplates()

spanner_default_version = pathlib.Path("spanner/v1")
spanner_admin_instance_default_version = pathlib.Path("spanner_admin_instance/v1")
spanner_admin_database_default_version = pathlib.Path("spanner_admin_database/v1")

staging_dir = pathlib.Path('owl-bot-staging')

if staging_dir.is_dir():
    library = staging_dir / spanner_default_version
    # Work around gapic generator bug https://github.com/googleapis/gapic-generator-python/issues/902
    s.replace(library / f"google/cloud/spanner_{library.name}/types/transaction.py",
            r""".
Attributes:""",
            r""".\n
Attributes:""",
    )

    # Work around gapic generator bug https://github.com/googleapis/gapic-generator-python/issues/902
    s.replace(library / f"google/cloud/spanner_{library.name}/types/transaction.py",
            r""".
    Attributes:""",
            r""".\n
    Attributes:""",
    )

    # Remove headings from docstring. Requested change upstream in cl/377290854 due to https://google.aip.dev/192#formatting.
    s.replace(library / f"google/cloud/spanner_{library.name}/types/transaction.py",
        """\n    ==.*?==\n""",
        ":",
    )

    # Remove headings from docstring. Requested change upstream in cl/377290854 due to https://google.aip.dev/192#formatting.
    s.replace(library / f"google/cloud/spanner_{library.name}/types/transaction.py",
        """\n    --.*?--\n""",
        ":",
    )

    s.move(library, excludes=["google/cloud/spanner/**", "*.*", "docs/index.rst", "google/cloud/spanner_v1/__init__.py"])

    library = staging_dir / spanner_admin_instance_default_version
    s.move(library, excludes=["google/cloud/spanner_admin_instance/**", "*.*", "docs/index.rst"])

    library = staging_dir / spanner_admin_database_default_version
    s.move(library, excludes=["google/cloud/spanner_admin_database/**", "*.*", "docs/index.rst"])

s.remove_staging_dirs()

# ----------------------------------------------------------------------------
# Add templated files
# ----------------------------------------------------------------------------
templated_files = common.py_library(microgenerator=True, samples=True)
s.move(templated_files, excludes=[".coveragerc", "noxfile.py"])

# Ensure CI runs on a new instance each time
s.replace(
    ".kokoro/build.sh",
    "# Remove old nox",
    "# Set up creating a new instance for each system test run\n"
    "export GOOGLE_CLOUD_TESTS_CREATE_SPANNER_INSTANCE=true\n"
    "\n\g<0>",
)

# ----------------------------------------------------------------------------
# Samples templates
# ----------------------------------------------------------------------------

python.py_samples()

s.shell.run(["nox", "-s", "blacken"], hide_output=False)
