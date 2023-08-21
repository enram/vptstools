# vptstools

[![Project generated with PyScaffold](https://img.shields.io/badge/-PyScaffold-005CA0?logo=pyscaffold)](https://pyscaffold.org/)
[![PyPI-Server](https://img.shields.io/pypi/v/vptstools.svg)](https://pypi.org/project/vptstools/)
[![.github/workflows/release.yml](https://github.com/enram/vptstools/actions/workflows/release.yml/badge.svg)](https://github.com/enram/vptstools/actions/workflows/release.yml)

vptstools is a Python library to transfer and convert vpts data. VPTS (vertical profile time series) express the 
density, speed and direction of biological signals such as birds, bats and insects within a weather radar volume, 
grouped into altitude layers (height) and measured over time (datetime).

## Installation

Python 3.9+ is required.

```
pip install vptstools
```

If you need the tools/services to transfer data (SFTP, S3) install these additional dependencies:

```ini
pip install vptstools\[transfer\]
```

## Usage

As a library user interested in working with ODIM h5 and vpts files, the most important functions provided by the 
package are {py:func}`vptstools.vpts.vp`, {py:func}`vptstools.vpts.vpts` and {py:func}`vptstools.vpts.vpts_to_csv`, 
which can be used respectively to convert a single `h5` file, a set of `h5` files and save a `vpts` DataFrame 
to a csv-file:

- Convert a single local ODIM h5 file to a vp DataFrame:

```python
from vptstools.vpts import vp

file_path_h5 = "./NLDBL_vp_20080215T0010_NL50_v0-3-20.h5"
df_vp = vp(file_path_h5)
```

- Convert a set of locally stored ODIM h5 files to a vpts DataFrame:

```python
from pathlib import Path
from vptstools.vpts import vpts

file_paths = sorted(Path("./data").rglob("*.h5"))  # Get all h5 files within the data directory
df_vpts = vpts(file_paths)
```

- Store a `vp` or `vpts` DataFrame to a [VPTS CSV](https://aloftdata.eu/vpts-csv/) file:

```python
from vptstools.vpts import vpts_to_csv

vpts_to_csv(df_vpts, "vpts.csv")
```

```{note} 
Both {py:func}`vptstools.vpts.vp` and {py:func}`vptstools.vpts.vpts` have 2 other optional parameters related to the
[VPTS-CSV data exchange format](https://aloftdata.eu/vpts-csv/). The `vpts_csv_version` parameter defines the version of the 
[VPTS-CSV data exchange standard](https://aloftdata.eu/vpts-csv/) (default v1) whereas the `source_file` provides a way to define
a custom [source_file](https://aloftdata.eu/vpts-csv/#source_file) field to reference the source from which the 
data were derived. 
```

To validate a vpts DataFrame against the frictionless data schema as defined by the VPTS-CSV data exchange 
format and return a report, use the {py:func}`vptstools.vpts.validate_vpts`:

```python
from vptstools.vpts import validate_vpts

report = validate_vpts(df_vpts, version="v1")
report.stats["errors"]
```

Other modules in the package are:

- {py:mod}`vptstools.odimh5`: This module extents the implementation of the original 
  [odimh5 package](https://pypi.org/project/odimh5/) which is now deprecated.
- {py:mod}`vptstools.vpts_csv`: This module contains - for each version of the VPTS-CSV exchange format - the 
  corresponding implementation which can be used to generate a `vp` or `vpts` DataFrame. For more information on how to
  support a new version of the VPTS-CSV format, see [contributing docs](#new-vptscsv-version). 
- {py:mod}`vptstools.s3`: This module contains the functions to manage the 
  aloft data repository](https://aloftdata.eu/browse/) S3 Bucket.

## CLI endpoints

In addition to using functions in Python scripts, two vptstools routines are available to be called from the command line
after installing the package:

```{eval-rst}
.. include:: click.rst
```

## Development instructions

See [contributing](docs/contributing.md) for a detailed overview and set of guidelines. If familiar with `tox`, 
the setup of a development environment boils down to:

```shell
tox -e dev   # Create development environment with venv and register an ipykernel. 
source venv/bin/activate  # Activate this environment to get started
```

Next, the following set of commands are available to support development:

```shell
tox              # Run the unit tests
tox -e docs      # Invoke sphinx-build to build the docs
tox -e format    # Run black code formatting

tox -e clean     # Remove old distribution files and temporary build artifacts (./build and ./dist)
tox -e build     # Build the package wheels and tar

tox -e linkcheck # Check for broken links in the documentation

tox -e publish   # Publish the package you have been developing to a package index server. By default, it uses testpypi. If you really want to publish your package to be publicly accessible in PyPI, use the `-- --repository pypi` option.
tox -av          # List all available tasks
```

To create a pinned `requirements.txt` set of dependencies, [pip-tools](https://github.com/jazzband/pip-tools) is used:

```bash
pip-compile --extra transfer --resolver=backtracking`
```

<!-- pyscaffold-notes -->

## Notes

- This project has been set up using PyScaffold 4.3.1. For details and usage information on PyScaffold see https://pyscaffold.org/.
- The `odimh5` module was originally developed and released to pypi as a separate [`odimh5`](https://pypi.org/project/odimh5/) package by Nicolas Noé ([@niconoe](https://github.com/niconoe)). Version 0.1.0 has been included into this vptstools package.
