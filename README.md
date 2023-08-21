# vptstools

[![Project generated with PyScaffold](https://img.shields.io/badge/-PyScaffold-005CA0?logo=pyscaffold)](https://pyscaffold.org/)
[![PyPI-Server](https://img.shields.io/pypi/v/vptstools.svg)](https://pypi.org/project/vptstools/)
[![.github/workflows/run_tests.yaml](https://github.com/enram/vptstools/actions/workflows/release.yml/badge.svg)](https://github.com/enram/vptstools/actions/workflows/release.yml)

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

## CLI endpoints

```{eval-rst}
.. include:: click.rst
```

In addition to using functions in Python scripts, two vptstools functions can be called from the command line:

### transfer_baltrad

CLI tool to move files from the Baltrad FTP server to an S3 bucket.

```shell
transfer_baltrad
```

Configuration is loaded from environmental variables:

- FTP_HOST: Baltrad FTP host ip address
- FTP_PORT: Baltrad FTP host port
- FTP_USERNAME: Baltrad FTP user name
- FTP_PWD: Baltrad FTP password
- FTP_DATADIR: Baltrad FTP directory to load data files from
- DESTINATION_BUCKET: AWS S3 bucket to write data to
- SNS_TOPIC: AWS SNS topic to report when routine fails
- AWS_PROFILE: AWS profile (mainly for local development)

```{click} vptstools.bin.vph5_to_vpts:cli
:prog: Convert h5 files to daily/monthly vpts files

```

### vph5_to_vpts

CLI tool to aggregate/convert the [ODIM hdf5 bird profile](https://github.com/adokter/vol2bird/wiki/ODIM-bird-profile-format-specification) 
files available on the aloft S3 bucket (as generated by [vol2bird](https://github.com/adokter/vol2bird)) to 
daily and monthly aggregates following the [VPTS CSV file specification](https://github.com/enram/vpts-csv).

The CLI checks the modified date of the uploaded ODIM hdf5 files and applies the aggregation/conversion for the files modified within the defined time window:

```shell
vph5_to_vpts --modified-days-ago=1
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

```commandline
pip-compile --extra transfer --resolver=backtracking`
```

<!-- pyscaffold-notes -->

## Notes

- This project has been set up using PyScaffold 4.3.1. For details and usage information on PyScaffold see https://pyscaffold.org/.
- The `odimh5` module was originally developed and released to pypi as a separate [`odimh5`](https://pypi.org/project/odimh5/) package by Nicolas Noé ([@niconoe](https://github.com/niconoe)). Version 0.1.0 has been included into this vptstools package.
