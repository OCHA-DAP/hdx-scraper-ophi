# OPHI Pipeline

[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-ophi/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-ophi/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-ophi/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-ophi?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

This pipeline retrieves Multidimensional Poverty Index (MPI) data from the
[Oxford Poverty and Human Development Initiative (OPHI)](https://ophi.org.uk/)
and publishes it to HDX first as per-country and global MPI datasets, and then
as a HAPI poverty rate dataset derived from the same data. It downloads 3 Excel files from the OPHI website (national
results, subnational results, and trends, each approximately 1–2 MB), 1 CSV from
Google Sheets (country showcase links), and makes a small number of HDX reads to
fetch admin-1 boundary P-codes. It makes approximately 100–110 HDX writes
(one per country dataset plus a global dataset and a HAPI dataset). The Excel
tables are parsed to extract MPI, Headcount Ratio, Intensity of Deprivation,
Vulnerable to Poverty, and In Severe Poverty values; admin-1 region names are
matched to P-codes using COD admin boundaries; poverty metrics are standardised
to 4 decimal places; and trend data (two timepoints per country) is joined; these results are written
to the per-country and global MPI datasets first, and then the HAPI poverty rate
dataset is generated from the global MPI data. It is run annually on 20 October.

## Development

### Environment

Development is currently done using Python 3.13. We recommend using a virtual
environment such as ``venv``:

```shell
    python -m venv venv
    source venv/bin/activate
```

In your virtual environment, install all packages for development by running:

```shell
    pip install -r requirements.txt
```

### Installing and running

To install and run, execute:

```shell
    pip install .
    python -m hdx.scraper.ophi
```

### Pre-commit

Be sure to install `pre-commit`, which is run every time you make a git commit:

```shell
    pip install pre-commit
    pre-commit install
```

With pre-commit, all code is formatted according to
[ruff](https://docs.astral.sh/ruff/) guidelines.

To check if your changes pass pre-commit without committing, run:

```shell
    pre-commit run --all-files
```

### Testing

Ensure you have the required packages to run the tests:

```shell
    pip install -r requirements-test.txt
```

To run the tests and view coverage, execute:

```shell
    pytest -c --cov hdx
```

## Packages

[uv](https://github.com/astral-sh/uv) is used for package management.  If
you’ve introduced a new package to the source code (i.e. anywhere in `src/`),
please add it to the `project.dependencies` section of `pyproject.toml` with
any known version constraints.

To add packages required only for testing, add them to the `test` section under
`[project.optional-dependencies]`.

Any changes to the dependencies will be automatically reflected in
`requirements.txt` and `requirements-test.txt` with `pre-commit`, but you can
re-generate the files without committing by executing:

```shell
    pre-commit run pip-compile --all-files
```

## Project

[Hatch](https://hatch.pypa.io/) is used for project management. The project can be built using:

```shell
    hatch build
```

Linting and syntax checking can be run with:

```shell
    hatch fmt --check
```

Tests can be executed using:

```shell
    hatch test
```
