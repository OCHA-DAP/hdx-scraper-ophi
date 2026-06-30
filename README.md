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
dataset is generated from the global MPI data. It runs annually in late October
(between the 20th and 24th, on the nearest weekday) at around 1 PM UTC and takes
approximately 10 minutes to complete.

## Data Pipeline

### API reads (~3 downloads + small number of HDX reads per run)

- **OPHI Excel downloads** (3 downloads): national results, subnational results,
  and trends files from the OPHI website, each approximately 1–2 MB.
- **Google Sheets CSV** (1 download): country showcase links.
- **HDX admin boundary reads** (small number): admin-1 P-codes fetched from COD
  admin boundary datasets.

### API writes (~100–110 calls per run)

- **Per-country MPI datasets** (~one write per country): each dataset contains MPI,
  Headcount Ratio, Intensity of Deprivation, Vulnerable to Poverty, and In Severe
  Poverty values at national and subnational level.
- **Global MPI dataset** (1 write): aggregates data across all countries.
- **HAPI poverty rate dataset** (1 write): derived from the global MPI data.

### Temporary files

- None significant; data is read directly into memory from downloaded files.

### Uploaded files

- Per-country MPI datasets with national and subnational poverty metrics.
- Global MPI dataset.
- HAPI poverty rate dataset.

### Transformations

1. **Excel parsing**: national results, subnational results, and trend tables are
   extracted from the downloaded Excel files.
2. **P-code matching**: admin-1 region names are matched to P-codes using COD
   admin boundaries.
3. **Metric standardisation**: poverty metrics (MPI, Headcount Ratio, Intensity of
   Deprivation, Vulnerable to Poverty, In Severe Poverty) are standardised to 4
   decimal places.
4. **Trend join**: trend data covering two timepoints per country is joined to the
   national results.

## Development

### Environment

Development is currently done using Python 3.13. The environment can be created with:

```shell
    uv sync
```

This creates a .venv folder with the versions specified in the project's uv.lock file.

### Installing and running

For the script to run, you will need to have a file called
.hdx_configuration.yaml in your home directory containing your HDX key, e.g.:

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod

 You will also need to supply the universal .useragents.yaml file in your home
 directory as specified in the parameter *user_agent_config_yaml* passed to
 facade in run.py. The collector reads the key
 **hdx-scraper-ophi** as specified in the parameter
 *user_agent_lookup*.

 Alternatively, you can set up environment variables: `USER_AGENT`, `HDX_KEY`,
`HDX_SITE`, `EXTRA_PARAMS`, `TEMP_DIR`, and `LOG_FILE_ONLY`.

To run, execute:

```shell
    uv run python -m hdx.scraper.ophi
```

### Pre-commit

pre-commit will be installed when syncing uv. It is run every time you make a git
commit if you call it like this:

```shell
    pre-commit install
```

With pre-commit, all code is formatted according to
[ruff](https://docs.astral.sh/ruff/) guidelines.

To check if your changes pass pre-commit without committing, run:

```shell
    pre-commit run --all-files
```

## Packages

[uv](https://github.com/astral-sh/uv) is used for package management.  If
you've introduced a new package to the source code (i.e. anywhere in `src/`),
please add it to the `project.dependencies` section of `pyproject.toml` with
any known version constraints.

To add packages required only for testing, add them to the
`[dependency-groups]`.

Any changes to the dependencies will be automatically reflected in
`uv.lock` with `pre-commit`, but you can re-generate the files without committing by
executing:

```shell
    uv lock --upgrade
```

## Project

[uv](https://github.com/astral-sh/uv) is used for project management. The project can be
built using:

```shell
    uv build
```

Linting and syntax checking can be run with:

```shell
    uv run ruff check
```

To run the tests and view coverage, execute:

```shell
    uv run pytest
```
