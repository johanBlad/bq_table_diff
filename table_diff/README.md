# Table Diff

## Overview

Command line tool to compare the data content of two tables in BigQuery. The key idea is to be able to compare tables that should be similar, e.g. the same table mirrored in the TEST/PROD. It could for example be used for the following use cases, and more.

1. As a dbt developer, run the same model in parallell in your sandbox as two separate versions; one in full-refresh and the other in incremental mode, and comparing their content to check that they are equal.
2. As a dbt developer, comparing the content of the same table in TEST/PROD when debugging a model or simply validating that tables are equal.
3. As a dbt developer when updating an already existing model A, comparing your updated sandbox version of model A to the same model A in staging (on a limited dataset).
4. In Github CI, when updating an already existing model, check the changes that the PR will cause by comparing the model run in CI and the one existing in statging (on a limited dataset), and posting the results back as a comment to the PR.

Inspired by https://github.com/datafold/data-diff

## Usage

Check the [Examples](#examples) section for ready-to-go examples.

To run the program, you need to have python >=3.8 and the python-bigquery-client installed (you should already have these installed from when you set up dbt).

```bash
python -m pyutils.table_diff.cli --help
```

In short, you need to provide the following

- **output** (required): `{rows,statistics}`
  - `--output statistics` returns a condensed output of differences between the tables.
  - `--output rows` returns the actual rows that did not match between the two tables.
- **t1** (required): `string`
  - Example: `--t1 dwh-prod.schema.fact_tbl`
- **t2** (required): `string`
  - Example: `--t2 dwh-test.schema.fact_tbl`
- **join-columns**: `string`
  - A comma-separated list of columns to use as keys to join between the two tables. This is not required, but highly recommended. If not supplied, the tool can't provide a column level diff.
  - Example: `--join-on="subscription_id, date"`

You can also supply optional parameters. See the full list in the `--help` output above, but here are the most important ones.

- **filter**: `string`
  - applies a filter to the SQL statement that will be run against each table in BigQuery. Useful for selecting a subset of the data to run on. This string is passed directly to the `WHERE` statements in the query.
  - Example: `--filter="DATE(ingestion_timestamp) >= '2022-10-01'"` --> `... WHERE DATE(ingestion_timestamp) >= '2022-10-01' ...`
- **dry-run**: `string`
  - Use the `--dry-run` flag to check the query cost and print the SQL query that will be run against BigQuery. Useful if you want to run the query yourself in the BigQuery console.
- **diff-columns**: `string`
  - A comma-separated list of columns to explicitly set the columns to check for differences (default is all columns, i.e. `*`). Useful for reducing cost/runtime of the query and the output size.
  - Example: `--include="revenue,churn"`
- **exclude-columns**: `string`
  - A comma-separated list of columns to explicitly exclude from the check (note that a few blacklisted columns are excluded by default, such as `insertion_ts`).
  - Example: `--exclude="user_unique_id"`

## Examples

### TEST/PROD output statistic diff with known join key and filter

```bash
python -m pyutils.table_diff.cli --output statistics --t1 dwh-test.schema.fact_tbl --t2 dwh-prod.schema.fact_tbl --join-on="subscription_id,date"  --filter="date >= '2022-10-01'"
```

### TEST/PROD output rows diff with known join key and subset of diff-columns

```bash
python -m pyutils.table_diff.cli --output rows --t1 dwh-test.schema.dim_user_latest --t2 dwh-prod.schema.dim_user_latest --join-on="user_unique_id" --include="user_pii_unique_key,account_type.account_type_id"
```

### TEST/PROD output statistics diff without join key

```bash
python -m pyutils.table_diff.cli --output statistics --t1 dwh-test.schema.dim_user_latest --t2 dwh-prod.schema.dim_user_latest
```

### TEST/PROD output statistics without join key dry-run

```bash
python -m pyutils.table_diff.cli --output statistics --t1 dwh-test.schema.dim_user_latest --t2 dwh-prod.schema.dim_user_latest --dry-run
```
