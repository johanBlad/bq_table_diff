# Pyutils

This directory contains python applications for various use-cases related to data testing against BigQuery, Data Quality, dbt developer experience and the like.

## Table Diff

Command line tool to compare the data content of two tables in BigQuery. The key idea is to be able to compare tables that should be similar, e.g. the same table mirrored in the TEST/PROD.

> Check out the [README](/pyutils/table_diff/README.md) for further details!

## comparly

`comparly` is a command line tool that allows you to compare the data of the `incremental` and `full-refresh` runs of your model!
 
It creates two tables one
created running a `full-refresh` run and one an `incremental` run and uses [table_diff] to compare the differences of the tables.

> Check out the [README](/pyutils/comparly/README.md) for further details!
