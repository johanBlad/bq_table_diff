import os
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from typing import List, Optional, Tuple

from pyutils.common.dbt_metadata import get_model_metadata
from pyutils.table_diff.app import (
    TableDiff,
    TableDiffResultsStatistics,
)


# Allow the caller to set the limit, but cap it at as maximum, to avoid huge consequences of the caller making mistakes
GB_MODEL_COST_LIMIT = min(
    int(os.environ.get("REPORTBUILDER_GB_MODEL_COST_LIMIT", 250)), 2000
)
NUM_MODELS_LIMIT = min(int(os.environ.get("REPORTBUILDER_NUM_MODELS_LIMIT", 8)), 20)
MODEL_EXCLUSION_LIST = [
    "stg_snowplow_combine_event_versions",
    "stg_snowplow_combine_minor_versions",
    "int_events",
]


def create_diffreport(
    changed_models: List[str],
    base_bq_project: str,
    compare_bq_project: str,
    base_schema_prefix: str = "",
    compare_schema_prefix: str = "",
    result_limit: int = 10,
) -> str:
    if len(changed_models) > NUM_MODELS_LIMIT:
        return f"report skipped! models to diff is {len(changed_models)}; limit is {NUM_MODELS_LIMIT} models"

    # utilize dbt to get metadata for each dbt model and exract relevant fields
    metadata_for_models = get_model_metadata(changed_models)
    metadata_extracted = [
        extract_model_metadata(
            model_dict,
            base_bq_project,
            compare_bq_project,
            base_schema_prefix,
            compare_schema_prefix,
        )
        for model_dict in metadata_for_models
    ]

    models_to_diff = [e for e in metadata_extracted if e["materialized"] != "ephemeral"]

    table_diff_entities: List[TableDiff] = [
        initialize_table_diff(params) for params in models_to_diff
    ]
    model_names = [e["model"] for e in models_to_diff]

    with ThreadPoolExecutor(NUM_MODELS_LIMIT) as executor:
        query_costs = [
            (q_cost, q_error)
            for q_cost, q_error in executor.map(dry_run_query, table_diff_entities)
        ]
        costs, dry_run_errors = zip(*query_costs)

        model_zip = zip(model_names, table_diff_entities, costs, dry_run_errors)
        query_results = [res for res in executor.map(run_query, model_zip)]

    for model, _, cost, skipped, errored in query_results:
        print(
            "cost",
            model.ljust(35),
            "NA" if errored else f"{cost:.5f} GB",
            f"{skipped=}",
            f"{errored=}",
            sep="\t",
        )

    markdown_diff_per_model = [
        build_markdown_table_for_model(
            model, result, cost, skipped, errored, column_limit=result_limit
        )
        for model, result, cost, skipped, errored in query_results
    ]
    markdown_diff_report = "\n\n".join(markdown_diff_per_model)
    return markdown_diff_report


def extract_model_metadata(
    all_model_metadata: dict,
    base_bq_project: str,
    compare_bq_project: str,
    base_schema_prefix: str,
    compare_schema_prefix: str,
) -> dict:
    unique_key = all_model_metadata["config"]["unique_key"]
    dbt_schema = all_model_metadata["config"]["schema"]
    materialized = all_model_metadata["config"]["materialized"]
    model_name = all_model_metadata["alias"]

    partition_field = (
        all_model_metadata["config"]["partition_by"]["field"]
        if all_model_metadata["config"].get("partition_by", None)
        else None
    )

    model_meta = {
        "model": model_name,
        "materialized": materialized,
        "base_table": f"{base_bq_project}.{base_schema_prefix}{dbt_schema}.{model_name}",
        "compare_table": f"{compare_bq_project}.{compare_schema_prefix}{dbt_schema}.{model_name}",
        "unique_key": unique_key
        if type(unique_key) is list or unique_key is None
        else [unique_key],
        "partition_field": partition_field,
    }

    return model_meta


def initialize_table_diff(model_meta: dict) -> TableDiff:
    """
    Initialize table diff for a dbt model based on model metadata
    """
    table_diff_params = dict(
        t1=model_meta["base_table"], t2=model_meta["compare_table"]
    )

    if model_meta["unique_key"]:
        table_diff_params.update(dict(join_columns=model_meta["unique_key"]))

    if model_meta["partition_field"]:
        table_diff_params.update(
            dict(
                t1_filter=f"DATE({model_meta['partition_field']}) >= DATE('1990-01-01')",
                t2_filter=f"DATE({model_meta['partition_field']}) >= DATE('1990-01-01')",
            )
        )

    return TableDiff(**table_diff_params)


def _run_query_safe(
    table_diff_obj: TableDiff, dry_run: bool
) -> Tuple[Optional[TableDiffResultsStatistics], Optional[str]]:
    """
    Runs a table diff query wrapped in a try-catch, to gracefully catch query errors and return them
    """
    error_value = None
    result = None

    try:
        if dry_run:
            result = table_diff_obj.dry_run_query("statistics")
        else:
            result = table_diff_obj.run_query("statistics")
    except:
        error_type, error_value, traceback = sys.exc_info()
    return result, error_value


def dry_run_query(
    table_diff: TableDiff,
) -> Tuple[str, Optional[TableDiffResultsStatistics], float, bool]:
    result, error = _run_query_safe(table_diff, dry_run=True)
    return result, error


def run_query(
    model_tuple: Tuple[str, TableDiff, float, str]
) -> Tuple[str, Optional[TableDiffResultsStatistics], float, bool, str]:
    model, table_diff, query_cost, dry_run_error = model_tuple
    result = None
    error = None
    skipped = False
    if dry_run_error:
        error = dry_run_error
    elif query_cost < GB_MODEL_COST_LIMIT:
        result, error = _run_query_safe(table_diff, dry_run=False)
    else:
        skipped = True
    if error:
        print(f"QUERY ERROR FOR MODEL {model}", error, sep="\n")
    return model, result, query_cost, skipped, bool(error)


def build_markdown_table_for_model(
    model_name: str,
    results: TableDiffResultsStatistics,
    query_cost: float,
    skipped: bool,
    errored: bool,
    column_limit: int,
) -> str:
    """
    Builds a markdown output for the data diff based on the table diff results
    """
    model_marker = "🔸" if errored or skipped or results.is_diff else "🔹"
    model_report = f"### {model_marker} {model_name}"

    # if the model diff was skipped due to cost reasons, add message and return
    if model_name in MODEL_EXCLUSION_LIST:
        model_report = append_to_report(
            model_report,
            f"❗ **skipped!** model is blacklisted, e.g. too heavy to analyze",
        )
    elif errored:
        model_report = append_to_report(
            model_report,
            "❌ **error!** table diff query failed, check CI logs or contact Analytics Engineering for help",
        )

    # if the model diff was skipped due to cost reasons, add message and return
    elif skipped:
        model_report = append_to_report(
            model_report,
            f"❗ **skipped!** table diff query cost was {query_cost:.2f} GB; limit is {GB_MODEL_COST_LIMIT} GB",
        )

    # if the model diff yielded no differences, add message and return
    elif not results.is_diff:
        # Browse through diff summary to find row count (to avoid hardcoding an index which can change)
        row_count = None
        for key, value in results.diff_summary:
            if key == "count_matching_rows":
                row_count = value

        msg = f"*no data diff between t1 and t2 found for this model (examined {row_count} rows)*"

        # If diff was done on no rows, be louder
        if row_count == 0:
            msg = "❗ **diffed tables were both empty!** ❗ "

        model_report = append_to_report(
            model_report,
            msg,
        )
    else:
        # list removed columns
        if len(results.t1_only_columns) > 0:
            model_report = append_to_report(
                model_report,
                f"➖ **removed columns:** {results.t1_only_columns}",
            )

        # list new columns
        if len(results.t2_only_columns) > 0:
            model_report = append_to_report(
                model_report,
                f"➕ **new columns:** {results.t2_only_columns}",
            )

        # if no column diff granularity is provided, add message to encourage adding an unique key
        if len(results.diff_columns) == 0:
            model_report = append_to_report(
                model_report,
                "*`unique_key` not found between t1 & t2 -> can not provide column-level diff. Add a `unique_key` to this dbt model for granular diff*",
            )

        # add all statistics for the model
        model_report = append_table_diff_output_to_report(
            model_report=model_report,
            results=results,
            column_limit=column_limit,
        )

    model_report = append_to_report(model_report, "<br>")
    return model_report


def append_table_diff_output_to_report(
    model_report: str,
    results: TableDiffResultsStatistics,
    column_limit: int,
) -> str:
    """
    Appends a table diff output as a string to another string after a newline, aligned as much left a possible
    """
    l_align = 8
    r_align = 40

    # add all statistics for the model
    diff_columns_reduced = results.diff_columns[:column_limit]
    all_cols = results.diff_summary + diff_columns_reduced

    diffs = [
        f"| {str(round(value, 5)).ljust(l_align)} | {str(field).ljust(r_align)} |"
        for field, value in all_cols
    ]
    diff_str = "\n".join(diffs)

    model_report = f"""{model_report}
| {'Value'.ljust(l_align)} | {'Statistic'.ljust(r_align)} |
| {'-'*l_align} |:{'-'*r_align} |
{diff_str}
"""
    # if column diffs were truncated, add message to explain this
    if len(diff_columns_reduced) < len(results.diff_columns):
        model_report = append_to_report(
            model_report,
            f"*truncating at column limit {column_limit} ({len(results.diff_columns)} total)*",
        )
    return model_report


def append_to_report(report: str, str_to_append) -> str:
    """
    Appends a string to another string after a newline, aligned as much left a possible
    """
    report = f"""{report}
{str_to_append}
    """
    return report
