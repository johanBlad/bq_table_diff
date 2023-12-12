import math
import textwrap
from dataclasses import dataclass
from typing import List, Literal, Optional, Tuple, Union

from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator
from pyutils.table_diff.query_builder import (
    JoinQueryBuilder,
    UnionQueryBuilder,
    ColumnMetadata,
)


@dataclass
class TableDiffResultsRow:
    is_diff: bool
    row_iterator: Union[RowIterator, _EmptyRowIterator]


@dataclass
class TableDiffResultsStatistics:
    is_diff: bool
    diff_summary: List[Tuple[str, Union[int, float]]]
    diff_columns: List[Tuple[str, Union[int, float]]]
    t1_only_columns: List[str]
    t2_only_columns: List[str]


class TableDiff:
    """
    Main class for setting up a diff operation between two tables.
    Queries the BigQuery `INFORMATION_SCHEMA` to get metadata about
    the tables and columns, to be able to diff them properly.
    """

    DEFAULT_EXCLUDE_COLUMNS: List[str] = ["insertion_ts"]
    QUERY_SCAN_GB_PROMPT_USER: int = 100

    def __init__(
        self,
        t1: str,
        t2: str,
        join_columns: Optional[List[str]] = None,
        t1_filter: str = "true",
        t2_filter: str = "true",
        include_columns: Optional[List[str]] = None,
        exclude_columns: List[str] = None,
        result_limit: int = 100,
    ):
        self.bq_client = bigquery.Client()
        self._t1 = t1
        self._t2 = t2
        self._t1_filter = t1_filter
        self._t2_filter = t2_filter

        if not exclude_columns:
            exclude_columns = []

        self._result_limit = result_limit

        (
            self._t1_t2_columns,
            self._t1_columns,
            self._t2_columns,
        ) = self._get_diff_columns(
            t1=self._t1,
            t2=self._t2,
            exclude_columns=exclude_columns + TableDiff.DEFAULT_EXCLUDE_COLUMNS,
            include_columns=include_columns,
        )

        query_builder_args = dict(
            t1=self._t1,
            t2=self._t2,
            diff_columns=self._t1_t2_columns,
            join_columns=join_columns,
            t1_filter=self._t1_filter,
            t2_filter=self._t2_filter,
        )
        use_join_method = self._join_method_available(join_columns, self._t1_t2_columns)
        if use_join_method:
            self.query_builder = JoinQueryBuilder(**query_builder_args)
        else:
            self.query_builder = UnionQueryBuilder(**query_builder_args)

        print(
            f"Running TableDiff with method='{'join' if use_join_method else 'union'}'"
        )

    def do_table_comparison(
        self,
        output_mode: Literal["statistics", "rows"],
        dry_run: bool,
        force_query: bool,
    ) -> Tuple[bool, str]:
        """Run the actual comparison"""
        if dry_run:
            print(self.build_query(output_mode))

        dry_run_gb = self.dry_run_query(output_mode)
        print(f"This query will process {dry_run_gb:.3f} GB.")

        if not dry_run:
            if dry_run_gb > self.QUERY_SCAN_GB_PROMPT_USER and not force_query:
                if input("Continue? [y/n]: ") != "y":
                    exit(0)
            res = self.run_query(output_mode)
            message = self.build_result_message(res, output_mode)
            return res.is_diff, message
        return None, ""

    def _join_method_available(
        self, join_columns: List[str], common_columns: List[str]
    ) -> bool:
        if join_columns:
            common_column_names = [e.name for e in common_columns]
            mismatching_join_columns = [
                col for col in join_columns if col not in common_column_names
            ]

            if len(mismatching_join_columns) == 0:
                return True
            print(
                f"Warning! Columns '{', '.join(mismatching_join_columns)}' not in both tables."
            )
        return False

    def build_query(self, output_mode: Literal["statistics", "rows"]) -> str:
        if output_mode == "statistics":
            return self.query_builder.build_unmatching_rows_statistics_query()
        return self.query_builder.build_unmatching_rows_query()

    def dry_run_query(
        self,
        output_mode: Literal["statistics", "rows"],
    ) -> float:
        query = self.build_query(output_mode)
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        dry_run_job = self.bq_client.query(query, job_config=job_config)
        return dry_run_job.total_bytes_processed / math.pow(1024, 3)

    def run_query(
        self,
        output_mode: Literal["statistics", "rows"],
    ) -> Union[TableDiffResultsRow, TableDiffResultsStatistics]:
        if output_mode == "statistics":
            return self._run_unmatching_rows_statistics_query()
        return self._run_unmatching_rows_query()

    def _run_unmatching_rows_query(self) -> TableDiffResultsRow:
        query = self.build_query("rows")
        row_iterator = self.bq_client.query(query).result()
        return TableDiffResultsRow(
            is_diff=row_iterator.total_rows > 0, row_iterator=row_iterator
        )

    def _run_unmatching_rows_statistics_query(self) -> TableDiffResultsStatistics:
        query = self.build_query("statistics")
        row_iterator = self.bq_client.query(query).result()
        first_row = next(row_iterator)
        if first_row.diff_columns:
            diff_columns = sorted(
                first_row.diff_columns.items(),
                key=lambda e: e[1],
                reverse=True,
            )
        else:
            diff_columns = []

        is_diff = (
            first_row.diff_summary["count_unmatching_rows"] > 0
            or len(self._t1_columns) > 0
            or len(self._t2_columns) > 0
        )
        return TableDiffResultsStatistics(
            is_diff=is_diff,
            diff_summary=list(first_row.diff_summary.items()),
            diff_columns=list(diff_columns),
            t1_only_columns=[f"{col.name}:{col.t1_type}" for col in self._t1_columns],
            t2_only_columns=[f"{col.name}:{col.t2_type}" for col in self._t2_columns],
        )

    def build_result_message(
        self,
        diff_results: Union[TableDiffResultsRow, TableDiffResultsStatistics],
        output_mode: Literal["statistics", "rows"],
    ) -> str:
        if output_mode == "statistics":
            return self._build_unmatching_rows_statistics_message(diff_results)
        return self._build_unmatching_rows_message(diff_results)

    def _build_unmatching_rows_message(self, table_diff: TableDiffResultsRow) -> str:
        total_rows = table_diff.row_iterator.total_rows
        message_list = []
        for i, row in enumerate(table_diff.row_iterator):
            value = {k: str(v) for k, v in row.items() if v is not None}
            message_list.append(f"{value}")
            if i >= self._result_limit:
                message_list.append(
                    f"Truncating after limit {self._result_limit} reached"
                )
                break
        message_list.append(f"\nTotal result contained {total_rows} records")
        message = "\n".join(message_list)
        return message

    def _build_unmatching_rows_statistics_message(
        self,
        diff_results: TableDiffResultsStatistics,
    ) -> str:
        message_list = []
        message_list = message_list + [
            f"{str(round(value, 5)).ljust(10)}\t{key}"
            for key, value in diff_results.diff_summary
        ]

        message_list = message_list + [
            f"{str(round(value, 5)).ljust(10)}\t{key}"
            for key, value in diff_results.diff_columns[: self._result_limit]
        ]
        if len(diff_results.diff_columns) > self._result_limit:
            message_list.append(
                f"truncating at column limit {self._result_limit} ({len(diff_results.diff_columns)} total)"
            )
        if len(diff_results.t1_only_columns) > 0:
            message_list.append(
                f"{'t1 exclusive'.ljust(10)}\t{diff_results.t1_only_columns}"
            )

        if len(diff_results.t2_only_columns) > 0:
            message_list.append(
                f"{'t2 exclusive'.ljust(10)}\t{diff_results.t2_only_columns}"
            )
        message = "\n".join(message_list)
        return message

    def _get_diff_columns(
        self,
        t1: str,
        t2: str,
        exclude_columns: List[str],
        include_columns: List[str],
    ) -> Tuple[List[ColumnMetadata], List[ColumnMetadata], List[ColumnMetadata]]:
        """Determine which columns to do the diff on, taking into account any explicit include/exclude columns.
        Return a tuple of 1) list of colum names to diff, 2) list of column names that require stringification, i.e. repeated structs"""
        t1_project, t1_dataset, t1_table = t1.split(".")
        t2_project, t2_dataset, t2_table = t2.split(".")

        column_name_query = textwrap.dedent(
            f"""
            WITH
            t1_columns AS (
                SELECT
                    field_path, data_type
                FROM `{t1_project}.{t1_dataset}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
                WHERE table_name = '{t1_table}'
            ),
            t2_columns AS (
                SELECT
                    field_path, data_type
                FROM `{t2_project}.{t2_dataset}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
                WHERE table_name = '{t2_table}'
            ),
            join_columns AS (
                SELECT
                    COALESCE(t1_columns.field_path, t2_columns.field_path) AS field_path,
                    t1_columns.data_type AS t1_data_type, 
                    t2_columns.data_type AS t2_data_type,
                    t1_columns.field_path IS NOT NULL AND t2_columns.field_path IS NULL AS t1_only,
                    t1_columns.field_path IS NULL AND t2_columns.field_path IS NOT NULL AS t2_only,
                FROM t1_columns
                FULL OUTER JOIN t2_columns
                    USING (field_path)
            )
            SELECT * FROM join_columns
        """
        )

        row_iterator = self.bq_client.query(column_name_query).result()

        structs = []
        arrays = []
        t1_t2_columns = []
        t1_only_columns = []
        t2_only_columns = []
        # skip all column fields that starts with any of the specific 'exclude_columns'
        # if explicit 'include_columns' are set, only add columns that starts with any of the specific 'include_columns'
        # also, record all fields that are either structs or arrays
        for name, t1_type, t2_type, t1_only, t2_only in row_iterator:
            if any([str(name).startswith(excl) for excl in exclude_columns]):
                continue

            if not include_columns or any(
                [name.startswith(incl) for incl in include_columns]
            ):
                if t1_only:
                    t1_only_columns.append(ColumnMetadata(name, t1_type, t2_type))
                elif t2_only:
                    t2_only_columns.append(ColumnMetadata(name, t1_type, t2_type))
                else:
                    t1_t2_columns.append(ColumnMetadata(name, t1_type, t2_type))

            if not (t1_only or t2_only):
                if t1_type.startswith("STRUCT") and t2_type.startswith("STRUCT"):
                    structs.append(name)
                if t1_type.startswith("ARRAY") and t2_type.startswith("ARRAY"):
                    arrays.append(name)

        # remove all root structs, as their children will automatically be selected for the diff.
        t1_t2_columns = [e for e in t1_t2_columns if e.name not in structs]
        # also, remove all subfields of array structs, since the arrays themselves will be diffed
        t1_t2_columns = [
            e
            for e in t1_t2_columns
            if not any([e.name.startswith(arr) and e.name != arr for arr in arrays])
        ]

        return t1_t2_columns, t1_only_columns, t2_only_columns
