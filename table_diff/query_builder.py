import textwrap
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class ColumnMetadata:
    name: str
    t1_type: str
    t2_type: str


class QueryBuilder:
    """
    Parent class for setting up common logic for building table diff queries
    """

    def __init__(
        self,
        t1: str,
        t2: str,
        diff_columns: List[ColumnMetadata],
        t1_filter: str,
        t2_filter: str,
        join_columns: Optional[List[str]] = None,
    ):
        self.t1 = t1
        self.t2 = t2
        self.t1_filter = t1_filter
        self.t2_filter = t2_filter
        self.diff_columns = diff_columns
        self.join_columns = join_columns

    def _fmt_column(self, column: str) -> str:
        return column.replace(".", "__")

    def build_unmatching_rows_query(self) -> str:
        raise NotImplementedError()

    def build_unmatching_rows_statistics_query(self) -> str:
        raise NotImplementedError()


class JoinQueryBuilder(QueryBuilder):
    """
    QueryBuilder class for building a table diff query,
    utilizing the FULL OUTER JOIN method on a particular,
    supplied join key.
    """

    def build_unmatching_rows_query(self) -> str:
        return self._build_base_query() + f"SELECT * FROM final WHERE NOT row_match"

    def build_unmatching_rows_statistics_query(self) -> str:
        diff_columns_only = self._diff_columns_only()
        count_column_diffs = [
            f"""
            COUNTIF({self._fmt_column(col.name)} IS NOT NULL) AS count_{self._fmt_column(col.name)}_non_match"""
            for col in diff_columns_only
        ]
        count_column_diffs = ",".join(count_column_diffs)
        statistic_query_head = textwrap.dedent(
            f"""
        ,stats AS (
            SELECT
                STRUCT(
                    COUNTIF(ARRAY_LENGTH(origin) < 2) AS count_unmatching_joins,
                    COUNTIF(NOT row_match) AS count_unmatching_rows,
                    COUNTIF(row_match) AS count_matching_rows,
                    100*(COUNTIF(NOT row_match)/GREATEST(COUNT(*), 1)) AS unmatching_rows_percentage,
                    COUNTIF('t1' IN UNNEST(origin)) AS count_t1,
                    COUNTIF('t2' IN UNNEST(origin)) AS count_t2
                ) AS diff_summary,
                STRUCT(
                    {count_column_diffs}
                ) AS diff_columns,
            FROM final
        )
        SELECT * FROM stats
            """
        )
        return self._build_base_query() + statistic_query_head

    def _diff_columns_only(self):
        return [e for e in self.diff_columns if e.name not in self.join_columns]

    def _build_base_query(self) -> str:
        column_selection = [
            f"""
            {col.name} AS {self._fmt_column(col.name)},"""
            for col in self.diff_columns
        ]
        column_selection = "".join(column_selection)

        join_columns_coalesced = [
            f"""
            COALESCE(t1.{col}, t2.{col}) AS {col},"""
            for col in self.join_columns
        ]
        join_columns_coalesced = "".join(join_columns_coalesced)

        join_clause = (
            f"ON {' AND '.join([f't1.{jc} = t2.{jc}' for jc in self.join_columns])}"
        )

        diff_columns_only = self._diff_columns_only()
        column_comparisons = [
            self._build_column_comparison(col) for col in diff_columns_only
        ]
        column_comparisons = "".join(column_comparisons)

        return textwrap.dedent(
            f"""
            WITH
            t1 AS (
                SELECT
                    {column_selection}
                FROM `{self.t1}`
                WHERE {self.t1_filter}
            ),
            t2 AS (
                SELECT
                    {column_selection}
                FROM `{self.t2}`
                WHERE {self.t2_filter}
            ),
            final AS (
                SELECT
                    {join_columns_coalesced}
                    COALESCE(
                        TO_HEX(MD5(TO_JSON_STRING(t1))) = TO_HEX(MD5(TO_JSON_STRING(t2))),
                        FALSE
                    ) AS row_match,
                    (
                        SELECT 
                            ARRAY_AGG(x IGNORE NULLS) 
                            FROM UNNEST([
                                IF(t1.{self.join_columns[0]} IS NULL, NULL, 't1'),
                                IF(t2.{self.join_columns[0]} IS NULL, NULL, 't2')
                            ]) AS x
                    ) AS origin,
                    {column_comparisons}
                FROM t1
                FULL OUTER JOIN t2
                    {join_clause}
            )
        """
        )

    def _build_column_comparison(self, column: ColumnMetadata) -> str:
        if column.t1_type != column.t2_type or column.t1_type.startswith("ARRAY"):
            condition = f"TO_JSON_STRING(t1.{self._fmt_column(column.name)}) = TO_JSON_STRING(t2.{self._fmt_column(column.name)})"
        else:
            condition = f"t1.{self._fmt_column(column.name)} = t2.{self._fmt_column(column.name)}"

        comparison = f"""
        IF(({condition}) OR (t1.{self._fmt_column(column.name)} IS NULL AND t2.{self._fmt_column(column.name)} IS NULL),
            NULL,
            STRUCT(t1.{self._fmt_column(column.name)} AS t1, t2.{self._fmt_column(column.name)} AS t2)
        ) AS {self._fmt_column(column.name)},"""
        return comparison


class UnionQueryBuilder(QueryBuilder):
    """
    QueryBuilder class for building a table diff query,
    utilizing the UNION method on a particular.
    """

    def build_unmatching_rows_query(self) -> str:
        return self._build_base_query() + f"SELECT * FROM final"

    def build_unmatching_rows_statistics_query(self) -> str:
        statistic_query_head = textwrap.dedent(
            f"""
            ,count_unmatching AS (
                SELECT
                    COUNT(*) AS cnt
                FROM final
            ),
            union_table_stats AS (
                SELECT
                    count_unmatching.cnt AS count_unmatching_rows,
                    CAST(
                        (t1_stats.cnt + t2_stats.cnt + count_unmatching.cnt)/2 - count_unmatching.cnt AS int64
                    ) AS count_matching_rows,
                    t1_stats.cnt AS count_t1,
                    t2_stats.cnt AS count_t2
                FROM count_unmatching
                CROSS JOIN (SELECT COUNT(*) AS cnt FROM t1) AS t1_stats
                CROSS JOIN (SELECT COUNT(*) AS cnt FROM t2) AS t2_stats
            ),
            stats AS (
                SELECT
                    STRUCT(
                        count_unmatching_rows,
                        count_matching_rows,
                        100*(count_unmatching_rows/GREATEST(count_matching_rows + count_unmatching_rows, 1)) AS unmatching_rows_percentage,
                        count_t1,
                        count_t2
                    ) AS diff_summary,
                    NULL AS diff_columns
                FROM union_table_stats
            )
            SELECT * FROM stats
            """
        )
        return self._build_base_query() + statistic_query_head

    def _build_base_query(self) -> str:
        column_selection = [self._build_column_selection(e) for e in self.diff_columns]
        column_selection = "".join(column_selection)

        return textwrap.dedent(
            f"""
            WITH
            t1 AS (
                SELECT DISTINCT
                    {column_selection}
                FROM {self.t1}
                WHERE {self.t1_filter}
            ),
            t2 AS (
                SELECT DISTINCT
                    {column_selection}
                FROM {self.t2} AS t2
                WHERE {self.t2_filter}
            ),
            t1_distinct AS (
                SELECT * FROM t1
                EXCEPT DISTINCT
                SELECT * FROM t2
            ),
            t2_distinct AS (
                SELECT * FROM t2
                EXCEPT DISTINCT
                SELECT * FROM t1
            ),
            final AS (
                SELECT ['t1'] AS origin, * FROM t1_distinct
                UNION ALL
                SELECT ['t2'] AS origin, * FROM t2_distinct
            )
        """
        )

    def _build_column_selection(self, column: ColumnMetadata) -> str:
        if column.t1_type != column.t2_type or column.t1_type.startswith("ARRAY"):
            select = f"""
                    TO_JSON_STRING({column.name}) AS {self._fmt_column(column.name)},"""
        else:
            select = f"""
                    {column.name} AS {self._fmt_column(column.name)},"""
        return select
