from argparse import ArgumentParser
from pyutils.table_diff.app import TableDiff


def get_argparser(description: str) -> ArgumentParser:
    return ArgumentParser(description=description)


def add_table_diff_arguments(parser: ArgumentParser) -> ArgumentParser:

    parser.add_argument(
        "--output",
        "-o",
        choices=["statistics", "rows"],
        required=True,
        help="Return statistics of the diff between tables, or the actual differing rows.",
    )

    parser.add_argument("--t1", required=True, help="Table 1 to use in comparison")
    parser.add_argument("--t2", required=True, help="Table 2 to use in comparison")

    table_filter_help = "Optional filter to apply to table {}. Passed directly to the `WHERE` clause in the SQL statement."
    parser.add_argument("--filter", help=table_filter_help.format("1 & 2"))
    parser.add_argument(
        "--t1-filter", help=table_filter_help.format("1") + " Overrides `--filter ...`"
    )
    parser.add_argument(
        "--t2-filter", help=table_filter_help.format("2") + " Overrides `--filter ...`"
    )

    parser.add_argument(
        "--join-on",
        help='Unique join keys to use to join the two tables to diff. Usage: `--join-on="col1,col2"`.',
    )
    parser.add_argument(
        "--include",
        help=(
            'Columns to compare in the table diff. Usage: `--include="col1,col2"`." '
        ),
    )
    parser.add_argument(
        "--exclude",
        help=(
            'Columns to exclude in the table diff. Usage: `--exclude="col1,col2"`." '
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run cost of query, and print query without running it.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Force bypass query cost check",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit return rows from table-diff when run in 'rows' mode.",
    )

    return parser


def run_table_diff(parsed_args) -> None:

    table_diff_kwargs = {}
    if parsed_args.t1_filter:
        table_diff_kwargs["t1_filter"] = parsed_args.t1_filter
    if parsed_args.t2_filter:
        table_diff_kwargs["t2_filter"] = parsed_args.t2_filter

    if parsed_args.filter and parsed_args.t1_filter is None:
        table_diff_kwargs["t1_filter"] = parsed_args.filter
    if parsed_args.filter and parsed_args.t2_filter is None:
        table_diff_kwargs["t2_filter"] = parsed_args.filter

    if parsed_args.include:
        table_diff_kwargs["include_columns"] = [
            e.strip() for e in parsed_args.include.split(",")
        ]

    if parsed_args.exclude:
        table_diff_kwargs["exclude_columns"] = [
            e.strip() for e in parsed_args.exclude.split(",")
        ]

    if parsed_args.limit:
        table_diff_kwargs["result_limit"] = parsed_args.limit

    if parsed_args.join_on:
        join_columns = [e.strip() for e in parsed_args.join_on.split(",")]
    else:
        join_columns = None

    table_diff = TableDiff(
        t1=parsed_args.t1,
        t2=parsed_args.t2,
        join_columns=join_columns,
        **table_diff_kwargs
    )

    is_diff, message = table_diff.do_table_comparison(
        output_mode=parsed_args.output,
        dry_run=parsed_args.dry_run,
        force_query=parsed_args.force,
    )

    print(message)
    return is_diff, message


def run():
    description = "Script args for doing a table diff between two BigQuery tables."
    parser = get_argparser(description)
    parser = add_table_diff_arguments(parser)
    parsed_args = parser.parse_args()
    run_table_diff(parsed_args)


if __name__ == "__main__":
    run()
