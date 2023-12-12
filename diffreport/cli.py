from argparse import ArgumentParser

from pyutils.diffreport.reportbuilder import create_diffreport


def get_argparser() -> ArgumentParser:
    parser = ArgumentParser(
        description="Build diff report for multiple dbt models by using table diff"
    )
    parser.add_argument(
        "--changed-models",
        required=True,
        help="whitespace separated list of dbt models to diff",
        nargs="+",
        type=str,
    )
    parser.add_argument(
        "--base-schema", help="the base schema to compare against", type=str, default=""
    )
    parser.add_argument(
        "--compare-schema",
        help="the comparator schema to compare against base",
        type=str,
        default="",
    )

    parser.add_argument(
        "--base-bq-project",
        help="BigQuery project for the base schema",
        type=str,
        default="dwh-dev",
    )
    parser.add_argument(
        "--compare-bq-project",
        help="BigQuery project for the comparator schema",
        type=str,
        default="dwh-dev",
    )
    parser.add_argument(
        "--output", help="optional path to file to output the report to", type=str
    )
    return parser


def main():
    parser = get_argparser()
    parsed_args = parser.parse_args()

    report = create_diffreport(
        parsed_args.changed_models,
        base_bq_project=parsed_args.base_bq_project,
        compare_bq_project=parsed_args.compare_bq_project,
        base_schema_prefix=parsed_args.base_schema,
        compare_schema_prefix=parsed_args.compare_schema,
    )
    print(report)
    if parsed_args.output:
        with open(parsed_args.output, "w") as f:
            f.write(report)
        print(f"report saved to {parsed_args.output}")


if __name__ == "__main__":
    main()
