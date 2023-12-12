from pyutils.table_diff.app import TableDiff
from google.cloud import bigquery

t1 = "dwh-dev.johanblad.table_diff_test_t1"
t2 = "dwh-dev.johanblad.table_diff_test_t2"
join_key = ["unique_key"]


def main():
    # create_tables()
    run_join_statistics()
    run_join_rows()
    run_union_statistics()
    run_union_rows()


def run_join_statistics():
    table_diff_join = TableDiff(t1=t1, t2=t2, join_columns=join_key)

    q = table_diff_join.build_query("statistics")
    table_diff_join.dry_run_query("statistics")
    res = table_diff_join.run_query("statistics")
    msg = table_diff_join.build_result_message(res, "statistics")
    print(msg)


def run_join_rows():
    table_diff_join = TableDiff(t1=t1, t2=t2, join_columns=join_key)

    q = table_diff_join.build_query("rows")
    table_diff_join.dry_run_query("rows")
    res = table_diff_join.run_query("rows")
    msg = table_diff_join.build_result_message(res, "rows")
    print(msg)


def run_union_statistics():
    table_diff_union = TableDiff(t1=t1, t2=t2)

    q = table_diff_union.build_query("statistics")
    table_diff_union.dry_run_query("statistics")
    res = table_diff_union.run_query("statistics")
    msg = table_diff_union.build_result_message(res, "statistics")
    print(msg)


def run_union_rows():
    table_diff_union = TableDiff(t1=t1, t2=t2)

    q = table_diff_union.build_query("rows")
    table_diff_union.dry_run_query("rows")
    res = table_diff_union.run_query("rows")
    msg = table_diff_union.build_result_message(res, "rows")
    print(msg)


# compliments of ChatGPT for the dummy data
Q_t1 = f"""
    CREATE OR REPLACE TABLE `{t1}` (
        array_of_struct ARRAY<STRUCT<a STRING, b INT64>>,
        date_timestamp DATE,
        unique_key STRING,
        struct_plain STRUCT<a STRING, b INT64>,
        array_of_struct_of_array ARRAY<STRUCT<a STRING, b ARRAY<STRING>>>
    );

    -- Insert 5 rows of dummy data
    INSERT INTO `{t1}`
    (array_of_struct, date_timestamp, unique_key, struct_plain, array_of_struct_of_array) VALUES
    (
        ARRAY[STRUCT("abc", 1), STRUCT("def", 2), STRUCT("ghi", 3)],
        DATE '2020-01-01',
        'unique_key_1',
        STRUCT("jkl", 4),
        ARRAY[STRUCT("mno", ["p", "q"]), STRUCT("rst", ["u", "v"]), STRUCT("wxy", ["z", "a"])]
    ),
    (
        ARRAY[STRUCT("ghi", 5), STRUCT("jkl", 6), STRUCT("mno", 7)],
        DATE '2020-01-02',
        'unique_key_2',
        STRUCT("pqr", 8),
        ARRAY[STRUCT("stu", ["v", "w"]), STRUCT("xyz", ["a", "b"]), STRUCT("cde", ["f", "g"])]
    ),
    (
        ARRAY[STRUCT("vwx", 9), STRUCT("yz1", 10), STRUCT("234", 11)],
        DATE '2020-01-03',
        'unique_key_3',
        STRUCT("567", 12),
        ARRAY[STRUCT("890", ["1", "2"]), STRUCT("jkl", ["3", "4"]), STRUCT("mno", ["5", "6"])]
    ),
    (
        ARRAY[STRUCT("pqr", 13), STRUCT("stu", 14), STRUCT("vwx", 15)],
        DATE '2020-01-04',
        'unique_key_4',
        STRUCT("yz1", 16),
        ARRAY[STRUCT("234", ["5", "6"]), STRUCT("789", ["0", "1"]), STRUCT("abc", ["d", "e"])]
    ),
    (
        ARRAY[STRUCT("def", 17), STRUCT("ghi", 18), STRUCT("jkl", 19)],
        DATE '2020-01-05',
        'unique_key_5',
        STRUCT("mno", 20),
        ARRAY[STRUCT("pqr", ["s", "t"]), STRUCT("uvw", ["x", "y"]), STRUCT("z12", ["3", "4"])]
    );
"""

Q_t2 = f"""
    CREATE OR REPLACE TABLE `{t2}` (
        array_of_struct ARRAY<STRUCT<a STRING, b INT64>>,
        date_timestamp TIMESTAMP,
        unique_key STRING,
        struct_plain STRUCT<a STRING, b INT64>,
        array_of_struct_of_array ARRAY<STRUCT<a STRING, b ARRAY<STRING>>>
    );

    -- Insert 5 rows of dummy data
    INSERT INTO `{t2}`
    (array_of_struct, date_timestamp, unique_key, struct_plain, array_of_struct_of_array) VALUES
    (
        ARRAY[STRUCT("abc", 1), STRUCT("def", 2), STRUCT("ghi", 3)],
        TIMESTAMP '2020-01-01',
        'unique_key_1',
        STRUCT("jkl", 4),
        ARRAY[STRUCT("mno", ["p", "q"]), STRUCT("rst", ["u", "v"]), STRUCT("wxy", ["z", "a"])]
    ),
    (
        ARRAY[STRUCT("ghi", 5), STRUCT("jkl", 6), STRUCT("mno", 7)],
        TIMESTAMP '2020-01-02',
        'unique_key_2',
        STRUCT("pqr", 8),
        ARRAY[STRUCT("stu", ["v", "w"]), STRUCT("xyz", ["a", "b"]), STRUCT("cde", ["f", "g"])]
    ),
    (
        ARRAY[STRUCT("vwx", 9), STRUCT("yz1", 10), STRUCT("234", 11)],
        TIMESTAMP '2020-01-03',
        'unique_key_3',
        STRUCT("567", 12),
        ARRAY[STRUCT("890", ["1", "2"]), STRUCT("jkl", ["3", "4"]), STRUCT("mno", ["5", "6"])]
    ),
    (
        ARRAY[STRUCT("pqr", 13), STRUCT("stu", 14), STRUCT("vwx", 15)],
        TIMESTAMP '2020-01-04',
        'unique_key_4',
        STRUCT("yz1", 16),
        ARRAY[STRUCT("234", ["5", "6"]), STRUCT("789", ["0", "1"]), STRUCT("abc", ["d", "e"])]
    ),
    (
        ARRAY[STRUCT("def", 17), STRUCT("ghi", 18), STRUCT("jkl", 19)],
        TIMESTAMP '2020-01-05',
        'unique_key_5',
        STRUCT("__mno", 20),
        ARRAY[STRUCT("__pqr", ["s", "t"]), STRUCT("__uvw", ["x", "y"]), STRUCT("z12", ["3", "4"])]
    );

"""


def create_tables():
    client = bigquery.Client()
    print("create t1")
    client.query(Q_t1).result()
    print("create t2")
    client.query(Q_t2).result()


if __name__ == "__main__":
    main()
