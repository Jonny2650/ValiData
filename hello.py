"""Testing linting and formatting tools."""

import polars as pl
from connect import create_db_uri, create_simple_query, create_complex_query, read_from_db, read_from_csv



def main() -> None:
    """
    Main function to print a greeting message.

    Returns:
        None
    """

    test_schema = pl.Schema(
        {
            "header 1": pl.Int64,
            "header 2": pl.Int64,
            "header 3": pl.Int64
        }
    )

    test_1 = read_from_csv(file_path="test_csv_data/test_1.csv", schema=test_schema)
    test_2 = read_from_csv(file_path="test_csv_data/test_2.csv", schema=test_schema)
    test_3 = read_from_csv(file_path="test_csv_data/test_3.csv", schema=test_schema)

    test_1_df = test_1.collect()
    test_2_df = test_2.collect()
    test_3_df = test_3.collect()

    if test_1_df.equals(test_3_df):
        print("DataFrames are equal")
    else:
        print("DataFrames are not equal")

    return None


if __name__ == "__main__":
    main()

