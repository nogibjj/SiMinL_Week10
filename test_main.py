"""
Test goes here

"""

import pytest
import os
from pyspark.sql import SparkSession

from mylib.lib import (
    start_spark,
    end_spark,
    read_csv,
    describe,
    run_query,
    example_transform,
)

# Sample CSV path for testing
TEST_CSV_FILE_PATH = "data/pokemon.csv"


@pytest.fixture(scope="module")
def spark():
    """Fixture to initialize and
    tear down the Spark session for all tests."""
    spark_session = start_spark("TestApp")
    yield spark_session
    end_spark(spark_session)


def test_csv_file_exists():
    """Test to ensure the CSV file path exists for testing."""
    assert os.path.exists(TEST_CSV_FILE_PATH), "CSV file does not exist."


def test_read_csv():
    spark = start_spark(app_name="PySpark Data Processing")
    df = read_csv(spark, TEST_CSV_FILE_PATH)
    assert df.count() > 0, "Test failed."
    assert "name" in df.columns
    print("CSV file reading test passed successfully.")


def test_describe():
    spark = start_spark(app_name="PySpark Data Processing")
    df = read_csv(spark, TEST_CSV_FILE_PATH)
    result = describe(df)
    assert result is None


def test_run_query():
    # Create SparkSession for testing
    spark = SparkSession.builder.appName("Spark SQL Query Test").getOrCreate()
    df = read_csv(spark, TEST_CSV_FILE_PATH)
    result = run_query(
        spark,
        df,
        "SELECT name FROM pokemon WHERE abilities LIKE '%Beast Boost%'",
        "pokemon",
    )
    assert result is None


def test_example_transform():
    spark = SparkSession.builder.appName("Spark SQL Query Test").getOrCreate()
    df = read_csv(spark, TEST_CSV_FILE_PATH)
    result = example_transform(df)
    assert "Type_Category" in result.columns


if __name__ == "__main__":
    spark()
    test_csv_file_exists()
    test_read_csv()
    test_describe()
    test_run_query()
    test_example_transform()
