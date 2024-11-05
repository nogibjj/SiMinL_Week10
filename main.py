from mylib.lib import (
    start_spark,
    end_spark,
    read_csv,
    describe,
    run_query,
    example_transform,
)


def main():
    # Start Spark session
    spark = start_spark("PokemonAnalysis", memory="2g")

    # Define the path to your CSV file
    csv_file_path = "data/pokemon.csv"

    # Read the CSV file
    df = read_csv(spark, csv_file_path)

    # Describe the data
    describe(df)

    # Run a Spark SQL query
    run_query(
        spark,
        df,
        "SELECT name FROM pokemon WHERE abilities LIKE '%Beast Boost%'",
        "pokemon",
    )

    # Example transformation (uncomment if you want to apply it)
    example_transform(df)

    # End Spark session
    end_spark(spark)


if __name__ == "__main__":
    main()
