"""
library functions
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

LOG_FILE = "pyspark_output.md"


def start_spark(app_name: str, memory: str = "2g") -> SparkSession:
    """Initialize a Spark session with
    the specified application name and memory allocation."""
    session = (
        SparkSession.builder.appName(app_name)
        .config("spark.executor.memory", memory)
        .getOrCreate()
    )
    return session


def end_spark(spark):
    spark.stop()
    return "stopped spark session"


def read_csv(session: SparkSession, file_path: str) -> DataFrame:
    # Reading CSV file with header and inferring schema
    data_file = session.read.csv(file_path, header=True, inferSchema=True)
    return data_file


def log_output(operation, output, query=None):
    """adds to a markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is {operation}\n\n")
        if query:
            file.write(f"The query is {query}\n\n")
        file.write("The truncated output is: \n\n")
        file.write(output)
        file.write("\n\n")


def run_query(spark: SparkSession, df: DataFrame, query: str, view_name: str):
    """Run a Spark SQL query on a DataFrame and log the output."""
    df.createOrReplaceTempView(view_name)
    result = spark.sql(query)
    log_output("Run Query", result.toPandas().to_markdown(), query)
    return result.show()


def describe(df: DataFrame):
    """Generate summary statistics of the DataFrame and log the output."""
    summary_stats_str = df.describe().toPandas().to_markdown()
    log_output("Describe Data", summary_stats_str)
    return df.describe().show()


def example_transform(df: DataFrame) -> DataFrame:
    """An example transformation ."""
    conditions = [
        (F.col("type1") == "water"),
        (F.col("type1") == "fire"),
        (F.col("type1") == "grass"),
    ]
    categories = ["Water", "Fire", "Grass"]

    # Apply transformation to add the 'Type_Category' column
    df = df.withColumn(
        "Type_Category",
        F.when(conditions[0], categories[0])
        .when(conditions[1], categories[1])
        .when(conditions[2], categories[2])
        .otherwise("Other"),
    )

    log_output("Example Transform", df.limit(10).toPandas().to_markdown())
    return df
