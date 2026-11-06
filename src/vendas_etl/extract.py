
from pyspark.sql import SparkSession, DataFrame

def extract(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Extracts sales data from a CSV file.

    Args:
        spark: Active Spark session.
    file_path: Path to the source CSV file.

    Returns:
        DataFrame containing the extracted sales data.
    """
    df = (
        spark.read
        .format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .option("sep", ",")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", True)
        .load(file_path)
    )
    
    return df
