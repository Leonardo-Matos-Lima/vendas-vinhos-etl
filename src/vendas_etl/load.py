from pyspark.sql import DataFrame


def load_parquet(df: DataFrame, output_path: str) -> None:
    """
    Saves a Spark DataFrame to Parquet format.

    The overwrite mode replaces existing data at the specified output path.

    Args:
        df: Spark DataFrame to be saved.
        output_path: Destination path for the Parquet files.

    Returns:
        None.
    """
    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )
    return None