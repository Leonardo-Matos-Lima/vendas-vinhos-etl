from pyspark.sql import DataFrame

from pyspark.sql.functions import col, sum, lit, make_date


def standardize_columns(df: DataFrame) -> DataFrame:
    """
    Standardizes DataFrame column names by replacing spaces with underscores.

    Args:
        df: Input Spark DataFrame.

    Returns:
        DataFrame with standardized column names.
    """
    for coluna in df.columns:
        nova_coluna = coluna.replace(" ", "_")
        df = df.withColumnRenamed(coluna, nova_coluna)

    return df


def cast_columns(df: DataFrame) -> DataFrame:
    """
    Converts specified DataFrame columns to their appropriate data types.

    Args:
        df: Input Spark DataFrame.

    Returns:
        DataFrame with columns cast to the specified data types.
    """
    colunas_numericas = {
        "YEAR": "integer",
        "MONTH": "integer",
        "RETAIL_SALES": "double",
        "RETAIL_TRANSFERS": "double",
        "WAREHOUSE_SALES": "double"
    }

    for coluna, tipo in colunas_numericas.items():
        df = df.withColumn(coluna, col(coluna).cast(tipo))

    return df


def analyze_nulls(df: DataFrame) -> DataFrame:
    """
    Counts the number of null values in each DataFrame column.

    Args:
        df: Input Spark DataFrame.

    Returns:
        DataFrame containing the null count for each column.
    """
    null_counts = df.select(
        *[
            sum(col(c).isNull().cast("integer")).alias(c)
            for c in df.columns
        ]
    )
    return null_counts


def handle_null_supplier(df: DataFrame) -> DataFrame:
    """
    Replaces null values in the SUPPLIER column with 'UNKNOWN'.

    Args:
        df: Input Spark DataFrame.

    Returns:
        DataFrame with null supplier values handled.
    """ 
    df = df.fillna({"SUPPLIER": "UNKNOWN"})

    return df

def handle_null_item_type(df: DataFrame) -> DataFrame:
    """
    Replaces null values in the ITEM_TYPE column with 'UNKNOWN'.

    Args:
        df: Input Spark DataFrame.

    Returns:
        DataFrame with null item type values handled.
    """
    df = df.fillna({"ITEM_TYPE": "UNKNOWN"})

    return df


def create_sale_date(df: DataFrame):
    """
    Creates a SALE_DATE column using the YEAR and MONTH columns.

    The day is set to the first day of each month.

    Args:
        df: Input Spark DataFrame.

    Returns:
        DataFrame containing the new SALE_DATE column.
    """
    df = df.withColumn(
        "SALE_DATE",
        make_date(
            col("YEAR"),
            col("MONTH"),
            lit(1)
        )
    )

    return df

def select_final_columns(df: DataFrame) -> DataFrame:
    """
    Selects the columns required for the final Silver dataset.

    Args:
        df: Input Spark DataFrame.

    Returns:
        DataFrame containing only the selected columns.
    """
    colunas = [
        "SALE_DATE",
        "SUPPLIER",
        "ITEM_CODE",
        "ITEM_DESCRIPTION",
        "ITEM_TYPE",
        "RETAIL_SALES",
        "RETAIL_TRANSFERS",
        "WAREHOUSE_SALES"
    ]

    return df.select(*colunas)