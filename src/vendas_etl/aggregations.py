from pyspark.sql import DataFrame

from pyspark.sql.functions import sum as spark_sum, year, month

def aggregate_monthly_sales(df: DataFrame) -> DataFrame:
    """
    Aggregates retail sales, retail transfers, and warehouse sales by month.

    Args:
        df: Input Silver DataFrame.

    Returns:
        DataFrame containing monthly sales aggregations.
    """
    df_aggregated = (
        df.groupBy(
            year("SALE_DATE").alias("YEAR"),
            month("SALE_DATE").alias("MONTH")
        )
        .agg(
            spark_sum("RETAIL_SALES").alias("TOTAL_RETAIL_SALES"),
            spark_sum("RETAIL_TRANSFERS").alias("TOTAL_RETAIL_TRANSFERS"),
            spark_sum("WAREHOUSE_SALES").alias("TOTAL_WAREHOUSE_SALES")
        )
        .orderBy("YEAR", "MONTH")
    )

    return df_aggregated


def aggregate_supplier_sales(df: DataFrame) -> DataFrame:
    """
    Aggregates sales metrics by supplier.

    Args:
        df: Input Silver DataFrame.

    Returns:
        DataFrame containing sales aggregations for each supplier.
    """
    df_aggregated = (
        df.groupBy("SUPPLIER")
        .agg(
            spark_sum("RETAIL_SALES").alias("TOTAL_RETAIL_SALES"),
            spark_sum("RETAIL_TRANSFERS").alias("TOTAL_RETAIL_TRANSFERS"),
            spark_sum("WAREHOUSE_SALES").alias("TOTAL_WAREHOUSE_SALES")
        )
        .orderBy("SUPPLIER")
    )

    return df_aggregated


def aggregate_category_sales(df: DataFrame) -> DataFrame:
    """
    Aggregates sales metrics by product category.

    Args:
        df: Input Silver DataFrame.

    Returns:
        DataFrame containing sales aggregations for each product category.
    """
    df_aggregated = (
        df.groupBy("ITEM_TYPE")
        .agg(
            spark_sum("RETAIL_SALES").alias("TOTAL_RETAIL_SALES"),
            spark_sum("RETAIL_TRANSFERS").alias("TOTAL_RETAIL_TRANSFERS"),
            spark_sum("WAREHOUSE_SALES").alias("TOTAL_WAREHOUSE_SALES")
        )
        .orderBy("ITEM_TYPE")
    )

    return df_aggregated


def aggregate_product_sales(df: DataFrame) -> DataFrame:
    """
    Aggregates sales metrics by product code, description, and category.

    Args:
        df: Input Silver DataFrame.

    Returns:
        DataFrame containing sales aggregations for each product.
    """
    df_aggregated = (
        df.groupBy(
            "ITEM_CODE",
            "ITEM_DESCRIPTION",
            "ITEM_TYPE"
        )
        .agg(
            spark_sum("RETAIL_SALES").alias("TOTAL_RETAIL_SALES"),
            spark_sum("RETAIL_TRANSFERS").alias("TOTAL_RETAIL_TRANSFERS"),
            spark_sum("WAREHOUSE_SALES").alias("TOTAL_WAREHOUSE_SALES")
        )
        .orderBy("ITEM_CODE")
    )

    return df_aggregated