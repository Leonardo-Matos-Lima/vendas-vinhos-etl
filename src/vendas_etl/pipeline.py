from pyspark.sql import SparkSession

from vendas_etl.extract import extract

from vendas_etl.transform import (
    standardize_columns,
    cast_columns,
    handle_null_supplier,
    handle_null_item_type,
    create_sale_date,
    select_final_columns
)

from vendas_etl.aggregations import (
    aggregate_monthly_sales,
    aggregate_supplier_sales,
    aggregate_category_sales,
    aggregate_product_sales
)

from vendas_etl.load import load_parquet



def run_pipeline():
    """
    Executes the complete wine sales ETL pipeline.

    The pipeline extracts data from the source CSV, applies transformations,
    saves the cleaned dataset to the Silver layer, generates Gold aggregations,
    and saves the resulting datasets in Parquet format.

    Returns:
        None.
    """
    spark = (
        SparkSession.builder
        .appName("VendasVinhosETL")
        .getOrCreate()
    )

    try:
        print("Iniciando pipeline ETL...")
        print("Extraindo dados do CSV...")

        raw_path = "/opt/airflow/data/raw/Warehouse_and_Retail_Sales.csv"

        df = extract(spark, raw_path)

        print(f"Extração concluída: {df.count()} registros.")
        print("Iniciando transformação dos dados...")

        df = standardize_columns(df)

        df = cast_columns(df)

        df = handle_null_supplier(df)

        df = handle_null_item_type(df)

        df = create_sale_date(df)

        df_silver = select_final_columns(df)

        print("Transformação concluída.")
        print("Salvando dados na camada Silver...")

        silver_path = "/opt/airflow/data/processed/silver"

        load_parquet(df_silver, silver_path)

        print("Camada Silver salva com sucesso.")

        print("Gerando agregações Gold...")

        # Vendas mensais
        df_monthly = aggregate_monthly_sales(df_silver)

        load_parquet(
            df_monthly,
            "/opt/airflow/data/processed/gold/monthly_sales"
        )

        # Vendas por fornecedor
        df_supplier = aggregate_supplier_sales(df_silver)

        load_parquet(
            df_supplier,
            "/opt/airflow/data/processed/gold/supplier_sales"
        )

        # Vendas por categoria
        df_category = aggregate_category_sales(df_silver)

        load_parquet(
            df_category,
            "/opt/airflow/data/processed/gold/category_sales"
        )

        # Vendas por produto
        df_product = aggregate_product_sales(df_silver)

        load_parquet(
            df_product,
            "/opt/airflow/data/processed/gold/product_sales"
        )

        print("Todas as agregações Gold foram salvas com sucesso.")
        print("Pipeline ETL executado com sucesso.")

    except Exception as erro:
        print(f"Erro durante a execução do pipeline: {erro}")
        raise

    finally:
        spark.stop()
        print("Sessão Spark encerrada.")


if __name__ == "__main__":
    run_pipeline()