
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from vendas_etl.extract import extract
from pathlib import Path
from vendas_etl.load import load_parquet
from vendas_etl.aggregations import aggregate_monthly_sales, aggregate_supplier_sales, aggregate_category_sales, aggregate_product_sales
from vendas_etl.transform import (
    standardize_columns,
    cast_columns,
    create_sale_date,
    handle_null_supplier,
    handle_null_item_type,
    select_final_columns,
    analyze_nulls
)


def test_standardize_columns():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TestesVendas")
        .getOrCreate()
    )

    dados = [
        (2025, 1, "Fornecedor A", 150.0)
    ]

    colunas = [
        "YEAR",
        "MONTH",
        "ITEM DESCRIPTION",
        "RETAIL SALES"
    ]

    df = spark.createDataFrame(dados, colunas)

    df_tratado = standardize_columns(df)

    colunas_esperadas = [
        "YEAR",
        "MONTH",
        "ITEM_DESCRIPTION",
        "RETAIL_SALES"
    ]

    assert df_tratado.columns == colunas_esperadas

    spark.stop()


def test_cast_columns():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TestesVendas")
        .getOrCreate()
    )

    dados = [
        (2025, 1, 150.0, 200.0, 300.0)
    ]

    colunas = [
        "YEAR",
        "MONTH",
        "RETAIL_SALES",
        "RETAIL_TRANSFERS",
        "WAREHOUSE_SALES"
    ]

    df = spark.createDataFrame(dados, colunas)

    df_tratado = cast_columns(df)

    tipos_esperados = {
        "YEAR": "int",
        "MONTH": "int",
        "RETAIL_SALES": "double",
        "RETAIL_TRANSFERS": "double",
        "WAREHOUSE_SALES": "double"
    }

    tipos_obtidos = {
        campo.name: campo.dataType.simpleString()
        for campo in df_tratado.schema.fields
    }

    for coluna, tipo in tipos_esperados.items():
        assert tipos_obtidos[coluna] == tipo

    spark.stop()


    
def test_create_sale_date():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TestesVendas")
        .getOrCreate()
    )

    dados = [
        (2025, 1),
        (2025, 6),
        (2025, 12)
    ]

    colunas = ["YEAR", "MONTH"]

    df = spark.createDataFrame(dados, colunas)

    df_tratado = create_sale_date(df)

    datas_obtidas = [
        linha["SALE_DATE"]
        for linha in df_tratado.collect()
    ]

    datas_esperadas = [
        "2025-01-01",
        "2025-06-01",
        "2025-12-01"
    ]

    assert [str(data) for data in datas_obtidas] == datas_esperadas

    spark.stop()



def test_handle_null_supplier():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TestesVendas")
        .getOrCreate()
    )

    dados = [
        ("Fornecedor A",),
        (None,),
        ("Fornecedor B",)
    ]

    colunas = ["SUPPLIER"]

    df = spark.createDataFrame(dados, colunas)

    df_tratado = handle_null_supplier(df)

    fornecedores = [
        linha["SUPPLIER"]
        for linha in df_tratado.collect()
    ]

    fornecedores_esperados = [
        "Fornecedor A",
        "UNKNOWN",
        "Fornecedor B"
    ]

    assert fornecedores == fornecedores_esperados

    spark.stop()


    
def test_handle_null_item_type():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TestesVendas")
        .getOrCreate()
    )

    dados = [
        ("WINE",),
        (None,),
        ("BEER",)
    ]

    colunas = ["ITEM_TYPE"]

    df = spark.createDataFrame(dados, colunas)

    df_tratado = handle_null_item_type(df)

    tipos_obtidos = [
        linha["ITEM_TYPE"]
        for linha in df_tratado.collect()
    ]

    tipos_esperados = [
        "WINE",
        "UNKNOWN",
        "BEER"
    ]

    assert tipos_obtidos == tipos_esperados

    spark.stop()



    
def test_select_final_columns():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TestesVendas")
        .getOrCreate()
    )

    dados = [
        (
            "2025-01-01",
            "Fornecedor A",
            "123",
            "Vinho Tinto",
            "WINE",
            150.0,
            200.0,
            300.0,
            "COLUNA EXTRA"
        )
    ]

    colunas = [
        "SALE_DATE",
        "SUPPLIER",
        "ITEM_CODE",
        "ITEM_DESCRIPTION",
        "ITEM_TYPE",
        "RETAIL_SALES",
        "RETAIL_TRANSFERS",
        "WAREHOUSE_SALES",
        "EXTRA_COLUMN"
    ]

    df = spark.createDataFrame(dados, colunas)

    df_tratado = select_final_columns(df)

    colunas_esperadas = [
        "SALE_DATE",
        "SUPPLIER",
        "ITEM_CODE",
        "ITEM_DESCRIPTION",
        "ITEM_TYPE",
        "RETAIL_SALES",
        "RETAIL_TRANSFERS",
        "WAREHOUSE_SALES"
    ]

    assert df_tratado.columns == colunas_esperadas

    spark.stop()



def test_analyze_nulls():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TestesVendas")
        .getOrCreate()
    )

    dados = [
        ("Fornecedor A", 100.0),
        (None, 200.0),
        ("Fornecedor B", None)
    ]

    colunas = ["SUPPLIER", "RETAIL_SALES"]

    df = spark.createDataFrame(dados, colunas)

    resultado = analyze_nulls(df).collect()[0].asDict()

    assert resultado["SUPPLIER"] == 1
    assert resultado["RETAIL_SALES"] == 1

    spark.stop()



def test_extract(tmp_path):
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TesteExtracao")
        .getOrCreate()
    )

    caminho_csv = tmp_path / "vendas_teste.csv"

    caminho_csv.write_text(
        "YEAR,MONTH,SUPPLIER,ITEM CODE,ITEM DESCRIPTION,ITEM TYPE,RETAIL SALES,RETAIL TRANSFERS,WAREHOUSE SALES\n"
        '2025,1,Fornecedor A,123,Vinho Tinto,WINE,150.0,200.0,300.0\n'
        '2025,2,Fornecedor B,456,Vinho Branco,WINE,250.0,100.0,400.0\n',
        encoding="utf-8"
    )

    df = extract(spark, str(caminho_csv))

    assert df.count() == 2

    assert "YEAR" in df.columns
    assert "SUPPLIER" in df.columns
    assert "ITEM CODE" in df.columns

    spark.stop()



def test_load_parquet(tmp_path):
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TesteLoad")
        .getOrCreate()
    )

    dados = [
        (1, "Vinho Tinto", 150.0),
        (2, "Vinho Branco", 200.0)
    ]

    colunas = ["ITEM_CODE", "ITEM_DESCRIPTION", "RETAIL_SALES"]

    df = spark.createDataFrame(dados, colunas)

    caminho_saida = str(tmp_path / "vendas.parquet")

    load_parquet(df, caminho_saida)

    df_carregado = spark.read.parquet(caminho_saida)

    assert df_carregado.count() == 2
    assert df_carregado.columns == colunas

    spark.stop()



def test_aggregate_monthly_sales():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TesteAgregacaoMensal")
        .getOrCreate()
    )

    dados = [
        ("2025-01-01", 100.0, 50.0, 200.0),
        ("2025-01-01", 150.0, 30.0, 100.0),
        ("2025-02-01", 200.0, 70.0, 300.0)
    ]

    colunas = [
        "SALE_DATE",
        "RETAIL_SALES",
        "RETAIL_TRANSFERS",
        "WAREHOUSE_SALES"
    ]

    df = spark.createDataFrame(dados, colunas)

    df = df.withColumn(
        "SALE_DATE",
        col("SALE_DATE").cast("date")
    )

    df_aggregated = aggregate_monthly_sales(df)

    resultado = df_aggregated.collect()

    assert len(resultado) == 2

    janeiro = resultado[0]

    assert janeiro["YEAR"] == 2025
    assert janeiro["MONTH"] == 1
    assert janeiro["TOTAL_RETAIL_SALES"] == 250.0
    assert janeiro["TOTAL_RETAIL_TRANSFERS"] == 80.0
    assert janeiro["TOTAL_WAREHOUSE_SALES"] == 300.0

    fevereiro = resultado[1]

    assert fevereiro["YEAR"] == 2025
    assert fevereiro["MONTH"] == 2
    assert fevereiro["TOTAL_RETAIL_SALES"] == 200.0

    spark.stop()


def test_aggregate_supplier_sales():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TesteAgregacaoFornecedor")
        .getOrCreate()
    )

    dados = [
        ("Fornecedor A", 100.0, 50.0, 200.0),
        ("Fornecedor A", 150.0, 30.0, 100.0),
        ("Fornecedor B", 200.0, 70.0, 300.0),
        ("UNKNOWN", 50.0, 20.0, 80.0)
    ]

    colunas = [
        "SUPPLIER",
        "RETAIL_SALES",
        "RETAIL_TRANSFERS",
        "WAREHOUSE_SALES"
    ]

    df = spark.createDataFrame(dados, colunas)

    df_aggregated = aggregate_supplier_sales(df)

    resultado = df_aggregated.collect()

    assert len(resultado) == 3

    fornecedor_a = resultado[0]

    assert fornecedor_a["SUPPLIER"] == "Fornecedor A"
    assert fornecedor_a["TOTAL_RETAIL_SALES"] == 250.0
    assert fornecedor_a["TOTAL_RETAIL_TRANSFERS"] == 80.0
    assert fornecedor_a["TOTAL_WAREHOUSE_SALES"] == 300.0

    fornecedor_b = resultado[1]

    assert fornecedor_b["SUPPLIER"] == "Fornecedor B"
    assert fornecedor_b["TOTAL_RETAIL_SALES"] == 200.0

    fornecedor_unknown = resultado[2]

    assert fornecedor_unknown["SUPPLIER"] == "UNKNOWN"
    assert fornecedor_unknown["TOTAL_RETAIL_SALES"] == 50.0

    spark.stop()


def test_aggregate_category_sales():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TesteAgregacaoCategoria")
        .getOrCreate()
    )

    dados = [
        ("WINE", 100.0, 50.0, 200.0),
        ("WINE", 150.0, 30.0, 100.0),
        ("BEER", 200.0, 70.0, 300.0),
        ("UNKNOWN", 50.0, 20.0, 80.0)
    ]

    colunas = [
        "ITEM_TYPE",
        "RETAIL_SALES",
        "RETAIL_TRANSFERS",
        "WAREHOUSE_SALES"
    ]

    df = spark.createDataFrame(dados, colunas)

    df_aggregated = aggregate_category_sales(df)

    resultado = df_aggregated.collect()

    assert len(resultado) == 3

    beer = resultado[0]

    assert beer["ITEM_TYPE"] == "BEER"
    assert beer["TOTAL_RETAIL_SALES"] == 200.0

    unknown = resultado[1]

    assert unknown["ITEM_TYPE"] == "UNKNOWN"
    assert unknown["TOTAL_RETAIL_SALES"] == 50.0

    wine = resultado[2]

    assert wine["ITEM_TYPE"] == "WINE"
    assert wine["TOTAL_RETAIL_SALES"] == 250.0
    assert wine["TOTAL_RETAIL_TRANSFERS"] == 80.0
    assert wine["TOTAL_WAREHOUSE_SALES"] == 300.0

    spark.stop()


def test_aggregate_product_sales():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TesteAgregacaoProduto")
        .getOrCreate()
    )

    dados = [
        ("1001", "Vinho Tinto", "WINE", 100.0, 50.0, 200.0),
        ("1001", "Vinho Tinto", "WINE", 150.0, 30.0, 100.0),
        ("1002", "Vinho Branco", "WINE", 200.0, 70.0, 300.0),
        ("2001", "Cerveja", "BEER", 50.0, 20.0, 80.0)
    ]

    colunas = [
        "ITEM_CODE",
        "ITEM_DESCRIPTION",
        "ITEM_TYPE",
        "RETAIL_SALES",
        "RETAIL_TRANSFERS",
        "WAREHOUSE_SALES"
    ]

    df = spark.createDataFrame(dados, colunas)

    df_aggregated = aggregate_product_sales(df)

    resultado = df_aggregated.collect()

    assert len(resultado) == 3

    vinho_tinto = resultado[0]

    assert vinho_tinto["ITEM_CODE"] == "1001"
    assert vinho_tinto["ITEM_DESCRIPTION"] == "Vinho Tinto"
    assert vinho_tinto["ITEM_TYPE"] == "WINE"
    assert vinho_tinto["TOTAL_RETAIL_SALES"] == 250.0
    assert vinho_tinto["TOTAL_RETAIL_TRANSFERS"] == 80.0
    assert vinho_tinto["TOTAL_WAREHOUSE_SALES"] == 300.0

    vinho_branco = resultado[1]

    assert vinho_branco["ITEM_CODE"] == "1002"
    assert vinho_branco["ITEM_DESCRIPTION"] == "Vinho Branco"
    assert vinho_branco["ITEM_TYPE"] == "WINE"
    assert vinho_branco["TOTAL_RETAIL_SALES"] == 200.0

    cerveja = resultado[2]

    assert cerveja["ITEM_CODE"] == "2001"
    assert cerveja["ITEM_DESCRIPTION"] == "Cerveja"
    assert cerveja["TOTAL_RETAIL_SALES"] == 50.0

    spark.stop()