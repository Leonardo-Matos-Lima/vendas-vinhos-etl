# 📊 Indicadores de Negócio — Vendas de Vinhos

## 1. Total Retail Sales

- **Descrição:** representa a soma das vendas realizadas no varejo durante o período analisado.
- **Cálculo:** soma dos valores da coluna `TOTAL_RETAIL_SALES`.
- **Origem:** tabela Gold `monthly_sales`.
- **Granularidade:** mensal.
- **Visualização:** cartão KPI e gráfico de evolução mensal na página Visão Geral.
- **Interpretação:** permite acompanhar o volume de vendas no varejo e sua evolução ao longo do tempo.


## 2. Total Retail Transfers

- **Descrição:** representa a soma das transferências de mercadorias destinadas ao varejo.
- **Cálculo:** soma dos valores da coluna `TOTAL_RETAIL_TRANSFERS`.
- **Origem:** tabela Gold `monthly_sales`.
- **Granularidade:** mensal.
- **Visualização:** cartão KPI na página Visão Geral.
- **Interpretação:** permite acompanhar o volume de produtos transferidos para o varejo, oferecendo uma perspectiva complementar às vendas realizadas.


## 3. Total Warehouse Sales

- **Descrição:** representa a soma das vendas registradas no armazém.
- **Cálculo:** soma dos valores da coluna `TOTAL_WAREHOUSE_SALES`.
- **Origem:** tabela Gold `monthly_sales`.
- **Granularidade:** mensal.
- **Visualização:** cartão KPI na página Visão Geral.
- **Interpretação:** permite acompanhar o volume de vendas associado às operações do armazém, complementando a análise das vendas no varejo.


## 4. Quantidade de Produtos

- **Descrição:** representa a quantidade de produtos distintos presentes na base de vendas.
- **Cálculo:** contagem distinta (`DISTINCTCOUNT`) dos códigos da coluna `ITEM_CODE`.
- **Origem:** tabela Gold `product_sales`.
- **Granularidade:** por produto, identificado pelo código `ITEM_CODE`.
- **Visualização:** cartão KPI na página Visão Geral.
- **Interpretação:** permite conhecer a diversidade de produtos registrados no conjunto de dados, sem contar repetidamente um mesmo produto.


## 5. Retail Sales por Categoria

- **Descrição:** representa o volume total de vendas no varejo agrupado por categoria de produto.
- **Cálculo:** soma dos valores da coluna `TOTAL_RETAIL_SALES` para cada categoria.
- **Origem:** tabela Gold `category_sales`.
- **Granularidade:** por categoria (`ITEM_TYPE`).
- **Visualização:** gráfico de barras na página Visão Geral.
- **Interpretação:** permite comparar o desempenho das diferentes categorias de produtos e identificar quais concentram os maiores volumes de vendas.


## 6. Retail Sales por Fornecedor

- **Descrição:** representa o volume total de vendas no varejo associado a cada fornecedor.
- **Cálculo:** soma dos valores da coluna `TOTAL_RETAIL_SALES` para cada fornecedor.
- **Origem:** tabela Gold `supplier_sales`.
- **Granularidade:** por fornecedor (`SUPPLIER`).
- **Visualização:** gráfico de barras horizontais, limitado aos 10 principais fornecedores, na página Análise Comercial.
- **Interpretação:** permite identificar os fornecedores com maior volume de vendas registrado e comparar os valores de vendas associados a cada um.


## 7. Retail Sales por Produto

- **Descrição:** representa o volume total de vendas no varejo associado a cada produto.
- **Cálculo:** soma dos valores da coluna `TOTAL_RETAIL_SALES` para cada produto.
- **Origem:** tabela Gold `product_sales`.
- **Granularidade:** por produto, identificado por `ITEM_CODE`, `ITEM_DESCRIPTION` e `ITEM_TYPE`.
- **Visualização:** gráfico de barras horizontais, limitado aos 10 produtos com maior volume de vendas, na página Análise Comercial.
- **Interpretação:** permite identificar os produtos com maior volume de vendas e destacar os itens de maior relevância comercial no conjunto de dados.


## Observações sobre os Dados

- **Valores negativos:** foram preservados durante o processamento, pois podem representar ajustes ou movimentações específicas. Sua origem não foi confirmada.
- **Valores nulos:** três registros de `RETAIL_SALES` permaneceram sem preenchimento para evitar a criação de valores artificiais.
- **Categorias e fornecedores desconhecidos:** registros sem identificação foram classificados como `UNKNOWN`, preservando sua presença nas análises.
- **Unidade de medida:** a unidade dos valores das métricas não foi confirmada. Portanto, não devem ser interpretados automaticamente como valores monetários.