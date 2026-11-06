# 🍷 Vendas de Vinhos — Pipeline ETL

Pipeline de Engenharia de Dados desenvolvido com **Python, PySpark e Apache Airflow**, responsável pela extração, transformação, limpeza e agregação de dados de vendas de vinhos.

O projeto utiliza uma arquitetura de processamento em camadas **Raw, Silver e Gold**, armazenando os resultados no formato Parquet e preparando conjuntos de dados para análises e construção de dashboards.

## 🎯 Objetivo

Desenvolver um pipeline ETL automatizado capaz de processar dados de vendas, garantir sua padronização e disponibilizar informações estruturadas para análises de negócio.

## 📁 Fonte dos dados

O projeto utiliza o dataset **Warehouse and Retail Sales**, disponibilizado na plataforma Kaggle.

- **Autor:** Lalit
- **Plataforma:** Kaggle
- **Dataset:** [Warehouse and Retail Sales](https://www.kaggle.com/datasets/lalit7881/warehouse-and-retail-sales)

O arquivo utilizado no processamento é `Warehouse_and_Retail_Sales.csv`.

Para reproduzir o projeto, baixe o dataset e disponibilize o arquivo no diretório `data/raw/`.

> Os dados originais não são armazenados diretamente neste repositório.

## 🏗️ Arquitetura

```text
CSV (Raw)
   |
   v
Extract
   |
   v
Transform (PySpark)
   |
   v
Silver (Parquet)
   |
   v
Aggregations
   |
   v
Gold (Parquet)
   |
   v
Power BI / Análise de Dados
```

### Camadas de dados

**Raw**
- Dados originais em formato CSV.
- Preservação da fonte de dados.

**Silver**
- Padronização dos nomes das colunas.
- Conversão dos tipos de dados.
- Tratamento de valores nulos.
- Criação da coluna SALE_DATE.
- Armazenamento dos dados tratados em Parquet.

**Gold**

Quatro conjuntos de dados agregados:

| Dataset | Descrição |
|---|---|
| monthly_sales | Indicadores de vendas por ano e mês |
| supplier_sales | Indicadores de vendas por fornecedor |
| category_sales | Indicadores de vendas por categoria |
| product_sales | Indicadores de vendas por produto |

## 🛠️ Tecnologias

- **Python:** linguagem principal.
- **PySpark:** processamento e transformação distribuída de dados.
- **Apache Airflow:** orquestração do pipeline ETL.
- **Docker:** containerização do ambiente.
- **PostgreSQL:** banco de metadados do Airflow.
- **Parquet:** armazenamento otimizado dos dados.
- **Pytest:** testes automatizados.

## 📂 Estrutura do projeto

```text
vendas-vinhos-etl/
│
├── dags/
│   └── etl_vinhos.py
│
├── data/
│   ├── raw/
│   └── processed/
│       ├── silver/
│       └── gold/
│           ├── monthly_sales/
│           ├── supplier_sales/
│           ├── category_sales/
│           └── product_sales/
│
├── src/
│   └── vendas_etl/
│       ├── __init__.py
│       ├── extract.py
│       ├── transform.py
│       ├── load.py
│       ├── aggregations.py
│       └── pipeline.py
│
├── tests/
│   └── test_vendas_etl.py
│
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
├── .gitignore
└── README.md
```

## ⚙️ Configuração e execução

### Pré-requisitos

- Docker Desktop
- Docker Compose
- WSL2 com Ubuntu (para ambiente Windows)

### 1. Clonar o repositório

```bash
git clone URL_DO_REPOSITORIO
cd vendas-vinhos-etl
```

### 2. Preparar os dados

Disponibilize o arquivo `Warehouse_and_Retail_Sales.csv` no diretório:

```text
data/raw/
```

O arquivo original não é versionado no GitHub.

### 3. Inicializar o Airflow

```bash
docker compose up airflow-init
```

### 4. Iniciar os serviços

```bash
docker compose up -d
```

### 5. Acessar o Airflow

Acesse:

```text
http://localhost:8080
```

Credenciais do ambiente local:

- Usuário: `admin`
- Senha: `admin`

> **Nota:** As credenciais acima são utilizadas apenas no ambiente local de desenvolvimento. Em ambientes de produção, utilize credenciais seguras e configure mecanismos adequados de autenticação.

Localize a DAG `ETL_vinhos` e execute-a manualmente pela interface do Airflow.

## 🧪 Testes automatizados

O projeto utiliza Pytest para validar os módulos de extração, transformação, carregamento e agregação.

Execute:

```bash
docker compose exec airflow-scheduler bash -c "PYTHONPATH=/opt/airflow/src pytest -v /opt/airflow/tests/test_vendas_etl.py"
```

## 📊 Resultados do processamento

Processamento realizado sobre **307.645 registros**.

| Camada | Dataset | Registros |
|---|---|---:|
| Silver | Dados tratados | 307.645 |
| Gold | Vendas mensais | 24 |
| Gold | Vendas por fornecedor | 397 |
| Gold | Vendas por categoria | 9 |
| Gold | Vendas por produto | 35.165 |

Todos os conjuntos de dados são armazenados em formato Parquet.

## 🚀 Próximos passos

- [x] Finalizar documentação do projeto.
- [ ] Publicar o repositório no GitHub.
- [ ] Desenvolver dashboards utilizando Power BI.
- [ ] Documentar os indicadores de negócio.
- [ ] Aprimorar monitoramento e validação dos dados.

---

**Projeto desenvolvido para estudos e construção de portfólio na área de Engenharia de Dados.**