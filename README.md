# BEES Brewery Data Pipeline

Pipeline de dados em arquitetura Medallion:

- `Bronze`: ingestão da API Open Brewery DB para `s3a://datalake/bronze/breweries`
- `Silver`: curadoria e particionamento parquet
- `Gold`: agregações por localização e tipo de cervejaria
- Orquestração com Airflow + processamento com Spark + armazenamento em MinIO

## Stack

- Apache Airflow `2.8.1` (LocalExecutor)
- Apache Spark `3.5.1`
- MinIO (S3-compatible)
- Postgres (metadados do Airflow)
- Pytest para testes unitários

## Pré-requisitos

- Docker + Docker Compose
- Porta `8080` livre (Airflow)
- Portas `9000` e `9001` livres (MinIO)
- Portas `7077`, `8081`, `8082` livres (Spark)

## Execução (reproduzível)

1. Clone o repositório
```bash
git clone https://github.com/Renatorp93/case_bees_brewery_data_pipeline.git
cd case_bees_brewery_data_pipeline
```

2. Suba o ambiente completo
```bash
docker compose up -d --build
```

3. Acesse interfaces
- Airflow UI: `http://localhost:8080` (`airflow` / `airflow`)
- MinIO Console: `http://localhost:9001` (`minio` / `minio123`)
- Spark Master UI: `http://localhost:8081`

4. Dispare uma execução manual da DAG (para validação imediata)
```bash
docker compose exec airflow-scheduler \
  airflow dags trigger breweries_medallion_pipeline
```

5. Acompanhe status
```bash
docker compose exec airflow-scheduler \
  airflow tasks states-for-dag-run breweries_medallion_pipeline <dag_run_id>
```

## Testes

```bash
docker compose run --rm tests
```

## Idempotência e rerun

- `bronze_ingest` suporta `write_mode`:
  - `skip` (padrão): se o manifesto já existe para o `run_id`, encerra com sucesso
  - `overwrite`: reprocessa removendo saída anterior
  - `fail`: falha se já existir saída/manifesto
- Isso permite rerun seguro da task sem quebrar por colisão de `run_id`.

## Estrutura

```text
dags/
  breweries_medallion_dag.py
src/breweries_pipeline/
  jobs/
    bronze_ingest.py
    silver_curate.py
    gold_aggregate.py
  guard/
    s3_guard.py
  lib/
    spark.py
tests/
```
