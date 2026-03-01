# BEES Brewery Data Pipeline

## [PT-BR]

Pipeline de dados em arquitetura Medallion:

- `Bronze`: ingestao da API Open Brewery DB para `s3a://datalake/bronze/breweries`
- `Silver`: curadoria e particionamento em parquet
- `Gold`: agregacoes por localizacao e tipo de cervejaria
- Orquestracao com Airflow, processamento com Spark e armazenamento em MinIO

### Stack

- Apache Airflow `2.8.1` (LocalExecutor)
- Apache Spark `3.5.1`
- MinIO (S3-compatible)
- Postgres (metadados do Airflow)
- Pytest

### Pre-requisitos

- Docker + Docker Compose
- Porta `8080` livre (Airflow UI)
- Portas `9000` e `9001` livres (MinIO)
- Portas `7077`, `8081`, `8082` livres (Spark)

### Execucao (clone e roda)

1. Clonar o repositorio
```bash
git clone https://github.com/Renatorp93/case_bees_brewery_data_pipeline.git
cd case_bees_brewery_data_pipeline
```

2. Subir o ambiente completo
```bash
docker compose up -d --build
```

3. Acessar interfaces
- Airflow UI: `http://localhost:8080` (`airflow` / `airflow`)
- MinIO Console: `http://localhost:9001` (`minio` / `minio123`)
- Spark Master UI: `http://localhost:8081`

4. Disparar uma execucao manual da DAG
```bash
docker compose exec airflow-scheduler \
  airflow dags trigger breweries_medallion_pipeline
```

5. Listar runs e pegar o `dag_run_id`
```bash
docker compose exec airflow-scheduler \
  airflow dags list-runs -d breweries_medallion_pipeline --no-backfill
```

6. Acompanhar status das tasks
```bash
docker compose exec airflow-scheduler \
  airflow tasks states-for-dag-run breweries_medallion_pipeline <dag_run_id>
```

### Testes

```bash
docker compose run --rm tests
```

### Idempotencia e rerun

- `bronze_ingest` aceita `write_mode`:
  - `skip` (padrao): se o manifesto do `run_id` existe, finaliza em sucesso
  - `overwrite`: remove a saida anterior e reprocessa
  - `fail`: falha se existir saida/manifesto
- Isso permite rerun seguro sem quebrar por colisao de `run_id`.

## [EN]

Medallion data pipeline:

- `Bronze`: ingest Open Brewery DB API into `s3a://datalake/bronze/breweries`
- `Silver`: curated and partitioned parquet dataset
- `Gold`: aggregations by location and brewery type
- Orchestrated by Airflow, processed with Spark, stored in MinIO

### Stack

- Apache Airflow `2.8.1` (LocalExecutor)
- Apache Spark `3.5.1`
- MinIO (S3-compatible)
- Postgres (Airflow metadata DB)
- Pytest

### Prerequisites

- Docker + Docker Compose
- Port `8080` available (Airflow UI)
- Ports `9000` and `9001` available (MinIO)
- Ports `7077`, `8081`, `8082` available (Spark)

### Run (clone and execute)

1. Clone the repository
```bash
git clone https://github.com/Renatorp93/case_bees_brewery_data_pipeline.git
cd case_bees_brewery_data_pipeline
```

2. Start the full stack
```bash
docker compose up -d --build
```

3. Open UIs
- Airflow UI: `http://localhost:8080` (`airflow` / `airflow`)
- MinIO Console: `http://localhost:9001` (`minio` / `minio123`)
- Spark Master UI: `http://localhost:8081`

4. Trigger a manual DAG run
```bash
docker compose exec airflow-scheduler \
  airflow dags trigger breweries_medallion_pipeline
```

5. List runs and get `dag_run_id`
```bash
docker compose exec airflow-scheduler \
  airflow dags list-runs -d breweries_medallion_pipeline --no-backfill
```

6. Track task status
```bash
docker compose exec airflow-scheduler \
  airflow tasks states-for-dag-run breweries_medallion_pipeline <dag_run_id>
```

### Tests

```bash
docker compose run --rm tests
```

### Idempotency and rerun

- `bronze_ingest` supports `write_mode`:
  - `skip` (default): if manifest for `run_id` exists, task exits successfully
  - `overwrite`: deletes previous output and reprocesses
  - `fail`: fails if output/manifest already exists
- This enables safe reruns without `run_id` collision issues.
