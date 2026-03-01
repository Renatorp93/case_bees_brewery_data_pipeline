# BEES Brewery Data Pipeline

## [PT-BR]

Pipeline de dados em arquitetura Medallion:

- `Bronze`: ingestão da API Open Brewery DB para `s3a://datalake/bronze/breweries`
- `Silver`: curadoria e particionamento em parquet
- `Gold`: agregações por localização e tipo de cervejaria
- Orquestração com Airflow, processamento com Spark e armazenamento em MinIO

### Stack

- Apache Airflow `2.8.1` (LocalExecutor)
- Apache Spark `3.5.1`
- MinIO (S3-compatible)
- Postgres (metadados do Airflow)
- Pytest

### Pré-requisitos

- Docker + Docker Compose
- Porta `8080` livre (Airflow UI)
- Portas `9000` e `9001` livres (MinIO)
- Portas `7077`, `8081`, `8082` livres (Spark)

### Execução (clonar e executar)

1. Clonar o repositório
```bash
git clone https://github.com/Renatorp93/case_bees_brewery_data_pipeline.git
cd case_bees_brewery_data_pipeline
```

2. Criar arquivo de ambiente local
```bash
cp .env.example .env
```
No Windows PowerShell:
```powershell
Copy-Item .env.example .env
```

3. Subir o ambiente completo
```bash
docker compose up -d --build
```

4. Acessar interfaces
- Airflow UI: `http://localhost:8080` (usuário/senha definidos no `.env`)
- MinIO Console: `http://localhost:9001` (usuário/senha definidos no `.env`)
- Spark Master UI: `http://localhost:8081`

5. Disparar uma execução manual da DAG
```bash
docker compose exec airflow-scheduler \
  airflow dags trigger breweries_medallion_pipeline
```

6. Listar runs e pegar o `dag_run_id`
```bash
docker compose exec airflow-scheduler \
  airflow dags list-runs -d breweries_medallion_pipeline --no-backfill
```

7. Acompanhar status das tasks
```bash
docker compose exec airflow-scheduler \
  airflow tasks states-for-dag-run breweries_medallion_pipeline <dag_run_id>
```

### Validação via UI (Airflow + MinIO)

1. No Airflow UI, abra a DAG `breweries_medallion_pipeline`.
2. Execute um `Trigger DAG` manual.
3. Em `Grid` ou `Graph`, valide as tasks em `success`:
   - `bronze_ingest`
   - `guard_bronze_manifest`
   - `silver_curate`
   - `gold_aggregate`
4. No MinIO UI, abra o bucket `datalake`.
5. Valide a criação de artefatos em:
   - `bronze/breweries/`
   - `silver/breweries/`
   - `gold/breweries/`
6. Opcional: abrir `bronze/.../_metadata.json` para checar `run_id`, `fetched_rows` e timestamps.

### Testes

```bash
docker compose run --rm tests
```

### Idempotência e rerun

- `bronze_ingest` aceita `write_mode`:
  - `skip` (padrão): se o manifesto do `run_id` existe, finaliza em sucesso
  - `overwrite`: remove a saída anterior e reprocessa
  - `fail`: falha se existir saída/manifesto
- Isso permite rerun seguro sem quebrar por colisão de `run_id`.

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

2. Create local environment file
```bash
cp .env.example .env
```
On Windows PowerShell:
```powershell
Copy-Item .env.example .env
```

3. Start the full stack
```bash
docker compose up -d --build
```

4. Open UIs
- Airflow UI: `http://localhost:8080` (credentials defined in `.env`)
- MinIO Console: `http://localhost:9001` (credentials defined in `.env`)
- Spark Master UI: `http://localhost:8081`

5. Trigger a manual DAG run
```bash
docker compose exec airflow-scheduler \
  airflow dags trigger breweries_medallion_pipeline
```

6. List runs and get `dag_run_id`
```bash
docker compose exec airflow-scheduler \
  airflow dags list-runs -d breweries_medallion_pipeline --no-backfill
```

7. Track task status
```bash
docker compose exec airflow-scheduler \
  airflow tasks states-for-dag-run breweries_medallion_pipeline <dag_run_id>
```

### Validation via UI (Airflow + MinIO)

1. In Airflow UI, open DAG `breweries_medallion_pipeline`.
2. Run a manual `Trigger DAG`.
3. In `Grid` or `Graph`, validate all tasks as `success`:
   - `bronze_ingest`
   - `guard_bronze_manifest`
   - `silver_curate`
   - `gold_aggregate`
4. In MinIO UI, open bucket `datalake`.
5. Validate artifacts created under:
   - `bronze/breweries/`
   - `silver/breweries/`
   - `gold/breweries/`
6. Optional: open `bronze/.../_metadata.json` and check `run_id`, `fetched_rows`, and timestamps.

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
