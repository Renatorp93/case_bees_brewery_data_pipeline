# BEES Brewery Data Pipeline

## [PT-BR]

Pipeline de dados em arquitetura Medallion:

- `Bronze`: ingestao da API Open Brewery DB para `s3a://datalake/bronze/breweries`
- `Silver`: curadoria e particionamento em parquet
- `Data Quality`: validacoes criticas na Silver com relatorio por `run_id`
- `Gold`: agregacoes por localizacao e tipo de cervejaria
- Orquestrado por Airflow, processado com Spark, armazenado no MinIO, com alertas SMTP

### Stack

- Apache Airflow `2.8.1` (LocalExecutor)
- Apache Spark `3.5.1`
- MinIO (S3-compatible)
- Postgres (metadados do Airflow)
- Alertas de e-mail via SMTP
- Pytest (unitarios + integracao)

### Pre-requisitos

- Docker + Docker Compose
- Porta `8080` livre (Airflow UI)
- Portas `9000` e `9001` livres (MinIO)
- Portas `7077`, `8081`, `8082` livres (Spark)

### Execucao (clonar e executar)

1. Clonar o repositorio
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

4. Configurar alertas por e-mail (opcional) no `.env`
```dotenv
SMTP_HOST=smtp.seu-provedor.com
SMTP_PORT=587
SMTP_USER=usuario
SMTP_PASSWORD=senha
SMTP_STARTTLS=true
SMTP_SSL=false
ALERT_EMAIL_FROM=pipeline@empresa.com
ALERT_EMAIL_TO=oncall@empresa.com,data-team@empresa.com
ALERT_EMAIL_ON_SUCCESS=false
```
Importante:
- O destino dos alertas fica em `ALERT_EMAIL_TO` (lista separada por virgula).
- O remetente fica em `ALERT_EMAIL_FROM`.
- O envio e feito pelos callbacks em `src/breweries_pipeline/monitoring/alerts.py`.
- Essas variaveis sao injetadas nos containers Airflow via `docker-compose.yml`.

5. Acessar interfaces
- Airflow UI: `http://localhost:8080` (usuario/senha definidos no `.env`)
- MinIO Console: `http://localhost:9001` (usuario/senha definidos no `.env`)
- Spark Master UI: `http://localhost:8081`

6. Disparar execucao manual da DAG
```bash
docker compose exec airflow-scheduler \
  airflow dags trigger breweries_medallion_pipeline
```

7. Listar runs e pegar o `dag_run_id`
```bash
docker compose exec airflow-scheduler \
  airflow dags list-runs -d breweries_medallion_pipeline --no-backfill
```

8. Acompanhar status das tasks
```bash
docker compose exec airflow-scheduler \
  airflow tasks states-for-dag-run breweries_medallion_pipeline <dag_run_id>
```

### Execucao para entrevista (recomendado)

Para uma demonstracao previsivel, execute nesta ordem:

1. Suba stack limpa:
```bash
docker compose up -d postgres minio minio-init spark-master spark-worker airflow-init airflow-webserver airflow-scheduler
```
2. Rode testes (unitarios + integracao):
```bash
docker compose run --rm tests
```
3. Dispare run e monitore:
```bash
docker compose exec airflow-scheduler airflow dags trigger breweries_medallion_pipeline
docker compose exec airflow-scheduler airflow dags list-runs -d breweries_medallion_pipeline --no-backfill
docker compose exec airflow-scheduler airflow tasks states-for-dag-run breweries_medallion_pipeline <dag_run_id>
```

Observacao operacional:
- A DAG usa `max_active_runs=1`. Se uma run manual ficar `queued`, normalmente existe outra run `running`.
- Nessa situacao, aguarde a ativa terminar ou limpe tasks pendentes da run anterior antes de tentar outra demonstracao.

### Validacao via UI (Airflow + MinIO)

1. No Airflow UI, abra a DAG `breweries_medallion_pipeline`.
2. Execute `Trigger DAG`.
3. Em `Grid` ou `Graph`, valide tasks em `success`:
   - `bronze_ingest`
   - `guard_bronze_manifest`
   - `silver_curate`
   - `dq_silver`
   - `gold_aggregate`
4. No MinIO UI, abra bucket `datalake`.
5. Valide artefatos em:
   - `bronze/breweries/`
   - `silver/breweries/`
   - `monitoring/data_quality/silver/`
   - `gold/breweries/`
6. Opcional:
   - abrir `bronze/.../_metadata.json`
   - abrir `monitoring/data_quality/silver/run_id=<run_id>/report.json`

### Monitoracao, alertas e data quality

- Monitoracao por callbacks do Airflow:
  - `on_task_failure`
  - `on_task_retry`
  - `on_dag_failure`
  - `on_dag_success`
- Logs estruturados para monitoracao no scheduler: prefixo `[monitoring]` com payload JSON
- Alertas SMTP:
  - falha e retry de task
  - falha de DAG
  - sucesso de DAG (opcional com `ALERT_EMAIL_ON_SUCCESS=true`)
- Onde o e-mail e definido:
  - `ALERT_EMAIL_TO` e `ALERT_EMAIL_FROM` no `.env`
  - propagado no `docker-compose.yml`
  - lido em `src/breweries_pipeline/monitoring/alerts.py`
- Data quality da Silver (`dq_silver`):
  - colunas obrigatorias
  - minimo de linhas (`dq_min_rows`, parametro da DAG)
  - `id` nulo (critico)
  - consistencia de `ingestion_run_id` (critico)
  - duplicidade de `id` (warning)

### Testes

Todos os testes:
```bash
docker compose run --rm tests
```

Somente integracao:
```bash
docker compose run --rm tests bash -lc "python -m pytest -q -m integration"
```

### Idempotencia e rerun

- `bronze_ingest` aceita `write_mode`:
  - `skip` (padrao): se manifesto do `run_id` existe, finaliza em sucesso
  - `overwrite`: remove saida anterior e reprocessa
  - `fail`: falha se existir saida/manifesto
- Permite rerun seguro sem colisao de `run_id`.

### Troubleshooting rapido (Airflow scheduler)

- Sintoma: run manual fica `queued`.
  - Causa comum: outra run ainda `running` (`max_active_runs=1`).
  - Acao: verificar `airflow dags list-runs -d breweries_medallion_pipeline --no-backfill`.
- Sintoma: downstream nao agenda mesmo com upstream `success`.
  - Causa comum: instabilidade do scheduler.
  - Acao: `docker compose restart airflow-scheduler`.
- Sintoma: task em `restarting` por muito tempo.
  - Acao: limpar a task para a mesma data de execucao e reprocessar.

## [EN]

Medallion data pipeline:

- `Bronze`: ingest Open Brewery DB API into `s3a://datalake/bronze/breweries`
- `Silver`: curated and partitioned parquet dataset
- `Data Quality`: critical Silver checks with run-scoped report
- `Gold`: aggregations by location and brewery type
- Orchestrated by Airflow, processed with Spark, stored in MinIO, with SMTP alerts

### Stack

- Apache Airflow `2.8.1` (LocalExecutor)
- Apache Spark `3.5.1`
- MinIO (S3-compatible)
- Postgres (Airflow metadata DB)
- SMTP e-mail alerts
- Pytest (unit + integration)

### Prerequisites

- Docker + Docker Compose
- Port `8080` available (Airflow UI)
- Ports `9000` and `9001` available (MinIO)
- Ports `7077`, `8081`, `8082` available (Spark)

### Run (clone and execute)

1. Clone repository
```bash
git clone https://github.com/Renatorp93/case_bees_brewery_data_pipeline.git
cd case_bees_brewery_data_pipeline
```

2. Create local env file
```bash
cp .env.example .env
```
On Windows PowerShell:
```powershell
Copy-Item .env.example .env
```

3. Start full stack
```bash
docker compose up -d --build
```

4. Optional e-mail alert config in `.env`
```dotenv
SMTP_HOST=smtp.your-provider.com
SMTP_PORT=587
SMTP_USER=user
SMTP_PASSWORD=password
SMTP_STARTTLS=true
SMTP_SSL=false
ALERT_EMAIL_FROM=pipeline@company.com
ALERT_EMAIL_TO=oncall@company.com,data-team@company.com
ALERT_EMAIL_ON_SUCCESS=false
```
Important:
- Alert recipients are defined in `ALERT_EMAIL_TO` (comma-separated list).
- Sender is defined in `ALERT_EMAIL_FROM`.
- Callback implementation is in `src/breweries_pipeline/monitoring/alerts.py`.
- Variables are passed to Airflow containers through `docker-compose.yml`.

5. Open UIs
- Airflow UI: `http://localhost:8080`
- MinIO Console: `http://localhost:9001`
- Spark Master UI: `http://localhost:8081`

6. Trigger DAG manually
```bash
docker compose exec airflow-scheduler \
  airflow dags trigger breweries_medallion_pipeline
```

7. List runs and get `dag_run_id`
```bash
docker compose exec airflow-scheduler \
  airflow dags list-runs -d breweries_medallion_pipeline --no-backfill
```

8. Track task status
```bash
docker compose exec airflow-scheduler \
  airflow tasks states-for-dag-run breweries_medallion_pipeline <dag_run_id>
```

### Interview Demo Flow (recommended)

For a predictable demo:

1. Start clean stack:
```bash
docker compose up -d postgres minio minio-init spark-master spark-worker airflow-init airflow-webserver airflow-scheduler
```
2. Run full test suite:
```bash
docker compose run --rm tests
```
3. Trigger and monitor:
```bash
docker compose exec airflow-scheduler airflow dags trigger breweries_medallion_pipeline
docker compose exec airflow-scheduler airflow dags list-runs -d breweries_medallion_pipeline --no-backfill
docker compose exec airflow-scheduler airflow tasks states-for-dag-run breweries_medallion_pipeline <dag_run_id>
```

Operational note:
- DAG has `max_active_runs=1`. A manual run can stay `queued` while another run is `running`.

### Validation via UI (Airflow + MinIO)

1. Open DAG `breweries_medallion_pipeline`.
2. Trigger the DAG.
3. Validate tasks in `success`:
   - `bronze_ingest`
   - `guard_bronze_manifest`
   - `silver_curate`
   - `dq_silver`
   - `gold_aggregate`
4. Open bucket `datalake` in MinIO.
5. Validate artifacts under:
   - `bronze/breweries/`
   - `silver/breweries/`
   - `monitoring/data_quality/silver/`
   - `gold/breweries/`
6. Optional:
   - inspect `bronze/.../_metadata.json`
   - inspect `monitoring/data_quality/silver/run_id=<run_id>/report.json`

### Monitoring, alerts, and data quality

- Airflow callbacks:
  - `on_task_failure`
  - `on_task_retry`
  - `on_dag_failure`
  - `on_dag_success`
- Structured monitoring logs in scheduler (`[monitoring]` with JSON payload)
- SMTP alerts:
  - task failure/retry
  - DAG failure
  - DAG success optional (`ALERT_EMAIL_ON_SUCCESS=true`)
- Where e-mail settings live:
  - `.env`: `ALERT_EMAIL_TO` and `ALERT_EMAIL_FROM`
  - `docker-compose.yml`: variable injection into Airflow services
  - `src/breweries_pipeline/monitoring/alerts.py`: SMTP callback logic
- Silver data quality (`dq_silver`):
  - required columns
  - minimum rows (`dq_min_rows`)
  - null `id` (critical)
  - `ingestion_run_id` consistency (critical)
  - duplicate `id` (warning)

### Tests

Run all tests:
```bash
docker compose run --rm tests
```

Run integration tests only:
```bash
docker compose run --rm tests bash -lc "python -m pytest -q -m integration"
```

### Idempotency and rerun

- `bronze_ingest` supports `write_mode`:
  - `skip` (default): success if manifest already exists
  - `overwrite`: delete previous output and reprocess
  - `fail`: fail when output/manifest already exists
- Enables safe reruns without `run_id` collisions.

### Quick Troubleshooting (Airflow scheduler)

- Symptom: manual run stays `queued`.
  - Common cause: another run already `running` (`max_active_runs=1`).
- Symptom: downstream tasks do not start after upstream success.
  - Common cause: scheduler instability/timeouts.
  - Action: `docker compose restart airflow-scheduler`.
