# Interview Notes (Local)

Este arquivo e para estudo de entrevista e NAO sera versionado automaticamente.

## 1) Objetivo do projeto

Pipeline Medallion (Bronze -> Silver -> Gold) para dados de cervejarias da Open Brewery DB, usando:
- Airflow para orquestracao
- Spark para processamento
- MinIO como data lake S3-compatible
- Postgres como metadata DB do Airflow

## 2) Funcionalidades implementadas

### Orquestracao (Airflow)
- DAG principal: `breweries_medallion_pipeline`.
- Ordem de execucao:
  1. `bronze_ingest`
  2. `guard_bronze_manifest`
  3. `silver_curate`
  4. `dq_silver`
  5. `gold_aggregate`
- Retries com backoff exponencial em `default_args`.
- `max_active_runs=1` para reduzir concorrencia nao controlada.

### Bronze (ingestao real de API)
- Consome `https://api.openbrewerydb.org/v1/breweries`.
- Paginacao por `page` e `per_page`.
- Retry HTTP com tenacity para erros transientes (429/5xx, timeout, conexao).
- Escreve JSONL em `s3a://.../bronze/breweries/run_id=<...>`.
- Gera `_metadata.json` como commit marker.

### Guard (fail-fast)
- `guard_bronze_manifest` valida existencia do `_metadata.json` via boto3.
- Falha cedo se Bronze nao estiver consistente.

### Silver
- Le JSON de Bronze por `run_id`.
- Normaliza colunas e adiciona metadados de ingestao.
- Escreve parquet particionado por `country/state_province/city`.

### Data Quality (Silver)
- Job dedicado `dq_silver` (`data_quality.py`) executado entre Silver e Gold.
- Regras implementadas:
  - colunas obrigatorias presentes
  - minimo de linhas (`dq_min_rows`)
  - `id` nulo (critico)
  - consistencia de `ingestion_run_id` (critico)
  - duplicidade de `id` (warning)
- Persistencia de relatorio por run:
  - `s3a://datalake/monitoring/data_quality/silver/run_id=<run_id>/report.json`
- Falha a DAG quando check critico falha.

### Gold
- Le Silver e calcula agregacao por `country/state_province/city/brewery_type`.
- `countDistinct(id)` para `brewery_count`.
- Persiste em caminho run-scoped no Gold.

### Monitoracao e alertas
- Callbacks de monitoracao:
  - `on_task_failure`, `on_task_retry`, `on_dag_failure`, `on_dag_success`
- Logs estruturados em JSON com prefixo `[monitoring]`.
- Alertas por e-mail SMTP:
  - task failure/retry
  - DAG failure
  - DAG success (opcional via `ALERT_EMAIL_ON_SUCCESS=true`)
- Configuracao por variaveis de ambiente:
  - `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`
  - `SMTP_STARTTLS`, `SMTP_SSL`
  - `ALERT_EMAIL_FROM`, `ALERT_EMAIL_TO`
- Origem tecnica da configuracao:
  - leitura e envio em `src/breweries_pipeline/monitoring/alerts.py`
  - injecao das variaveis no `docker-compose.yml`
  - valores definidos no `.env` (a partir de `.env.example`)

### Idempotencia e rerun
- `write_mode` no Bronze:
  - `skip`, `overwrite`, `fail`
- Rerun seguro sem colisao de `run_id`.

### Empacotamento e import path
- Codigo organizado no pacote `src/breweries_pipeline`.
- Build de wheel no Dockerfile e instalacao no container.

### Testes
- Unitarios:
  - ingestao Bronze
  - guard de manifesto
  - transformacao Silver
  - agregacao Gold
  - callbacks/alertas de monitoracao
  - engine de data quality
  - job `data_quality.py`
- Integracao:
  - fluxo Silver -> DQ -> Gold no Spark local (`@pytest.mark.integration`)
- Testes com fallback de `skip` quando Spark/Airflow nao estao disponiveis no ambiente local.

### Execucao para entrevista (roteiro pratico)
- Passo 1: subir stack
  - `docker compose up -d postgres minio minio-init spark-master spark-worker airflow-init airflow-webserver airflow-scheduler`
- Passo 2: rodar testes
  - `docker compose run --rm tests`
- Passo 3: disparar run produtiva e acompanhar
  - `airflow dags trigger breweries_medallion_pipeline`
  - `airflow dags list-runs -d breweries_medallion_pipeline --no-backfill`
  - `airflow tasks states-for-dag-run breweries_medallion_pipeline <dag_run_id>`
- Passo 4: mostrar evidencias no MinIO
  - bronze manifesto
  - report de DQ
  - output gold

### Comportamento observado em demo real
- Run manual pode ficar `queued` quando existe run anterior `running`.
- Motivo esperado: `max_active_runs=1` no DAG.
- Em caso de instabilidade do scheduler (timeouts), pode haver atraso no agendamento de downstream.
- Mitigacao pragmatica em demo:
  - reiniciar scheduler (`docker compose restart airflow-scheduler`)
  - se task ficar em `restarting`, limpar/reexecutar task da mesma execution_date
  - manter apenas 1 run ativa durante a apresentacao

## 3) Trade-offs e decisoes de engenharia

### S3A (Spark) + boto3 (Airflow guard)
- Pro: separa responsabilidades por runtime.
- Contra: duas formas de acesso ao storage.

### `_metadata.json` como commit marker
- Pro: criterio claro de completude da Bronze.
- Contra: exige disciplina para gravar manifesto por ultimo.

### Data quality como etapa dedicada da DAG
- Pro: fail-fast antes de Gold e evidencia formal de qualidade por run.
- Contra: aumenta tempo total de execucao.

### Duplicidade de ID como warning
- Pro: nao bloqueia pipeline por anomalia potencialmente aceitavel da origem.
- Contra: exige monitoramento operacional para tratar warnings recorrentes.

### Alertas SMTP por callback
- Pro: simples, sem dependencia de ferramenta externa.
- Contra: depende de credenciais SMTP e disponibilidade do servidor de e-mail.

## 4) Riscos conhecidos e melhorias futuras

- Expandir DQ para checks de freshness e drift estatistico.
- Incluir historico de metricas de qualidade em tabela consolidada (time-series).
- Integrar com observability stack dedicada (ex.: Prometheus/Grafana/Datadog).
- Adicionar testes de integracao E2E em container (com DAG real + MinIO + Spark).

## 5) Evidencias praticas para entrevista

- Pipeline com ingestao real da API e camadas Bronze/Silver/Gold.
- Gate de qualidade automatizado antes de Gold.
- Alertas de falha/retry por e-mail via SMTP.
- Relatorio de DQ run-scoped persistido no data lake.
- Suite de testes unitarios + integracao executavel via Docker Compose.
