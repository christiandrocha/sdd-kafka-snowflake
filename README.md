# ai-kafka-microbatch

> Pipeline de streaming de dados em tempo real — PostgreSQL → Debezium → Kafka → Snowflake, com transformações via dbt Core (Bronze / Silver / Gold), orquestração via Dagster e observabilidade completa com Prometheus e Grafana.

![Version](https://img.shields.io/badge/version-2.0.0-blue?style=flat-square)
![Python](https://img.shields.io/badge/Python-81.3%25-3776AB?style=flat-square&logo=python&logoColor=white)
![Shell](https://img.shields.io/badge/Shell-16.6%25-4EAA25?style=flat-square&logo=gnu-bash&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-Confluent_7.5-231F20?style=flat-square&logo=apache-kafka&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Enterprise-29B5E8?style=flat-square&logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Core-FF694B?style=flat-square&logo=dbt&logoColor=white)
![Dagster](https://img.shields.io/badge/Dagster-Orchestrator-7C3AED?style=flat-square)

---

## Visão geral

Este projeto implementa um pipeline completo de **Streaming Data Delivery** usando Change Data Capture (CDC) para capturar mudanças em tempo real de um banco PostgreSQL e entregá-las ao Snowflake, onde são transformadas em modelos analíticos via dbt.

```
PostgreSQL (WAL lógico + REPLICA IDENTITY FULL)
    └─► Debezium Source Connector (CDC via pgoutput)
            └─► Apache Kafka — tópico: pocdb.public.<tabela>
                    └─► Snowflake Sink Connector (Snowpipe Streaming)
                                └─► CDC_POC.PUBLIC — RECORD_CONTENT VARIANT
                                        └─► dbt Bronze — views (extração JSON)
                                                └─► dbt Silver — incremental MERGE (CDC UPSERT)
                                                        └─► dbt Gold — tables (modelos para BI)
```

**Exemplo de ponta a ponta:** um `UPDATE orders SET status='shipped' WHERE id=42` no PostgreSQL percorre todo o pipeline e aparece como `gross_revenue = 179.80` na tabela `fact_orders` do Snowflake em menos de 60 segundos.

---

## Stack

| Camada | Tecnologia | Versão | Função |
|---|---|---|---|
| Fonte | PostgreSQL | 15 | Banco operacional com WAL lógico para CDC |
| CDC | Debezium | 2.4 | Lê o WAL via slot de replicação `pgoutput` |
| Broker | Apache Kafka | Confluent 7.5.0 | Armazena eventos com retenção de 7 dias |
| Schema | Confluent Schema Registry | 7.5.0 | Registra schemas Avro (compatibilidade BACKWARD) |
| Integração | Kafka Connect | Debezium + Snowflake Sink 2.1.2 | Ponte PostgreSQL → Kafka → Snowflake |
| Auth Connector | Bouncy Castle (bc-fips + bcpkix-fips) | 1.0.2.4 / 1.0.7 | Suporte a RSA Key Pair PKCS8 na JVM |
| Destino | Snowflake Enterprise | — | Data warehouse com Snowpipe Streaming |
| Transformação | dbt Core | 1.x | Medallion Architecture: Bronze / Silver / Gold |
| Orquestração | Dagster | Latest | Jobs dbt com lineage, sensores e schedules |
| Monitoramento | Prometheus + Grafana | 2.49 / 10.2 | Métricas JMX do Kafka + dashboards provisionados |
| UI Kafka | Kafka UI (Provectus) | Latest | Inspeciona tópicos, offsets e connectors |

---

## Serviços Docker

O projeto sobe **11 serviços** via `docker-compose.yml`. Portas são expostas apenas em desenvolvimento através do `docker-compose.override.yml` (carregado automaticamente pelo Docker Compose).

| Serviço | Imagem | Porta (dev) | Healthcheck |
|---|---|---|---|
| `zookeeper` | confluentinc/cp-zookeeper:7.5.0 | 2181 | `nc -z localhost 2181` |
| `kafka` | confluentinc/cp-kafka:7.5.0 | 9092 | `kafka-broker-api-versions` |
| `schema-registry` | confluentinc/cp-schema-registry:7.5.0 | 8081 | `curl /subjects` |
| `postgres` | postgres:15 | 5432 | `pg_isready` |
| `kafka-connect` | build: `Dockerfile.connect` | 8083 | `curl /connectors` |
| `dagster` | build: `dagster/Dockerfile` | 3000 | `curl /server_info` |
| `dagster-daemon` | build: `dagster/Dockerfile` | — | restart: unless-stopped |
| `kafka-ui` | provectuslabs/kafka-ui:latest | 8080 | — |
| `jmx-exporter` | bitnami/jmx-exporter:latest | 5556 | — |
| `prometheus` | prom/prometheus:v2.49.0 | 9090 | — |
| `grafana` | grafana/grafana:10.2.0 | 3001 | — |

> Todos os serviços dependem de healthchecks em cascata — o Kafka Connect só sobe após Kafka, PostgreSQL e Schema Registry estarem saudáveis.

---

## Estrutura do repositório

```
sdd-kafka-snowflake/
├── connectors/
│   ├── debezium-source.json        # Debezium PostgreSQL Source (CDC → Kafka)
│   └── snowflake-sink.json         # Snowflake Sink (Kafka → Snowflake Streaming)
│
├── dbt/
│   ├── models/
│   │   ├── bronze/                 # Views — extração do VARIANT sem transformação
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_customers.sql
│   │   │   └── stg_products.sql
│   │   ├── silver/                 # Incrementais — dedup por LSN, MERGE CDC, soft-delete
│   │   │   ├── int_orders.sql
│   │   │   ├── int_customers.sql
│   │   │   └── int_products.sql
│   │   └── gold/                   # Tables físicas com métricas calculadas para BI
│   │       ├── fact_orders.sql
│   │       ├── dim_customers.sql
│   │       └── dim_products.sql
│   ├── macros/
│   │   └── parse_cdc_record.sql    # Macro reutilizável para extração de campos CDC
│   ├── tests/
│   │   └── assert_no_negative_price.sql
│   ├── dbt_project.yml             # Config central: materializações por camada + tags
│   ├── profiles.yml                # Conexão Snowflake: targets dev e prod
│   └── sources.yml                 # Tabelas raw + freshness checks (warn: 5min, error: 30min)
│
├── dagster/
│   ├── Dockerfile                  # Imagem customizada do Dagster com dbt integrado
│   └── workspace.yaml              # Definição dos assets e jobs
│
├── observability/
│   ├── jmx/
│   │   └── kafka-jmx-exporter.yml  # Config do JMX Exporter para métricas do Kafka
│   ├── prometheus/
│   │   ├── prometheus.yml          # Scrape configs
│   │   └── alert_rules.yml         # Regras de alerta
│   └── grafana/
│       ├── provisioning/           # Datasources e dashboards auto-provisionados
│       └── dashboards/
│
├── scripts/
│   ├── init.sql                    # Cria tabelas no PostgreSQL + REPLICA IDENTITY FULL
│   ├── snowflake_setup.sql         # Roles, warehouse, database e GRANTs no Snowflake
│   ├── register_connectors.sh      # Registra connectors via REST API do Kafka Connect
│   └── generate_keys.sh            # Gera par RSA (PKCS8) para Key Pair Auth
│
├── tests/                          # Testes de integração do pipeline
│
├── Dockerfile.connect              # debezium/connect:2.4 + Snowflake JAR + Bouncy Castle
├── docker-compose.yml              # Infraestrutura base (dev + prod)
├── docker-compose.override.yml     # Portas expostas e volumes — dev only (auto-loaded)
├── docker-compose.prod.yml         # Referência de topologia para produção
├── .env.example                    # Template de variáveis (seguro para commitar)
└── .gitignore
```

---

## Pré-requisitos

- Docker Desktop 4.x+ com **mínimo 8 GB de RAM** alocados
- Docker Compose v2
- Conta **Snowflake Enterprise Edition** (Snowpipe Streaming requer Enterprise)
- `jq` instalado localmente
- `openssl` instalado localmente

---

## Setup completo

### 1. Clonar o repositório

```bash
git clone https://github.com/christiandrocha/sdd-kafka-snowflake.git
cd sdd-kafka-snowflake
```

### 2. Gerar chaves RSA para autenticação no Snowflake

```bash
bash scripts/generate_keys.sh
```

Gera três arquivos (todos gitignored):

| Arquivo | Uso |
|---|---|
| `snowflake_rsa_key.pem` | Chave privada PKCS1 — base para derivação |
| `snowflake_rsa_key.pub` | Chave pública — vai no `ALTER USER` do Snowflake |
| `snowflake_rsa_key_pkcs8.pem` | Chave privada **PKCS8** — usada pelo connector e pelo dbt |

> ⚠️ O connector exige PKCS8 obrigatoriamente. O Bouncy Castle no `Dockerfile.connect` foi incluído especificamente para suportar esse formato na JVM.

### 3. Configurar o Snowflake

Execute `scripts/snowflake_setup.sql` como `ACCOUNTADMIN` (via Snowsight ou SnowSQL):

```bash
snowsql -a <sua-conta> -u <seu-admin> -f scripts/snowflake_setup.sql
```

O script cria os seguintes objetos:

```sql
ROLE      CDC_ROLE              -- acesso mínimo ao pipeline
USER      KAFKA_CONNECTOR_USER  -- autenticação RSA Key Pair
WAREHOUSE CDC_WH                -- XSMALL, AUTO_SUSPEND=60, AUTO_RESUME=TRUE
DATABASE  CDC_POC
SCHEMA    CDC_POC.PUBLIC        -- raw (Sink Connector)
SCHEMA    CDC_POC.BRONZE        -- criado pelo dbt
SCHEMA    CDC_POC.SILVER        -- criado pelo dbt
SCHEMA    CDC_POC.GOLD          -- criado pelo dbt
-- GRANT ON FUTURE TABLES garante permissões automáticas em tabelas novas
```

Cole o conteúdo de `snowflake_rsa_key.pub` no comando:
```sql
ALTER USER KAFKA_CONNECTOR_USER SET RSA_PUBLIC_KEY = '<conteúdo do .pub>';
```

### 4. Configurar variáveis de ambiente

```bash
cp .env.example .env
```

Edite o `.env` com os valores reais:

```bash
# PostgreSQL
POSTGRES_USER=poc_user
POSTGRES_PASSWORD=poc_pass123
POSTGRES_DB=pocdb
DATABASE_URL=postgresql://poc_user:poc_pass123@localhost:5432/pocdb

# Snowflake
SNOWFLAKE_URL=<conta>.snowflakecomputing.com
SNOWFLAKE_ACCOUNT=<conta>
SNOWFLAKE_USER=KAFKA_CONNECTOR_USER
SNOWFLAKE_PRIVATE_KEY=<conteúdo da chave PKCS8 em uma linha>
SNOWFLAKE_PRIVATE_KEY_PATH=/secrets/snowflake_private_key.pem
SNOWFLAKE_DATABASE=CDC_POC
SNOWFLAKE_WAREHOUSE=CDC_WH
SNOWFLAKE_ROLE=CDC_ROLE

# Schema Registry
SCHEMA_REGISTRY_URL=http://schema-registry:8081

# dbt
DBT_TARGET=dev
```

### 5. Subir a infraestrutura

```bash
docker compose up -d
```

O `docker-compose.override.yml` é carregado **automaticamente** junto com o `docker-compose.yml`, expondo todas as portas para acesso local. O `init.sql` é executado automaticamente na primeira subida do PostgreSQL, criando as tabelas com `REPLICA IDENTITY FULL` e a publicação `dbz_publication`.

Acompanhe a inicialização:

```bash
docker compose ps        # verifica o estado de cada serviço
docker compose logs -f kafka-connect  # aguarda "Kafka Connect started"
```

### 6. Registrar os connectors

```bash
bash scripts/register_connectors.sh
```

O script aguarda o Kafka Connect estar saudável e registra:

- **`postgres-cdc-source`** — Debezium lendo o WAL via slot `debezium_slot`
- **`snowflake-sink`** — entregando eventos ao Snowflake via Snowpipe Streaming

Verifique o status:

```bash
curl -s http://localhost:8083/connectors?expand=status | jq \
  '.[] | {name: .status.name, state: .status.connector.state}'
```

O pipeline está ativo quando ambos mostrarem `"state": "RUNNING"`.

---

## Ambientes: dev vs. produção

O projeto separa configurações de ambiente em três arquivos:

| Arquivo | Quando é usado | O que contém |
|---|---|---|
| `docker-compose.yml` | sempre | Definição base de todos os serviços, sem portas |
| `docker-compose.override.yml` | **automático em dev** | Portas expostas + volumes de hot-reload |
| `docker-compose.prod.yml` | produção (manual) | Referência de topologia: Kafka gerenciado, restart:always, sem portas |

```bash
# Desenvolvimento (padrão — override é automático)
docker compose up -d

# Produção
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

Em produção, o `docker-compose.prod.yml` documenta:
- **Kafka / Schema Registry** → substituir por Confluent Cloud ou AWS MSK
- **Prometheus / Grafana** → substituir por Grafana Cloud ou solução gerenciada
- **`DBT_TARGET: prod`** → aponta para warehouse e threads maiores

---

## Modelos dbt

### Medallion Architecture

| Camada | Materialização | Schema Snowflake | Descrição |
|---|---|---|---|
| **Bronze** | `view` | `CDC_POC.BRONZE` | Extrai campos do `RECORD_CONTENT VARIANT` via notação `:`. Zero storage. |
| **Silver** | `incremental` (merge) | `CDC_POC.SILVER` | Filtra por `pg_lsn`, deduplica com `ROW_NUMBER()`, aplica MERGE CDC. Soft-delete via `is_deleted`. |
| **Gold** | `table` | `CDC_POC.GOLD` | Joins entre Silver, métricas calculadas (`gross_revenue`, `discount_amount`), `cluster_by` para BI. |

### Execução manual via Dagster

Acesse `http://localhost:3000` para disparar jobs, visualizar o lineage e acompanhar histórico de runs. Para execução manual via CLI:

```bash
# Rodar todas as camadas
docker exec dagster-webserver dbt run --project-dir /opt/dagster/dbt

# Rodar por camada (tags definidas no dbt_project.yml)
docker exec dagster-webserver dbt run --select tag:bronze
docker exec dagster-webserver dbt run --select tag:silver
docker exec dagster-webserver dbt run --select tag:gold

# Testes de qualidade
docker exec dagster-webserver dbt test

# Verificar freshness das fontes (warn: 5min, error: 30min)
docker exec dagster-webserver dbt source freshness

# Full refresh — recria a tabela Silver do zero (use com cautela)
docker exec dagster-webserver dbt run --select tag:silver --full-refresh
```

---

## Interfaces locais (dev)

| Interface | URL | Credenciais |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| Kafka Connect REST API | http://localhost:8083 | — |
| Schema Registry | http://localhost:8081 | — |
| Dagster Webserver | http://localhost:3000 | — |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3001 | admin / admin |
| PostgreSQL | localhost:5432 | ver `.env` |

---

## Segurança

- **RSA Key Pair Auth** no Snowflake — sem senhas trafegando em texto claro
- **Bouncy Castle FIPS** (`bc-fips` + `bcpkix-fips`) no `Dockerfile.connect` — leitura de chaves PKCS8 FIPS-compliant na JVM
- **Princípio do menor privilégio** — `CDC_ROLE` acessa somente `CDC_POC.PUBLIC`; `ANALYST_ROLE` acessa somente `GOLD`
- **`x-snowflake-env` anchor** no `docker-compose.yml` — centraliza o `env_file: .env` para todos os serviços que precisam de credenciais
- **Secrets via variáveis de ambiente** — `.env` no `.gitignore`; em produção usar HashiCorp Vault ou AWS Secrets Manager
- **Override file isolado** — portas nunca são expostas no `docker-compose.yml` base; apenas no `docker-compose.override.yml` de dev

> ⚠️ O `.env.example` é seguro para commitar. O `.env` real com credenciais **nunca deve ser commitado**.

---

## Monitoramento

### Kafka (Prometheus + Grafana)
- Throughput de mensagens por tópico (`kafka-ui` em :8080)
- Consumer lag do Snowflake Sink (métrica JMX via `jmx-exporter` em :5556)
- Estado dos connectors via REST: `GET /connectors?expand=status`
- Alertas configurados em `observability/prometheus/alert_rules.yml`
- Dashboards Grafana provisionados automaticamente de `observability/grafana/provisioning/`

### dbt / Dagster
- Lineage completo em `http://localhost:3000`
- `dbt source freshness` — alerta se nenhum dado novo chegar em 5+ minutos no raw

### Snowflake (ACCOUNT_USAGE)
```sql
-- Créditos consumidos por warehouse (últimos 30 dias)
SELECT WAREHOUSE_NAME,
       ROUND(SUM(CREDITS_USED), 3)       AS creditos,
       ROUND(SUM(CREDITS_USED) * 3.0, 2) AS custo_usd_enterprise
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY WAREHOUSE_NAME ORDER BY creditos DESC;
```

---

## Otimização de custos no Snowflake

| Prática | Status |
|---|---|
| `AUTO_SUSPEND = 60` no `CDC_WH` | ✅ Implementado |
| dbt Silver incremental com filtro por `pg_lsn` | ✅ Implementado |
| Bronze como `view` — zero storage | ✅ Implementado |
| `cluster_by=['order_date']` nas tabelas Gold | ✅ Implementado |
| Snowpipe Streaming (0,0037 créditos/GB desde dez/2025) | ✅ Ativo |
| Resource Monitor com teto mensal | ✅ Implementado |
| Time Travel raw: 90d → 1d (Kafka tem replay de 7 dias) | ✅ Implementado |

Para adicionar o Resource Monitor:
```sql
CREATE RESOURCE MONITOR cdc_poc_monitor
  WITH CREDIT_QUOTA = 20 FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 75 PERCENT DO NOTIFY
           ON 90 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE CDC_WH SET RESOURCE_MONITOR = cdc_poc_monitor;
```

---

## Comandos úteis

```bash
# Status de todos os serviços
docker compose ps

# Logs de um serviço específico
docker compose logs -f kafka-connect

# Reiniciar um connector travado
curl -X POST http://localhost:8083/connectors/snowflake-sink/restart

# Pausar o CDC para manutenção no PostgreSQL
curl -X PUT http://localhost:8083/connectors/postgres-cdc-source/pause

# Inserir dado de teste
docker exec -it postgres \
  psql -U poc_user -d pocdb \
  -c "INSERT INTO orders (customer_id, product_id, quantity, status, total_price) VALUES (1, 1, 2, 'pending', 199.90);"

# Parar todos os serviços
docker compose down

# Reset completo (remove volumes)
docker compose down -v
```

---

## Troubleshooting

**Connector em estado FAILED**
```bash
curl -s http://localhost:8083/connectors/postgres-cdc-source/status | jq .
# Causa mais comum: PostgreSQL ainda inicializando ao registrar o connector.
# Aguardar e re-executar: bash scripts/register_connectors.sh
```

**Erro JWT / autenticação Snowflake**
- Confirmar que a chave no `.env` é PKCS8 (gerada com `-topk8` pelo `generate_keys.sh`)
- Confirmar que a chave pública foi aplicada: `ALTER USER KAFKA_CONNECTOR_USER SET RSA_PUBLIC_KEY = '...'`
- O Bouncy Castle no `Dockerfile.connect` é obrigatório — não substituir a imagem base sem incluí-lo

**Dagster não encontra modelos dbt**
- Verificar se os volumes `./dbt:/opt/dagster/dbt` estão montados corretamente no `docker-compose.override.yml`
- Confirmar que `DBT_TARGET` está definido no `.env` (default: `dev`)

**Mensagens Avro não aparecem no Kafka UI**
```bash
curl http://localhost:8081/subjects  # Schema Registry deve retornar lista de subjects
# Se vazio: o Debezium ainda não publicou nenhuma mensagem
```

---

## Licença

MIT — veja [LICENSE](LICENSE) para detalhes.

---

## Autor

**Christian D. Rocha** — [@christiandrocha](https://github.com/christiandrocha)
