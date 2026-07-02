# Guia de Uso: Execução em Cluster Spark

Os guias anteriores ([Limpeza](./cleaning.md), [Indexação](./elasticsearch_indexing.md), [Linkage](./linkage.md) e [Deduplicação](./deduplicate.md)) cobrem a configuração de cada etapa do pipeline. Este guia cobre o que muda quando essas etapas saem do laboratório local (`make up`, `spark.master: "local[*]"`) e passam a rodar contra um **cluster Spark real** (YARN, Spark Standalone ou Kubernetes).

---

## Pré-requisitos de Infraestrutura

- **Spark 3.4+** instalado e configurado no cluster, com Java 11+ em todos os nós.
- O pacote `cidacsrl` instalado (`poetry install --only main` ou `pip install`) no nó a partir do qual o job será submetido — em `cluster` deploy-mode, também precisa estar disponível para o driver remoto (via imagem Docker, `--py-files` ou ambiente pré-provisionado nos nós).
- **Elasticsearch acessível por todos os executors**, não apenas pelo nó que dispara o job — cada executor faz suas próprias queries multisearch diretamente ao ES.
- **Storage distribuído** (HDFS, S3/S3A, ou NFS montado) para os caminhos de entrada, saída e telemetria — paths locais (`/data/...`, `/tmp/...`) só funcionam em `local[*]`.

---

## Configurando o bloco `spark` no YAML

A `SparkSession` é criada a partir do bloco `spark.spark_configs` do YAML de ambiente — basta trocar `spark.master` de `local[*]` para o endereço do cluster:

```yaml
spark:
  spark_configs:
    spark.master: "yarn"                     # ou "spark://host:7077", "k8s://https://<api-server>"
    spark.executor.memory: "8g"
    spark.executor.instances: "10"
    spark.driver.memory: "4g"
    spark.jars.packages: "org.elasticsearch:elasticsearch-spark-30_2.12:9.1.8"
```

Para `deduplication`, o job depende também do GraphFrames:

```yaml
spark:
  spark_configs:
    spark.master: "yarn"
    spark.executor.memory: "4g"
    spark.driver.memory: "2g"
    spark.jars.packages: "graphframes:graphframes:0.8.3-spark3.5-s_2.12"
    spark.jars.repositories: "https://repos.spark-packages.org/"
```

Qualquer chave prefixada com `spark.` no bloco `spark_configs` é repassada para `SparkSession.builder.config(...)` — `spark_factory.py` (`src/cidacsrl/adapters/outbound/spark/spark_factory.py`) não restringe quais configs são aceitas.

---

## Como Disparar o Job

### 1. Direto via CLI (`client` mode)

```bash
cidacsrl linkage --env-config env.yaml --spec-config spec.yaml
```

Funciona sempre que `spark.master` no YAML já aponta para o cluster e a máquina de onde o comando roda tem acesso de rede e credenciais para submeter jobs (tipicamente um *edge node* com Spark configurado). O driver roda localmente, na máquina que executou o comando.

### 2. Via `spark-submit` (controle de `deploy-mode` e alocação)

Quando for necessário rodar o driver dentro do próprio cluster (`--deploy-mode cluster`) ou controlar alocação de recursos no nível do orquestrador (filas do YARN, namespaces do Kubernetes), envolva a CLI em vez de chamá-la diretamente:

```bash
spark-submit \
  --master yarn --deploy-mode cluster \
  --packages org.elasticsearch:elasticsearch-spark-30_2.12:9.1.8 \
  -m cidacsrl.adapters.inbound.cli \
  linkage --env-config /caminho/no/cluster/env.yaml --spec-config /caminho/no/cluster/spec.yaml
```

> A CLI resolve o empacotamento e a UX de invocação — não substitui o orquestrador de cluster onde o `deploy-mode`/alocação de recursos forem exigidos pela infraestrutura de destino. Veja [ADR 010](../adr/adr-010-cli-unica-instalavel.md) para o histórico dessa decisão.

---

## Storage Compartilhado: o que precisa sair do disco local

Em `local[*]`, tudo roda em um único processo e paths locais funcionam por acidente. Em cluster, cada executor roda em uma máquina diferente — qualquer path referenciado no YAML precisa ser resolvível por todos os nós.

| Campo | Exemplo local (não funciona em cluster) | Exemplo em cluster |
|---|---|---|
| `storage.source_path` | `/data/cleaned/fonte.parquet` | `hdfs:///dados/cleaned/fonte.parquet` ou `s3a://bucket/cleaned/` |
| `storage.output_path` | `/data/output/linkage` | `hdfs:///dados/output/linkage` |
| `execution.audit_log_path` | `/data/audit` | `hdfs:///dados/audit` |

**Atenção — deduplicação:** o diretório de checkpoint do GraphFrames é fixo em `/tmp/cidacsrl_dedup_checkpoint` (`deduplication_bootstrap.py`, em `src/cidacsrl/bootstrap/`), um path local. Em cluster multi-nó isso precisa ser redirecionado para um filesystem distribuído — hoje isso não é configurável via YAML, então avalie montar esse caminho em storage compartilhado (ex: montar `/tmp/cidacsrl_dedup_checkpoint` sobre um volume HDFS/NFS comum a todos os nós) antes de rodar `cidacsrl deduplication` em produção.

---

## Conectividade com Elasticsearch

Se o cluster Spark e o Elasticsearch estiverem em redes distintas (containers, VPCs com NAT, hostnames internos não resolvíveis pelos executors), habilite `wan_only` no bloco `elasticsearch` do `env.yaml`:

```yaml
elasticsearch:
  es_connection_url: "http://elasticsearch.exemplo.com:9200"
  wan_only: true
```

Se o Elasticsearch exigir certificado (CA interna/self-signed ou mTLS), veja a seção **[TLS e Certificados](./elasticsearch_indexing.md#tls-e-certificados)** do guia de indexação — que detalha `ca_certs`/`client_cert`/`client_key` (cliente de consulta) e o truststore Java necessário para a escrita em massa via Spark.

Isso evita que o conector tente descobrir e se conectar diretamente aos nós internos do ES, usando somente a URL pública informada.

---

## Checklist antes de rodar em produção

1. `spark.master` aponta para o cluster (não `local[*]`).
2. `storage.*` e `execution.audit_log_path` apontam para storage distribuído.
3. Todos os executors têm rota de rede até o Elasticsearch (`wan_only: true` se necessário).
4. Para `deduplication`, o path de checkpoint do GraphFrames está em storage compartilhado.
5. `spark.jars.packages`/`spark.jars.repositories` corretos para a etapa (conector ES para `indexing`/`linkage`; GraphFrames para `deduplication`).
6. Decidido se o job roda via CLI direta (`client` mode) ou via `spark-submit --deploy-mode cluster`.
