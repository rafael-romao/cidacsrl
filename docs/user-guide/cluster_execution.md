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

> A CLI resolve o empacotamento e a UX de invocação — não substitui o orquestrador de cluster onde o `deploy-mode`/alocação de recursos forem exigidos pela infraestrutura de destino. Veja [ADR 009](../adr/adr-009-cli-unica-instalavel.md) para o histórico dessa decisão.

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

## Execução Offline / Air-gapped (Servidores sem internet)

Em um servidor sem acesso à rede externa (nós de cálculo isolados), a configuração acima **não funciona como está**. O motivo é um só:

> `spark.jars.packages` dispara resolução **Ivy contra o Maven Central em runtime**. Sem internet no nó de execução, o job falha ao tentar baixar o jar.

Num ambiente air-gapped os jars precisam estar **fisicamente presentes** antes do job rodar. Há duas estratégias suportadas — todo o trabalho de download é feito **uma vez, numa máquina com internet**, e o artefato resultante é transferido para o servidor.

### Estratégia A (recomendada): imagem Singularity com os jars embutidos

O repositório traz um Dockerfile dedicado a esse cenário em [`deploy/hpc/Dockerfile`](https://github.com/rafael-romao/cidacsrl/blob/main/deploy/hpc/Dockerfile). Diferente do Dockerfile de laboratório (`tests/environment/Dockerfile`), ele **não resolve nada em runtime**: os jars são baixados no build e copiados para `/opt/spark/jars`, de onde o Spark os carrega automaticamente — sem `spark.jars.packages`.

**Na máquina com internet — build e conversão para `.sif`:**

```bash
# 1. Build (a partir da raiz do repositório). O alvo "full" embute o jar do
#    Elasticsearch e o do GraphFrames num único artefato que serve para
#    linkage, indexing e deduplication.
docker build -f deploy/hpc/Dockerfile --target full -t cidacsrl:offline .
#    (para imagens mínimas por etapa: --target linkage ou --target deduplication)

# 2. Converter Docker -> Singularity (.sif)
singularity build cidacsrl.sif docker-daemon://cidacsrl:offline
#    Sem acesso ao docker-daemon, exporte e converta via tar:
#    docker save cidacsrl:offline -o cidacsrl.tar
#    singularity build cidacsrl.sif docker-archive://cidacsrl.tar

# 3. Transferir cidacsrl.sif (e os YAMLs) para o servidor (scp/rsync).
```

**No servidor — como os jars já estão em `/opt/spark/jars`, remova `spark.jars.packages` do `env.yaml`.** Nenhuma coordenada Maven precisa ser resolvida.

Script Slurm de exemplo (`local[*]` em um nó com muitos cores):

```bash
#!/bin/bash
#SBATCH --job-name=cidacsrl-linkage
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=04:00:00

module load singularity   # ajuste conforme o módulo do seu site

singularity exec \
  --bind /scratch/dados:/data \
  --bind "$PWD/configs":/configs \
  cidacsrl.sif \
  cidacsrl linkage \
    --env-config /configs/env.yaml \
    --spec-config /configs/spec.yaml
```

O `.sif` também serve de imagem para os executors caso você rode Spark distribuído sobre o cluster; mas, em cenários `spark.master: "local[*]"` num fat node costuma ser o caminho mais simples e não exige rede alguma além do acesso ao Elasticsearch.

### Estratégia B: jars em filesystem compartilhado + `spark.jars`

Se preferir usar o Spark já instalado nos nós (sem container), baixe os jars por **caminho local** e referencie-os com `spark.jars` (arquivos locais) em vez de `spark.jars.packages` (coordenada Maven).

**Na máquina com internet — baixe os jars e os wheels Python:**

```bash
# Jars da JVM
mkdir -p offline/jars
curl -fL https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/9.1.8/elasticsearch-spark-30_2.12-9.1.8.jar \
     -o offline/jars/elasticsearch-spark-30_2.12-9.1.8.jar
curl -fL https://repos.spark-packages.org/graphframes/graphframes/0.8.3-spark3.5-s_2.12/graphframes-0.8.3-spark3.5-s_2.12.jar \
     -o offline/jars/graphframes-0.8.3-spark3.5-s_2.12.jar

# Pacote cidacsrl + todas as dependências Python como wheels
pip download . -d offline/wheels/

# Copie a pasta offline/ para o storage compartilhado do servidor (ex: /shared/cidacsrl)
```

**No servidor — instale o pacote Python sem internet e aponte `spark.jars` para os arquivos locais:**

```bash
pip install --no-index --find-links /shared/cidacsrl/wheels/ cidacsrl
```

```yaml
spark:
  spark_configs:
    spark.master: "local[*]"
    # Vários jars: separados por vírgula. NÃO use spark.jars.packages aqui.
    spark.jars: "/shared/cidacsrl/jars/elasticsearch-spark-30_2.12-9.1.8.jar"
```

> ⚠️ **Transitividade:** `spark.jars` **não resolve dependências transitivas** (só `spark.jars.packages` faz isso). O conector do Elasticsearch é um jar autocontido, então basta ele; mas, se algum jar exigir bibliotecas adicionais, você precisa baixá-las também e listá-las todas em `spark.jars`. Alternativa: pré-popular o cache Ivy (`~/.ivy2`) numa máquina com internet e copiá-lo para o `$HOME` no servidor — aí `spark.jars.packages` resolve offline a partir do cache.

### Certificados do Elasticsearch em ambiente air-gapped

Se o cluster ES exigir TLS com CA interna/self-signed ou mTLS, os mecanismos de certificado são os mesmos descritos em **[TLS e Certificados](./elasticsearch_indexing.md#tls-e-certificados)** (arquivos PEM para o cliente de consulta; truststore/keystore Java `.jks`/`.p12` para a escrita em massa via Spark). O que muda no air-gapped é que **nada pode ser buscado em runtime** — a CA não pode ser distribuída via `update-ca-certificates` puxando de um mirror interno no momento do job, nem o truststore pode ser montado a partir de um path que só existe na rede corporativa. Os certificados precisam **viajar junto com o artefato**. Duas formas:

- **Bind-mount no Singularity (recomendado — desacopla certificado da imagem):** copie os arquivos para o HPC e monte-os no container, apontando o YAML para os caminhos internos:

  ```bash
  singularity exec \
    --bind /scratch/dados:/data \
    --bind "$PWD/configs":/configs \
    --bind "$PWD/certs":/certs:ro \
    cidacsrl.sif \
    cidacsrl indexing --env-config /configs/env.yaml --spec-config /configs/spec.yaml
  ```

  ```yaml
  elasticsearch:
    es_connection_url: "https://es.interno.exemplo.com:9200"
    verify_certs: true
    ca_certs: "/certs/ca.pem"                                # cliente de consulta (PEM)
    es.net.ssl.truststore.location: "/certs/truststore.jks"  # escrita em massa via Spark
    es.net.ssl.truststore.pass: "senha-do-truststore"
  ```

- **Embutir a CA na imagem (build na máquina com internet):** importe a CA no trust store do SO e no `cacerts` da JVM dentro do `deploy/hpc/Dockerfile`, de modo que `verify_certs: true` funcione sem `ca_certs`/`truststore` no YAML. Menos flexível (recompilar a imagem a cada rotação de certificado), mas evita gerenciar arquivos soltos no servidor.

No caso de `spark.master: "local[*]"` (driver e executors no mesmo processo/container), a exigência de "replicar a CA em todos os nós" descrita no guia de indexação **colapsa para um único container** — basta o certificado estar acessível ali. Isso só volta a importar se você rodar Spark realmente distribuído sobre o cluster, quando cada executor precisa enxergar o mesmo truststore.

### Versões dos jars precisam bater exatamente

Offline não há como "cair" para outra versão: os jars presentes precisam ser compatíveis com o Spark da imagem/instalação e com o servidor Elasticsearch. As versões canônicas do projeto são:

| Componente | Versão | Compatível com |
|---|---|---|
| Conector Elasticsearch | `org.elasticsearch:elasticsearch-spark-30_2.12:9.1.8` | ES server 9.1.x, Spark 3.x, Scala 2.12 |
| GraphFrames | `graphframes:graphframes:0.8.3-spark3.5-s_2.12` | Spark 3.5, Scala 2.12 |

---

## Checklist antes de rodar em produção

1. `spark.master` aponta para o cluster (não `local[*]`).
2. `storage.*` e `execution.audit_log_path` apontam para storage distribuído.
3. Todos os executors têm rota de rede até o Elasticsearch (`wan_only: true` se necessário).
4. Para `deduplication`, o path de checkpoint do GraphFrames está em storage compartilhado.
5. `spark.jars.packages`/`spark.jars.repositories` corretos para a etapa (conector ES para `indexing`/`linkage`; GraphFrames para `deduplication`).
6. Decidido se o job roda via CLI direta (`client` mode) ou via `spark-submit --deploy-mode cluster`.
7. **Ambiente air-gapped:** jars embutidos na imagem (`/opt/spark/jars`) ou em filesystem compartilhado via `spark.jars` — **nunca** `spark.jars.packages`, que exige internet em runtime. Ver [Execução Offline / Air-gapped](#execucao-offline-air-gapped-servidores-sem-internet).
