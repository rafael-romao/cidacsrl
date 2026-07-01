# Guia de Uso: Indexação no Elasticsearch

Antes de executar o linkage, a **base alvo** precisa estar indexada no Elasticsearch. O índice é o que permite ao Spark recuperar, de forma eficiente, apenas um subconjunto pequeno de candidatos plausíveis para cada registro de origem — em vez de comparar tudo contra tudo.

## Visão Geral do Processo

O subcomando `cidacsrl indexing` executa três etapas:

1. **Leitura dos dados**: Carrega o arquivo Parquet da base alvo.
2. **Criação do índice**: Conecta-se ao Elasticsearch e cria o índice com o mapeamento definido na especificação.
3. **Ingestão dos dados**: Transfere os registros do Parquet para o índice.

---

## Estrutura de Configuração

A indexação usa dois arquivos YAML separados:

- **`env.yaml`** — configurações do ambiente: onde estão os dados, onde gravar, como conectar ao Elasticsearch e ao Spark. Reutilizável entre projetos.
- **`spec.yaml`** — especificação do índice: quais colunas indexar e como. Específico para cada dataset.

O `spec.yaml` pode ser referenciado diretamente no `env.yaml` ou passado via argumento na CLI.

---

## Arquivo de Ambiente (`env.yaml`)

```yaml
# env.yaml
storage:
  source_path: "/data/cleaned"
  source_format: "parquet"
  output_path: "/data/output"   # não utilizado na indexação, mas obrigatório no schema

elasticsearch:
  es_connection_url: "http://localhost:9200"
  verify_certs: false
  request_timeout: 60
  # es_user: "elastic"          # opcional
  # es_password: "senha"        # opcional
  # api_key: "chave"            # opcional
  # ca_certs: "/certs/ca.pem"           # opcional; CA customizada/self-signed
  # client_cert: "/certs/client.pem"    # opcional; mTLS
  # client_key: "/certs/client.key"     # opcional; mTLS
  # wan_only: true                      # opcional; ver seção "TLS e Certificados" abaixo

spark:
  spark_configs:
    spark.executor.memory: "4g"
    spark.driver.memory: "2g"
    spark.jars.packages: "org.elasticsearch:elasticsearch-spark-30_2.12:9.1.8"

# Opcional: embute o caminho da spec diretamente no env
specification:
  indexing_path: "/configs/pacientes_spec.yaml"
```

### Campos do bloco `elasticsearch`

| Campo | Obrigatório | Padrão | Descrição |
|---|---|---|---|
| `es_connection_url` | Sim | — | URL completa do cluster ES (o esquema `https://` habilita TLS na escrita via Spark) |
| `verify_certs` | Não | `true` | Verificação TLS. `false` aceita certificados self-signed |
| `ca_certs` | Não | — | Caminho para o CA cert (PEM) usado para validar o servidor, quando a CA não é pública |
| `client_cert` / `client_key` | Não | — | Certificado e chave do cliente (PEM), para mTLS |
| `request_timeout` | Não | `30` | Timeout de requisição em segundos |
| `es_user` / `es_password` | Não | — | Autenticação básica |
| `api_key` | Não | — | Autenticação via API key |
| `wan_only` | Não | `true` | Repassado como `es.nodes.wan.only` ao conector Spark na etapa `indexing` (ver abaixo) |

---

## TLS e Certificados

O bloco `elasticsearch:` alimenta **dois clientes diferentes**, e certificados se aplicam de forma um pouco distinta a cada um:

1. **Cliente de consulta** (`elasticsearch-py`) — usado para criar/verificar o índice (`indexing` e `linkage`) e para as queries de blocagem (`linkage`). Usa diretamente `verify_certs`, `ca_certs`, `client_cert` e `client_key` como arquivos **PEM**.
2. **Escrita em massa via Spark** (`org.elasticsearch.spark.sql`, usada apenas na etapa `indexing` para gravar o DataFrame no índice) — usa o conector ES-Hadoop, que enxerga TLS de forma diferente: `es.net.ssl` é ligado automaticamente quando `es_connection_url` começa com `https://`; `verify_certs: false` mapeia para `es.net.ssl.cert.allow.self.signed`. Esse conector, no entanto, **não aceita CA/cliente em PEM** — ele espera um *truststore*/*keystore* Java (`.jks` ou `.p12`).

Para apontar um truststore/keystore Java para a escrita em massa (ex: quando a CA do cluster ES não é pública), use as chaves cruas do conector — qualquer chave prefixada com `es.` no bloco `elasticsearch:` é repassada como está para o `DataFrameWriter`, sem precisar de suporte nomeado explícito:

```yaml
elasticsearch:
  es_connection_url: "https://elasticsearch.exemplo.com:9243"
  verify_certs: true
  ca_certs: "/certs/ca.pem"              # usado pelo cliente de consulta (indices.create, ping)
  es.net.ssl.truststore.location: "/certs/truststore.jks"   # usado pela escrita em massa via Spark
  es.net.ssl.truststore.pass: "senha-do-truststore"
```

> Gere o truststore Java a partir do PEM com `keytool -importcert -file ca.pem -keystore truststore.jks -alias es-ca`.

### Se a CA já está instalada no cluster

Se o time de infraestrutura já distribuiu a CA (self-signed ou interna) nos **trust stores padrão de todos os nós** — driver e executors —, nenhuma configuração extra de certificado é necessária no YAML: basta `verify_certs: true` (o padrão) e a conexão funciona para os dois clientes, sem `ca_certs` nem `es.net.ssl.truststore.location`.

Isso porque os dois clientes usam trust stores diferentes, então a CA precisa estar instalada em **ambos** os lugares:

- **Cliente de consulta** (`elasticsearch-py`) confia no trust store **do sistema operacional** por padrão (ex: `/etc/ssl/certs` em Debian/Ubuntu). Basta a CA ter sido adicionada via `update-ca-certificates` (ou equivalente da distro) na máquina onde o driver roda.
- **Escrita em massa via Spark** (conector ES-Hadoop) confia no trust store **da JVM** (`$JAVA_HOME/lib/security/cacerts`) por padrão. A CA precisa ter sido importada nesse keystore com `keytool` — e, como o job pode rodar em qualquer executor, isso precisa estar replicado em **todos os nós do cluster** (normalmente via provisionamento/imagem base, não por job).

Como conferir se já está instalada:

```bash
# Trust store do sistema operacional
openssl s_client -connect elasticsearch.exemplo.com:9243 -CApath /etc/ssl/certs </dev/null

# Trust store da JVM
keytool -list -keystore "$JAVA_HOME/lib/security/cacerts" -storepass changeit | grep -i es-ca
```

Se ambos confirmarem a CA como confiável, `ca_certs`/`es.net.ssl.truststore.location` só voltam a ser necessários se algum nó novo for adicionado ao cluster sem o provisionamento da CA, ou se você quiser isolar a confiança apenas para o job do `cidacsrl` em vez de todo o sistema.

---

## Arquivo de Especificação do Índice (`spec.yaml`)

```yaml
# pacientes_spec.yaml
source_config:
  source_table: "pacientes"   # nome lógico do dataset (usado em logs)
  id_field: "id_paciente"     # campo do Parquet usado como _id no Elasticsearch

index_config:
  name: "pacientes"           # nome do índice no Elasticsearch
  id_from_source: true        # usa id_field como _id do documento ES
  number_of_shards: 3         # paralelismo de busca; 1 shard por nó é um bom ponto de partida
  number_of_replicas: 0       # 0 para índices de leitura intensiva sem necessidade de HA
  refresh_interval: "30s"     # intervalo maior acelera a ingestão; reduza após indexar

index_columns:
  - name: nome_completo
    type: text

  - name: nome_da_mae
    type: text

  - name: municipio_nascimento
    type: integer

  - name: uf_nascimento
    type: integer

  - name: data_nascimento
    type: keyword              # campos de data usados em comparação exata são indexados como keyword
    index_as: keyword
```

### Campos de `index_columns`

| Campo | Obrigatório | Descrição |
|---|---|---|
| `name` | Sim | Nome da coluna no Parquet e no índice ES |
| `type` | Sim | Tipo ES: `text`, `keyword`, `integer`, `long`, `float`, `date`, etc. |
| `index_as` | Não | Estratégia alternativa de indexação para campos `text` ou `keyword`: `"keyword"`, `"text"` ou `"both"` |
| `format` | Não | Formato de data (ex: `"yyyy-MM-dd"`); aplicável quando `type: date` |

**Sobre `index_as: both`:** cria o campo como `text` (com análise para busca por token) e adiciona automaticamente um subcampo `.keyword` (sem análise). Isso permite usar o mesmo campo tanto em queries `match` (busca fuzzy de texto) quanto em queries `term` (comparação exata). Recomendado para campos de nome usados com Jaro-Winkler.

**Sobre tipos e similaridade no linkage:**
- Use `type: text` + `index_as: both` para campos comparados com Jaro-Winkler (ex: nomes)
- Use `type: keyword` para campos comparados com similaridade exata (ex: UF, código de município)
- Use `type: date` com `format` para campos de data

---

## Como Executar

```bash
# Com spec referenciada no env.yaml (via specification.indexing_path)
cidacsrl indexing --env-config /configs/env.yaml

# Ou passando a spec explicitamente
cidacsrl indexing --env-config /configs/env.yaml --spec-config /configs/pacientes_spec.yaml
```

O argumento `--spec-config` tem precedência sobre `specification.indexing_path` no `env.yaml`.

Para aumentar o nível de log durante a ingestão:

```bash
cidacsrl --log-level DEBUG indexing --env-config /configs/env.yaml
```

---

## Próximos Passos

Com o índice criado, siga para a execução do linkage entre a base fonte e o índice recém-criado.

**➡️ [Linkage de Dados](./linkage.md)**
