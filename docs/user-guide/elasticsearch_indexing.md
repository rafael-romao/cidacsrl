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
# obitos_spec.yaml
source_config:
  source_table: "obitos"      # nome lógico do dataset (usado em logs)
  id_field: "codigo_obito"    # campo do Parquet usado como _id no Elasticsearch

index_config:
  name: "obitos"              # nome do índice no Elasticsearch
  id_from_source: true        # usa id_field como _id do documento ES
  number_of_shards: 3         # paralelismo de busca; 1 shard por nó é um bom ponto de partida
  number_of_replicas: 0       # 0 para índices de leitura intensiva sem necessidade de HA
  refresh_interval: "30s"     # intervalo maior acelera a ingestão; reduza após indexar
  # Opcional: bloco 'analysis' cru do ES (analyzers/tokenizers/filters customizados),
  # referenciável por qualquer coluna via a propriedade 'analyzer'.
  analysis:
    analyzer:
      folding:
        tokenizer: standard
        filter: ["lowercase", "asciifolding"]

index_columns:
  # Nome comparado com Jaro-Winkler: text (busca por token) + subcampo .keyword (exato)
  - name: nome_completo
    type: text
    index_as: both

  - name: nome_da_mae
    type: text
    index_as: both

  # Texto com analyzer customizado (definido em index_config.analysis)
  - name: observacao
    type: text
    analyzer: folding

  # Data com formato explícito
  - name: data_obito
    type: date
    format: "yyyy-MM-dd"

  # Comparação exata (UF, código de município): keyword puro
  - name: uf_obito
    type: keyword

  # Keyword com limite de tamanho indexado
  - name: numero_declaracao
    type: keyword
    ignore_above: 32

  # Campos escalares
  - name: idade_obito
    type: integer
  - name: obito_violento
    type: boolean
```

### Propriedades de `index_config`

| Propriedade | Obrigatório | Padrão | Valores possíveis | Descrição |
|---|---|---|---|---|
| `name` | Sim | — | string | Nome do índice no Elasticsearch |
| `id_from_source` | Não | `false` | `true` / `false` | Se `true`, usa `source_config.id_field` como `_id` do documento (ingestão via `upsert`); se `false`, o ES gera o `_id` (ingestão via `index`) |
| `number_of_shards` | Não | `1` | inteiro > 0 | Número de shards primários |
| `number_of_replicas` | Não | `0` | inteiro ≥ 0 | Número de réplicas por shard |
| `refresh_interval` | Não | `"1s"` | ex.: `"1s"`, `"30s"`, `"-1"` | Intervalo de refresh; valores maiores (ou `"-1"`) aceleram a ingestão em massa |
| `analysis` | Não | — | objeto `analysis` do ES | Definição crua de analyzers/tokenizers/filters customizados, repassada ao settings do índice. Cada analyzer aqui pode ser referenciado por colunas via `analyzer` |

### Propriedades de `index_columns`

| Propriedade | Obrigatório | Aplicável a | Valores possíveis | Descrição |
|---|---|---|---|---|
| `name` | Sim | todos | string | Nome da coluna no Parquet e no índice ES |
| `type` | Sim | — | `text`, `keyword`, `date`, `integer`, `long`, `float`, `double`, `boolean` (o alias `string` é normalizado para `text`) | Tipo do campo no Elasticsearch |
| `index_as` | Não | `type: text` | `"text"`, `"keyword"`, `"both"` | Estratégia de indexação do campo textual (ver abaixo) |
| `format` | Não | `type: date` | string de formato do ES, ex.: `"yyyy-MM-dd"`, `"yyyy-MM-dd HH:mm:ss"`, `"epoch_millis"`, ou combinações com `||` | Formato de parsing da data. Sem ele, o ES usa `strict_date_optional_time||epoch_millis` |
| `analyzer` | Não | `type: text` | nome de analyzer built-in (ex.: `"brazilian"`, `"standard"`) ou custom definido em `index_config.analysis` | Analyzer aplicado na indexação/busca full-text do campo |
| `ignore_above` | Não | `type: keyword` (e o subcampo `.keyword` de `index_as: both`) | inteiro | Strings mais longas que este limite não são indexadas. Default do subcampo `.keyword` em `both`: `256` |

**Valores de `index_as` (para `type: text`):**

| Valor | Mapping resultante | Uso |
|---|---|---|
| `"text"` (ou omitido) | `{"type": "text"}` | Só busca full-text (queries `match`) |
| `"keyword"` | `{"type": "keyword"}` | Só comparação exata — o campo textual é indexado como keyword (queries `term`) |
| `"both"` | `text` + subcampo `.keyword` | Ambos: `match` no campo e `term` em `campo.keyword`. Recomendado para nomes usados com Jaro-Winkler |

### Analyzers

Um *analyzer* define como o Elasticsearch processa um campo `text` no momento da indexação e da busca. Ele é composto por três estágios em sequência:

1. **Character filters** — pré-processam o texto bruto (ex.: remover tags HTML).
2. **Tokenizer** — divide o texto em tokens (normalmente por espaços e pontuação).
3. **Token filters** — transformam cada token (ex.: converter para minúsculas, remover acentos, aplicar stemming).

Se nenhum `analyzer` for especificado em um campo `text`, o ES usa o `standard` por padrão, que tokeniza por espaços/pontuação e aplica `lowercase`.

**Analyzers built-in úteis para dados brasileiros:**

| Analyzer | O que faz | Quando usar |
|---|---|---|
| `standard` (padrão) | Tokeniza + lowercase | Campo genérico sem necessidade de normalização especial |
| `brazilian` | Tokeniza + lowercase + stemming do português | Texto livre em português onde variações morfológicas devem casar (ex.: "hospitalar" e "hospital") |
| `simple` | Tokeniza só por não-letras + lowercase | Campos que não devem sofrer stemming |

**Quando usar um analyzer customizado** (definido em `index_config.analysis`):

- Você precisa remover acentos (`asciifolding`) sem stemming — o `brazilian` faz stemming, o que pode ser indesejável para nomes próprios.
- Você quer combinar filtros específicos. Exemplo do `folding` do YAML de exemplo:
  ```yaml
  analysis:
    analyzer:
      folding:
        tokenizer: standard
        filter: ["lowercase", "asciifolding"]  # normaliza "José" → "jose"
  ```
  Esse analyzer é ideal para campos de texto comparados por blocos fonéticos ou por prefixo, onde a diferença de acento não deve impedir o match.

> **Atenção:** o `analyzer` só se aplica a campos `type: text`. Para campos `keyword`, use `normalizer` (configuração nativa do ES, fora do escopo desta spec).

---

**Combinações típicas por uso no linkage:**

| Caso de uso | Configuração recomendada | Motivo |
|---|---|---|
| Nomes comparados com Jaro-Winkler (nome, nome da mãe) | `type: text` + `index_as: both` | Permite blocagem por token (`match`) e similaridade exata sobre o valor bruto (`.keyword`) |
| Nomes com variação de acento ou caixa | `type: text` + `index_as: both` + `analyzer: folding` (custom) | Remove acentos na busca sem perder o valor original no `.keyword` |
| UF, código de município, CBO, identificadores curtos | `type: keyword` | Comparação exata; stemming/tokenização seria prejudicial |
| Datas de nascimento, óbito, internação | `type: date` + `format: "yyyy-MM-dd"` | Permite filtros de range na blocagem; sem `format` o ES pode recusar o parsing |
| Texto livre em português (observações, diagnósticos) | `type: text` + `analyzer: "brazilian"` | Stemming normaliza variações morfológicas comuns |
| Campos numéricos ou booleanos trazidos ao resultado | `type: integer` / `type: boolean` | Sem indexação textual; recuperados como `extra_target_fields` no [linkage](./linkage.md) |
| Identificadores longos (CNS, CPF) que devem ser preservados inteiros | `type: keyword` + `ignore_above: 20` | Evita indexar strings truncadas; CPF tem 11 dígitos, CNS tem 15 |

> **Nota:** apenas as propriedades acima são aplicadas ao mapping. Propriedades não listadas são ignoradas. O alias legado `type: string` é convertido para `text` automaticamente (o ES não possui mais o tipo `string`).

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
