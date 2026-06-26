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
  source_path: "/data/cleaned/pacientes.parquet"
  source_format: "parquet"   # padrão; pode ser omitido
  output_path: "/data/output" # não utilizado na indexação, mas obrigatório no schema

elasticsearch:
  es_connection_url: "http://localhost:9200"
  verify_certs: false
  request_timeout: 60
  # es_user: "elastic"        # opcional
  # es_password: "senha"      # opcional
  # api_key: "chave"          # opcional

spark:
  spark_configs:
    spark.executor.memory: "4g"
    spark.driver.memory: "2g"

# Opcional: embute o caminho da spec diretamente no env
specification:
  indexing_path: "/configs/pacientes_spec.yaml"
```

### Campos do bloco `elasticsearch`

| Campo | Obrigatório | Padrão | Descrição |
|---|---|---|---|
| `es_connection_url` | Sim | — | URL completa do cluster ES |
| `verify_certs` | Não | `true` | Verificação TLS |
| `request_timeout` | Não | `30` | Timeout de requisição em segundos |
| `es_user` / `es_password` | Não | — | Autenticação básica |
| `api_key` | Não | — | Autenticação via API key |

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
| `index_as` | Não | Estratégia alternativa de indexação (ex: `keyword` para um campo `text`) |

**Dica:** use `type: text` para campos comparados com Jaro-Winkler e `type: keyword` ou `type: integer` para campos comparados com similaridade exata. Isso garante que o analisador do ES não tokenize valores que devem ser tratados como unidade.

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
