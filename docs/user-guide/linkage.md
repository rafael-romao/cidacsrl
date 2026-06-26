# Guia de Uso: Linkage de Dados

O subcomando `cidacsrl linkage` executa o pipeline de linkage probabilístico: para cada registro de uma base fonte (Parquet), busca candidatos em um [índice Elasticsearch](./elasticsearch_indexing.md), calcula um score de similaridade e identifica os pares que se referem à mesma entidade.

---

## Conceitos Fundamentais

Antes de configurar o linkage, é importante entender o que acontece em cada fase.

### Sequential Blocking (Blocagem Sequencial)

O pipeline é composto por múltiplas **fases de blocagem**, executadas em sequência. Cada fase define uma estratégia diferente de busca de candidatos:

- A **fase 1** geralmente é mais restritiva (campos obrigatórios, threshold alto) e captura os pares mais óbvios com menos custo computacional.
- As **fases seguintes** são progressivamente mais permissivas, para capturar pares mais difíceis.

**Importante:** registros que encontram um match forte em uma fase são **removidos do conjunto de dados das fases seguintes**. Isso evita comparações redundantes e mantém a performance.

```
Fonte (N registros)
    │
    ▼ Fase 1 (critérios rígidos, threshold alto)
    ├── pares fortes → gravados no output
    └── sem match → continuam para a Fase 2
         │
         ▼ Fase 2 (critérios mais flexíveis, threshold menor)
         ├── pares fortes → gravados no output
         └── sem match → continuam para a Fase 3 ...
```

### Score de Similaridade

O score de cada par é calculado como uma **média ponderada** dos scores individuais de cada regra de comparação:

```
score = Σ(similaridade(campo_fonte, campo_alvo) × peso) / Σ(pesos)
```

Se um dos campos for nulo, a `penalty` da regra é subtraída do score.

### `strong_match_score_threshold`

O threshold define o score mínimo para considerar um par como match forte em uma fase. Um par com score ≥ threshold é gravado como resultado e o registro fonte é removido das fases seguintes.

- Valores **altos** (ex: 0.95): muita precisão, menor recall — apenas os pares mais seguros são capturados.
- Valores **baixos** (ex: 0.60): maior recall, mais falsos positivos — útil em fases finais para capturar casos difíceis.

### `es_clause_type`: `must` vs `should`

O `es_clause_type` determina o papel do campo na query Elasticsearch que recupera os candidatos:

| Valor | Efeito na busca |
|---|---|
| `must` | O documento **deve** conter um match neste campo. Candidatos sem match são excluídos. |
| `should` | O documento **preferencialmente** contém um match. Aumenta o score ES, mas não exclui. |
| `filter` | Como `must`, mas sem afetar o score de relevância do ES. Bom para filtros de partição. |
| `must_not` | O documento **não deve** ter match neste campo. |

**Regra prática:** use `must` para campos que são condição necessária para a busca (ex: nome em fase exata). Use `should` para campos que refinam o ranking mas não devem excluir candidatos (ex: nome da mãe em fase mais flexível).

### `query_type`: como o Elasticsearch interpreta o valor

| Valor | Quando usar |
|---|---|
| `match` | Campos de texto livre; aplica análise e tokenização (padrão) |
| `term` | Campos de valor exato (keyword, integer); sem análise |
| `match_phrase` | Campos onde a ordem das palavras importa (ex: logradouro) |
| `prefix` | Busca por prefixo; útil para nomes quando apenas os primeiros caracteres são confiáveis |

### Funções de Similaridade

A função de `similarity` é aplicada **após** a recuperação dos candidatos, pelo Spark, para calcular o score de cada par:

| Valor | Descrição | Quando usar |
|---|---|---|
| `jaro_winkler` | Similaridade Jaro-Winkler (0.0–1.0); favorece prefixos comuns | Nomes, logradouros, campos com possíveis erros de digitação |
| `exact` | 1.0 se idênticos, 0.0 caso contrário | Códigos, datas, sexo, campos discretos |
| `hamming` | Similaridade de Hamming normalizada; exige strings de mesmo comprimento | CEP, código de telefone com comprimento fixo |

> `hamming` retorna 0.0 automaticamente se os comprimentos das strings diferirem.

### `weight` e `penalty`

- **`weight`**: importância relativa da regra no score final. Regras com peso maior influenciam mais o resultado.
- **`penalty`**: valor subtraído do score quando um dos campos do par for nulo. Use para penalizar explicitamente pares com dados ausentes em campos críticos.

---

## Estrutura de Configuração

O linkage também usa dois arquivos YAML separados.

### Arquivo de Ambiente (`env.yaml`)

```yaml
# env.yaml
storage:
  source_path: "/data/cleaned/fonte.parquet"
  source_format: "parquet"
  output_path: "/data/output/linkage"
  output_format: "parquet"

execution:
  job_id: "linkage_pacientes_2024"     # gerado automaticamente se omitido
  sample_fraction: 0.1                 # processa 10% dos dados; omita para processar tudo
  sample_seed: 42
  audit_log_path: "/data/audit"        # habilita telemetria JSONL e checkpoints

  partitioning:                        # opcional; divide o job em work units independentes
    partition_column: "uf_nascimento"
    filter_partitions: ["SP", "RJ"]    # omita para processar todas as partições

elasticsearch:
  es_connection_url: "http://localhost:9200"
  verify_certs: false
  request_timeout: 60
  msearch_batch_size: 100              # número de queries por requisição multisearch
  search_strategy: "multisearch"       # "multisearch" (padrão) ou "single"

spark:
  spark_configs:
    spark.executor.memory: "8g"
    spark.driver.memory: "4g"

# Opcional: embute o caminho da spec no env
specification:
  linkage_path: "/configs/pacientes_linkage_spec.yaml"
```

### Campos do bloco `execution`

| Campo | Padrão | Descrição |
|---|---|---|
| `job_id` | gerado automaticamente | Identificador do job (usado em logs e checkpoints) |
| `sample_fraction` | `null` (todos os dados) | Fração 0.0–1.0 para amostragem reproduzível |
| `sample_seed` | `42` | Semente da amostragem |
| `audit_log_path` | `null` (desabilitado) | Caminho base para telemetria e checkpoints |
| `partitioning.partition_column` | `null` (execução global) | Coluna para dividir o job em work units |
| `partitioning.filter_partitions` | `[]` (todas) | Subconjunto de partições a processar |

### Arquivo de Especificação do Linkage (`spec.yaml`)

```yaml
# pacientes_linkage_spec.yaml
workflow_name: "linkage_pacientes_sinasc"
workflow_description: "Linkage entre tabela de pacientes e SINASC"

source_table: "fonte_pacientes"     # nome lógico da tabela fonte (Parquet)
id_source_table: "id_paciente"      # coluna de ID na tabela fonte

target_es_index: "pacientes"        # nome do índice Elasticsearch alvo
id_target_table: "id_sinasc"        # campo de ID no índice alvo

output_base_path: "/data/output/linkage"          # opcional; sobrescreve storage.output_path
final_output_filename: "final_linked_pairs.parquet"
intermediate_results_enabled: true  # grava resultados parciais por fase

extra_target_fields:               # campos adicionais a retornar do índice no output
  - "data_nascimento"
  - "sexo"

blocking_phases:
  - phase_name: "fase_exata"
    phase_description: "Busca por campos obrigatórios com critérios rígidos"
    enabled: true
    candidate_limit: 5
    strong_match_score_threshold: 0.95

    rules:
      - source_column: "nome_completo"
        target_column: "nome_completo"
        es_clause_type: "must"
        query_type: "match"
        similarity: "jaro_winkler"
        weight: 3.0
        penalty: 0.2

      - source_column: "nome_da_mae"
        target_column: "nome_da_mae"
        es_clause_type: "must"
        query_type: "match"
        similarity: "jaro_winkler"
        weight: 3.0
        penalty: 0.2

      - source_column: "municipio_nascimento"
        target_column: "municipio_nascimento"
        es_clause_type: "must"
        query_type: "term"
        similarity: "exact"
        weight: 1.0
        penalty: 0.1

  - phase_name: "fase_flexivel"
    phase_description: "Busca mais ampla para registros sem match na fase exata"
    enabled: true
    candidate_limit: 200
    strong_match_score_threshold: 0.75

    rules:
      - source_column: "nome_completo"
        target_column: "nome_completo"
        es_clause_type: "must"
        query_type: "match"
        similarity: "jaro_winkler"
        weight: 3.0
        penalty: 0.2

      - source_column: "nome_da_mae"
        target_column: "nome_da_mae"
        es_clause_type: "should"       # não obrigatório nesta fase
        query_type: "match"
        similarity: "jaro_winkler"
        weight: 3.0
        penalty: 0.1

      - source_column: "municipio_nascimento"
        target_column: "municipio_nascimento"
        es_clause_type: "should"
        query_type: "term"
        similarity: "exact"
        weight: 1.0
        penalty: 0.0
```

> Fases com `enabled: false` são silenciosamente ignoradas durante a execução.

---

## Como Executar

```bash
# Com spec referenciada no env.yaml (via specification.linkage_path)
cidacsrl linkage --env-config /configs/env.yaml

# Ou passando a spec explicitamente
cidacsrl linkage --env-config /configs/env.yaml --spec-config /configs/pacientes_linkage_spec.yaml
```

---

## Funcionalidades Operacionais

### Checkpoint e Retomada de Jobs

Quando `audit_log_path` está configurado, o pipeline grava checkpoints ao final de cada work unit processada. Se o job falhar ou for interrompido, ao ser reiniciado com o mesmo `job_id`, o pipeline **pula automaticamente as work units já concluídas** e retoma de onde parou.

Os checkpoints são gravados em:
```
{audit_log_path}/{source_table}_{target_es_index}/{job_id}/
```

### Telemetria JSONL

Com `audit_log_path` configurado, o sistema grava logs estruturados em três arquivos:

| Arquivo | Conteúdo |
|---|---|
| `job.jsonl` | Métricas globais do job (tempo total, registros processados) |
| `phases.jsonl` | Métricas por fase (pares encontrados, score médio, tempo) |
| `units.jsonl` | Métricas por work unit/partição |

Esses arquivos podem ser consumidos por ferramentas de análise (pandas, notebooks) para monitorar a qualidade do linkage fase a fase.

### Particionamento

O campo `partitioning.partition_column` divide o dataset fonte em **work units independentes** pela coluna especificada (ex: UF, ano). Cada work unit é processada separadamente, o que permite:

- Retomar jobs longos por partição (via checkpoint)
- Filtrar partições específicas com `filter_partitions` durante desenvolvimento e testes
- Paralelismo lógico sobre grandes volumes

---

## Próximos Passos

O output do linkage é um conjunto de pares de registros. Para resolver cadeias transitivas de pares e agrupar entidades únicas:

**➡️ [Deduplicação](./deduplicate.md)**
