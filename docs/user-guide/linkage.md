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

- Valores **altos** (ex: 0.99): muita precisão, menor recall — apenas os pares mais seguros são capturados.
- Valores **baixos** (ex: 0.75): maior recall, mais falsos positivos — útil em fases finais para capturar casos difíceis.

### `es_clause_type`: `must` vs `should`

O `es_clause_type` determina o papel do campo na query Elasticsearch que recupera os candidatos:

| Valor | Efeito na busca |
|---|---|
| `must` | O documento **deve** conter um match neste campo. Candidatos sem match são excluídos. |
| `should` | O documento **preferencialmente** contém um match. Aumenta o score ES, mas não exclui. |
| `filter` | Como `must`, mas sem afetar o `_score` do ES. **Não recomendado em `ComparisonRule`** — o campo ainda participa do `match_score`; use [`indexed_dataset_filter`](#indexed_dataset_filter-restringindo-o-universo-de-candidatos-no-es) para restringir candidatos sem pontuar. |
| `must_not` | O documento **não deve** ter match neste campo. |

**Regra prática:** use `must` em fases exatas para campos que são condição necessária. Use `should` em fases fuzzy para campos que refinam o ranking mas não devem excluir candidatos.

> **Evite `es_clause_type: filter` em uma `ComparisonRule`.** A diferença entre `must` e `filter` é só o `_score` interno do Elasticsearch (exposto como `es_score` no output), não o `match_score` usado para decidir e rankear pares. Ou seja, uma `ComparisonRule` com `filter` continua participando normalmente do score do CIDACS-RL (`weight`/`similarity`/`penalty`). Se a intenção é restringir candidatos **sem** que o campo participe do score, o `ComparisonRule` é a ferramenta errada — use [`indexed_dataset_filter`](#indexed_dataset_filter-restringindo-o-universo-de-candidatos-no-es), que foi desenhado justamente para isso.

### `query_type`: como o Elasticsearch interpreta o valor

| Valor | Quando usar |
|---|---|
| `match` | Campos de texto livre; aplica análise e tokenização (padrão) |
| `term` | Campos de valor exato (keyword, integer); sem análise |
| `match_phrase` | Campos onde a ordem das palavras importa (ex: logradouro) |
| `prefix` | Busca por prefixo; útil para nomes quando apenas os primeiros caracteres são confiáveis |

### `is_fuzzy`

Quando `is_fuzzy: true`, a query `match` enviada ao Elasticsearch usa `fuzziness: AUTO`, permitindo que o ES recupere candidatos com pequenas variações ortográficas já na fase de blocagem. Só pode ser usado com `query_type: match` (o padrão).

- **Fase exata:** `is_fuzzy: false` (padrão) — blocagem restrita, apenas correspondências exatas ou próximas do analisador de texto.
- **Fase fuzzy:** `is_fuzzy: true` — blocagem ampla, recupera candidatos mesmo com erros de digitação.

### `boost`

Fator opcional (> 0) que multiplica a contribuição da cláusula no `_score` **interno do Elasticsearch**, influenciando o **ranking** dos candidatos recuperados (e, com ele, quais entram no `candidate_limit`). Não altera o `match_score` do CIDACS-RL (que vem de `weight`/`similarity`). É suportado por todos os `query_type` (`match`, `term`, `match_phrase`, `prefix`).

```yaml
- source_column: "nome_completo"
  target_column: "nome_completo"
  similarity: "jaro_winkler"
  es_clause_type: "should"
  is_fuzzy: true
  boost: 2.0        # prioriza a similaridade de nome no ranking do ES
  weight: 0.4
```

Use para priorizar campos mais discriminativos (ex.: nome) na recuperação, garantindo que o candidato correto não seja cortado pelo `candidate_limit` em fases com muitos `should`.

### Funções de Similaridade

A função de `similarity` é aplicada **após** a recuperação dos candidatos, pelo Spark, para calcular o score de cada par:

| Valor | Descrição | Quando usar |
|---|---|---|
| `jaro_winkler` | Similaridade Jaro-Winkler (0.0–1.0); favorece prefixos comuns | Nomes, logradouros, campos com possíveis erros de digitação |
| `exact` (alias: `overlap`) | 1.0 se idênticos, 0.0 caso contrário | Códigos, datas, sexo, campos discretos |
| `hamming` | Similaridade de Hamming normalizada; exige strings de mesmo comprimento | UF, CEP, código de telefone e outros campos de comprimento fixo |

> `hamming` retorna 0.0 automaticamente se os comprimentos das strings diferirem.

### `weight` e `penalty`

- **`weight`**: importância relativa da regra no score final. Regras com peso maior influenciam mais o resultado.
- **`penalty`**: valor subtraído do score quando um dos campos do par for nulo. Use para penalizar explicitamente pares com dados ausentes em campos críticos.

### `indexed_dataset_filter`: restringindo o universo de candidatos no ES

Diferente das `rules` (que pontuam e opcionalmente restringem candidatos por campo via `es_clause_type`), o `indexed_dataset_filter` adiciona cláusulas **fixas** na seção `filter` da query booleana enviada ao Elasticsearch — aplicadas a **toda busca de candidatos da fase**, sem participar do cálculo de score (assim como o clause type `filter` do ES, não afeta relevância).

É útil para restringir o índice alvo a um subconjunto relevante antes mesmo da blocagem — por exemplo, apenas registros de um determinado ano, UF ou com um status ativo.

Pode ser declarado em dois níveis, que são **combinados** (sem substituição):

- **Nível do workflow** (`indexed_dataset_filter` na raiz do `spec.yaml`): aplicado a todas as fases.
- **Nível da fase** (`indexed_dataset_filter` dentro de um item de `blocking_phases`): itens adicionais aplicados apenas àquela fase, **somados** aos do workflow (não os substituem).

Não há hoje um mecanismo para uma fase *remover* um filtro herdado do workflow — se alguma fase precisa rodar sem uma restrição definida no nível global, essa restrição não deve ficar no workflow; deve ser declarada individualmente em cada fase que precisa dela.

Cada item da lista aceita **exatamente uma** das quatro chaves abaixo:

| Chave | Formato (YAML) | Comportamento |
|---|---|---|
| `term` | `term:`<br>`  campo: "valor"` | Cláusula `term` estática do ES, com valor fixo definido na configuração. |
| `range` | `range:`<br>`  campo:`<br>`    gte: ...`<br>`    lte: ...` | Cláusula `range` estática, com limites fixos definidos na configuração. |
| `column` | Forma curta: `column: "campo"`<br>Forma longa: `column:`<br>`  source_column: "..."`<br>`  target_column: "..."` | Cláusula `term` **dinâmica**: para cada registro de origem, filtra candidatos cujo campo no índice seja igual ao valor do campo correspondente no registro fonte. A forma curta exige o mesmo nome dos dois lados; a forma longa cobre nomes divergentes. |
| `query` | `query:`<br>`  - term: {...}`<br>`  - range: {...}` | Lista de cláusulas ES arbitrárias, inseridas como estão na seção `filter`. Use para combinações que não se encaixam em `term`/`range`/`column`. |

Exemplo — todas as fases só buscam candidatos com `status: "active"` (filtro do workflow); a fase flexível soma mais duas restrições próprias (mesma UF do registro fonte e nascidos a partir de 2015):

```yaml
# pacientes_linkage_spec.yaml
indexed_dataset_filter:              # aplicado a todas as fases
  - term:
      status: "active"

blocking_phases:
  - phase_name: "fase_exata"
    # ... (sem indexed_dataset_filter próprio -> usa só o filtro do workflow acima)

  - phase_name: "fase_flexivel"
    indexed_dataset_filter:          # somado ao filtro do workflow, só nesta fase
      - column: "uf_nascimento"      # exige uf_nascimento igual em fonte e alvo
      - range:
          data_nascimento:
            gte: "2015-01-01"
    rules:
      # ...
```

Nesse exemplo, a query ES da `fase_flexivel` recebe três cláusulas na seção `filter`: `status: "active"` (herdada do workflow), a igualdade dinâmica de `uf_nascimento` e o `range` de `data_nascimento` — todas aplicadas em conjunto (AND implícito do `bool.filter` do Elasticsearch).

### Exemplos por tipo de filtro

**`term` — valor fixo, igual para todos os registros:**

```yaml
indexed_dataset_filter:
  - term:
      status: "active"
```

**`range` — faixa fixa de valores:**

```yaml
indexed_dataset_filter:
  - range:
      data_nascimento:
        gte: "2015-01-01"
        lte: "2020-12-31"
```

**`column` — igualdade dinâmica entre fonte e alvo, mesmo nome de campo dos dois lados (forma curta):**

```yaml
indexed_dataset_filter:
  - column: "uf_nascimento"   # candidato só entra se uf_nascimento (alvo) == uf_nascimento (fonte, no registro atual)
```

**`column` — igualdade dinâmica com nomes divergentes entre fonte e alvo:**

```yaml
indexed_dataset_filter:
  - column:
      source_column: "uf_paciente"      # nome do campo na tabela fonte
      target_column: "uf_nascimento"    # nome do campo no índice ES
```

**`query` — cláusulas ES arbitrárias e estáticas (não fazem substituição a partir do registro fonte):**

Use `query` quando nenhum dos outros tipos cobrir o caso — por exemplo:

- filtrar por **múltiplos valores** com `terms` (OR entre valores fixos);
- combinar numa mesma entrada cláusulas de tipos diferentes;
- usar tipos de query do ES sem equivalente direto (`exists`, `wildcard`, `ids`, etc.).

```yaml
indexed_dataset_filter:
  # Exemplo 1: OR entre municípios fixos + restrição de idade
  - query:
      - terms:
          municipio_nascimento: [2927408, 2304400, 2611606]
      - range:
          idade_anos:
            gte: 18

  # Exemplo 2: apenas registros com o campo sexo preenchido
  - query:
      - exists:
          field: "sexo"
```

> As cláusulas dentro de `query` são inseridas **como estão** na seção `filter` da query booleana do ES, sem nenhum processamento adicional. Valores não são substituídos por dados do registro fonte — para igualdade dinâmica entre fonte e alvo, use `column`.

> Um `indexed_dataset_filter` inválido (mais de uma chave em `column`/`term`/`range`/`query`, chaves erradas em `column` como dict, ou nenhuma chave em um item) falha com erro descritivo assim que o contexto da fase é montado — no preflight do bootstrap, antes do pipeline processar qualquer Work Unit ou buscar candidatos no Elasticsearch.

---

## Estrutura de Configuração

O linkage usa dois arquivos YAML separados.

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
    spark.jars.packages: "org.elasticsearch:elasticsearch-spark-30_2.12:9.1.8"

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

> **Atenção:** `workflow_name` é usado como identificador de projeto nos caminhos de checkpoint, telemetria e dados de saída. Alterar este valor após o início de um job faz com que o pipeline não encontre o checkpoint existente e reinicie do zero.

```yaml
# pacientes_linkage_spec.yaml
workflow_name: "linkage_pacientes_sinasc"
workflow_description: "Linkage entre tabela de pacientes e SINASC"

source_table: "fonte_pacientes"     # nome lógico da tabela fonte (Parquet)
id_source_table: "id_paciente"      # coluna de ID na tabela fonte

target_es_index: "pacientes"        # nome do índice Elasticsearch alvo
id_target_table: "id_sinasc"        # campo de ID no índice alvo

extra_target_fields:               # campos adicionais a retornar do índice no output
  - "data_nascimento"
  - "sexo"

indexed_dataset_filter:            # opcional; restringe candidatos no ES antes da blocagem (ver seção dedicada abaixo)
  - term:
      status: "active"

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

### Campos de uma regra de comparação

| Campo | Obrigatório | Padrão | Descrição |
|---|---|---|---|
| `source_column` | Sim | — | Coluna na tabela fonte |
| `target_column` | Sim | — | Campo no índice Elasticsearch |
| `es_clause_type` | Sim | — | `must`, `should`, `filter` ou `must_not` |
| `similarity` | Sim | — | `jaro_winkler`, `exact` ou `hamming` |
| `weight` | Sim | — | Peso da regra no score final (valor positivo) |
| `query_type` | Não | `match` | `match`, `term`, `match_phrase` ou `prefix` |
| `is_fuzzy` | Não | `false` | Usa `fuzziness:AUTO` na query ES (só com `query_type: match`) |
| `penalty` | Não | `0.0` | Valor subtraído do score quando um dos campos for nulo |

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
