# Guia de Uso: Deduplicação de Dados

O subcomando `cidacsrl deduplication` resolve o output do [linkage](./linkage.md) em grupos de entidades únicas. O linkage produz **pares** de registros relacionados; a deduplicação agrupa esses pares em **clusters**, resolvendo cadeias transitivas.

---

## Como Funciona: Abordagem por Grafo

Se o linkage identificou que A↔B e B↔C são pares, a deduplicação infere que A, B e C pertencem ao mesmo grupo — mesmo que A e C nunca tenham sido comparados diretamente.

```
Pares do linkage:    Grafo:           Componente conectado:
  A ↔ B             A — B               [A, B, C]  →  cidacs_cluster_id = A
  B ↔ C                 |
                        C
```

O processo:

1. **Leitura dos pares**: carrega o Parquet gerado pelo linkage.
2. **Construção do grafo**: cada registro é um nó; cada par é uma aresta.
3. **Componentes conectados**: identifica todos os grupos de nós ligados direta ou indiretamente (via GraphFrames).
4. **Atribuição de ID de cluster**: adiciona a coluna `cidacs_cluster_id` ao dataset, com o identificador do componente ao qual cada registro pertence.
5. **Gravação do resultado**: salva o output enriquecido em Parquet.

---

## Colunas no Output do Linkage

O output do linkage nomeia as colunas com prefixos para distinguir a origem dos dados:

- **`source_{coluna}`** — campos originados da tabela fonte (ex: `source_codigo_acidente`, `source_nome_completo`)
- **`candidate_{coluna}`** — campos originados do índice Elasticsearch (ex: `candidate_codigo_obito`, `candidate_nome_completo`)
- **`match_score`** — score final do par (média ponderada das similaridades)
- **`es_score`** — score de relevância retornado pelo Elasticsearch
- **`sim_{coluna}`** — score individual de similaridade por regra (ex: `sim_nome_completo`)
- **`phase_match`** — nome da fase de blocagem que gerou o par

Os campos `id_source_column` e `id_target_column` no config de deduplicação devem usar esses nomes prefixados.

---

## Configuração

A deduplicação usa um único arquivo YAML:

```yaml
# deduplication_config.yaml
storage:
  source_path: "/data/output/linkage/final_linked_pairs.parquet"
  source_format: "parquet"   # padrão; pode ser omitido
  output_path: "/data/output/deduplicated"
  output_format: "parquet"

deduplication:
  id_source_column: "id_paciente"    # coluna de ID da tabela fonte no output do linkage
  id_target_column: "id_sinasc"      # coluna de ID da tabela alvo no output do linkage
  output_group_id_column: "cidacs_cluster_id"  # nome da coluna de cluster no output (padrão)

app_name: "CIDACS-RL Deduplication"  # nome da SparkSession (opcional)

spark:
  spark_configs:
    spark.executor.memory: "4g"
    spark.driver.memory: "2g"
    spark.jars.packages: "graphframes:graphframes:0.8.3-spark3.5-s_2.12"
    spark.jars.repositories: "https://repos.spark-packages.org/"
```

### Campos de `deduplication`

| Campo | Obrigatório | Padrão | Descrição |
|---|---|---|---|
| `id_source_column` | Sim | — | Coluna de ID da fonte no output do linkage (inclui prefixo `source_`) |
| `id_target_column` | Sim | — | Coluna de ID do alvo no output do linkage (inclui prefixo `candidate_`) |
| `output_group_id_column` | Não | `cidacs_cluster_id` | Nome da coluna de cluster no output |

> `id_source_column` e `id_target_column` não podem ser iguais.

**Sobre o GraphFrames:** o algoritmo de componentes conectados requer o pacote `graphframes`, que não está no repositório Maven padrão. Use `spark.jars.packages` junto com `spark.jars.repositories` apontando para `https://repos.spark-packages.org/`.

---

## Como Executar

```bash
cidacsrl deduplication --config-path /configs/deduplication_config.yaml
```

Com log detalhado:

```bash
cidacsrl --log-level DEBUG deduplication --config-path /configs/deduplication_config.yaml
```

---

## Output

O resultado é um arquivo Parquet com as mesmas colunas do input mais a coluna `cidacs_cluster_id` (ou o nome configurado em `output_group_id_column`). Todos os registros de um mesmo grupo compartilham o mesmo valor nessa coluna.

Exemplo:

| id_paciente | id_sinasc | cidacs_cluster_id |
|---|---|---|
| PAC-001 | SIN-042 | PAC-001 |
| PAC-003 | SIN-042 | PAC-001 |
| PAC-007 | SIN-099 | PAC-007 |

Registros com o mesmo `cidacs_cluster_id` representam a mesma entidade no mundo real.
