# CIDACS-RL — Plataforma de Linkage de Registros

Bem-vindo à documentação da **CIDACS-RL**, uma engine de linkage de registros probabilístico desenvolvida para conectar registros de diferentes bases de dados que se referem à mesma entidade — por exemplo, um mesmo paciente em sistemas de saúde distintos — mesmo na ausência de um identificador único universal.

A plataforma é projetada para escala: usa **Apache Spark** para processamento distribuído e **Elasticsearch** como motor de busca de candidatos, permitindo operar sobre dezenas de milhões de registros com controle fino de performance e rastreabilidade.

---

## Como o sistema funciona

O processo de linkage é dividido em quatro etapas. As três últimas são orquestradas pela CLI `cidacsrl`:

```
Dados Brutos (CSV / Parquet)
         │
         ▼
  [ Limpeza e Harmonização ]   ← pré-requisito externo (script / notebook)
         │
         ▼
  Dados Limpos (Parquet)
         │
         ├──────────────────────────────────────────┐
         ▼                                          ▼
  cidacsrl indexing                         cidacsrl linkage
  (indexa a base alvo                       (consulta o índice,
   no Elasticsearch)                         compara e pontua pares)
                                                     │
                                                     ▼
                                          Pares Linkados (Parquet)
                                                     │
                                                     ▼
                                          cidacsrl deduplication
                                          (resolve cadeias de pares
                                           em grupos únicos)
                                                     │
                                                     ▼
                                          Grupos Deduplicados (Parquet)
```

### Por que o Elasticsearch está no meio?

Comparar cada registro de uma base com todos os registros de outra é computacionalmente inviável em escala. O Elasticsearch resolve isso com **blocking**: para cada registro de origem, ele recupera apenas um subconjunto pequeno de candidatos plausíveis, usando análise de texto e índices invertidos. O Spark então calcula o score de similaridade só sobre esses candidatos.

---

## Os três subcomandos da CLI

| Subcomando | O que faz |
|---|---|
| `cidacsrl indexing` | Indexa uma base de dados (Parquet) no Elasticsearch |
| `cidacsrl linkage` | Executa o pipeline de linkage entre fonte e índice |
| `cidacsrl deduplication` | Agrupa pares linkados em entidades únicas via grafo |

A interface de linha de comando aceita um flag global de log antes do subcomando:

```bash
cidacsrl --log-level DEBUG linkage --env-config env.yaml
```

Níveis disponíveis: `DEBUG`, `INFO` (padrão), `WARNING`, `ERROR`, `CRITICAL`.

---

## Guias de Uso

- **[Limpeza de Dados](./user-guide/cleaning.md)** — Como preparar e harmonizar os dados antes do linkage.
- **[Indexação no Elasticsearch](./user-guide/elasticsearch_indexing.md)** — Como indexar a base alvo para busca de candidatos.
- **[Linkage de Dados](./user-guide/linkage.md)** — Como configurar e executar o pipeline de linkage probabilístico.
- **[Deduplicação](./user-guide/deduplicate.md)** — Como resolver grupos de pares em entidades únicas.

---

## Primeiros passos

O ponto de entrada recomendado é preparar e indexar a base alvo antes de executar o linkage.

**➡️ Comece por [Limpeza de Dados](./user-guide/cleaning.md)** se os seus dados ainda não estão harmonizados, ou vá direto para **[Indexação no Elasticsearch](./user-guide/elasticsearch_indexing.md)** se os dados já estão prontos.
