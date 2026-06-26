# Fluxos de Execução

Cada subcomando da CLI executa um pipeline distinto. Abaixo estão os fluxos detalhados de cada um.

---

## Pipeline de Indexação

Prepara a base alvo para busca de candidatos, indexando os registros no Elasticsearch.

```mermaid
flowchart TD
    A([Início: cidacsrl indexing]) --> B[Carrega env.yaml e spec.yaml]
    B --> C[IndexDatasetUseCase.execute]
    C --> D[SparkDataIngestionAdapter\nlê Parquet / CSV]
    D --> E{Dados\ndisponíveis?}
    E -- Não --> F([Erro: storage inacessível])
    E -- Sim --> G[SparkESIndexingAdapter\nindexação em batch no Elasticsearch]
    G --> H[Telemetry: FormattedLog + JSONL]
    H --> I([Fim: índice disponível no ES])
```

---

## Pipeline de Linkage

Núcleo da plataforma. Executa blocagem por Elasticsearch e scoring probabilístico por Spark em múltiplas fases sequenciais.

```mermaid
flowchart TD
    A([Início: cidacsrl linkage]) --> B[Carrega env.yaml e spec.yaml]
    B --> C[Bootstrap: instancia adapters\n+ preflight validations]
    C --> D{Schema ES\nconsistente?}
    D -- Não --> E([Erro: schema inválido])
    D -- Sim --> F[WorkUnitOrchestrator.prepare\ndivide fonte em work units]
    F --> G[Loop: para cada Work Unit]

    subgraph WU["Por Work Unit (partição da fonte)"]
        G --> H[lê partição via\nSparkDataIngestionAdapter]
        H --> I[Loop: para cada Fase de Blocagem]

        subgraph PHASE["Por Fase"]
            I --> J{Registros\nrestantes > 0?}
            J -- Sim --> L[SparkESSearchAdapter\nbusca candidatos no ES]
            L --> M[SparkScoringAdapter\ncalcula score de similaridade]
            M --> N[DataTransformationAdapter\nfiltra por threshold]
            N --> O[SparkDataPersistenceAdapter\nsalva pares fase N]
            O --> P[DataTransformationAdapter\nexclui pareados — left-anti join]
            P --> Q[Telemetry: log de fase]
            Q -->|"próxima fase"| I
        end

        J -- Não --> K[Fases esgotadas]
        K --> R[JSONCheckpointAdapter\natualiza status COMPLETED]
    end

    R --> S{Mais\nWork Units?}
    S -- Sim --> G
    S -- Não --> T[Telemetry: log de job]
    T --> U([Fim: pares linkados em Parquet\npor fase e partição])
```

### Estratégia de Blocagem Multifase

Cada fase define suas próprias regras de comparação e threshold. Registros pareados com alta confiança em uma fase são **removidos** das fases subsequentes (left-anti join), garantindo que cada par apareça em apenas uma fase.

```mermaid
flowchart LR
    SRC["Fonte\n100% registros"] --> F1

    subgraph F1["Fase 1 — Alta precisão"]
        Q1["Busca ES restrita\n(campos exatos)"]
        S1["Score ≥ threshold_alto"]
    end

    F1 -->|"pareados removidos"| F2

    subgraph F2["Fase 2 — Cobertura"]
        Q2["Busca ES ampla\n(campos fonéticos)"]
        S2["Score ≥ threshold_médio"]
    end

    F2 -->|"pareados removidos"| FN["Fase N..."]

    F1 --> OUT1[("Pares\nfase_1/")]
    F2 --> OUT2[("Pares\nfase_2/")]
    FN --> OUTN[("Pares\nfase_N/")]
```

---

## Pipeline de Deduplicação

Resolve os pares linkados em grupos de entidades únicas usando algoritmo de componentes conectados (GraphFrames).

```mermaid
flowchart TD
    A([Início: cidacsrl deduplication]) --> B[Carrega config.yaml]
    B --> C[DeduplicateUseCase.execute]
    C --> D[SparkDataReaderAdapter\nlê pares linkados Parquet]
    D --> E[GraphFramesAdapter\nconstrói grafo de pares]
    E --> F[Connected Components\nidentifica clusters]
    F --> G[Join: cluster_id → pares originais]
    G --> H[SparkDataPersistenceAdapter\nsalva resultado com cluster_id]
    H --> I[Telemetry: FormattedLog + JSONL]
    I --> J([Fim: grupos deduplicados em Parquet])
```

### Modelo de Grafo

```mermaid
flowchart LR
    subgraph PARES["Pares Linkados (entrada)"]
        direction LR
        P1["id_fonte=A → id_alvo=X"]
        P2["id_fonte=B → id_alvo=X"]
        P3["id_fonte=C → id_alvo=Y"]
    end

    subgraph GRAFO["Grafo Não-Dirigido"]
        A --- X
        B --- X
        C --- Y
    end

    subgraph CLUSTERS["Componentes Conectados (saída)"]
        G1["cluster_1: A, B, X"]
        G2["cluster_2: C, Y"]
    end

    PARES --> GRAFO --> CLUSTERS
```

---

## Telemetria e Auditoria

Todos os pipelines emitem eventos de telemetria usando o **padrão Composite**: múltiplos adapters recebem o mesmo evento simultaneamente.

```mermaid
flowchart LR
    UC["Use Case"] --> CT["CompositeTelemetryAdapter"]
    CT --> FL["FormattedLogAdapter\nlog estruturado no stdout"]
    CT --> JL["JsonlTelemetryAdapter\nevento por linha em .jsonl"]
    JL --> DISK[("audit_log_path/\n  {projeto}/{job_id}/\n    job.jsonl\n    phases.jsonl\n    units.jsonl")]
```
