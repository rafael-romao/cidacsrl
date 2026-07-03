# Visão Geral da Arquitetura

A CIDACS-RL segue o padrão **Hexagonal (Ports & Adapters)**, com três verticais independentes: **Linkage**, **Indexing** e **Deduplication**. Cada vertical possui seu próprio conjunto de ports (interfaces) e adapters (implementações), orquestrado por um use case na camada de aplicação.

---

## Contexto do Sistema

O diagrama abaixo mostra como a CIDACS-RL se posiciona em relação aos atores e sistemas externos.

```mermaid
flowchart TD
    USER(["👤 Cientista / Engenheiro de Dados"])

    subgraph EXT["Sistemas Externos"]
        STORAGE[("Storage\nParquet / CSV")]
        ES[("Elasticsearch\nMotor de Blocagem")]
    end

    subgraph CIDACSRL["CIDACS-RL"]
        CLI["CLI\ncidacsrl indexing | linkage | deduplication"]
    end

    USER -->|"executa via terminal"| CLI
    CLI -->|"lê dados brutos"| STORAGE
    CLI -->|"indexa / consulta candidatos"| ES
    CLI -->|"salva pares linkados\ne grupos deduplicados"| STORAGE
```

---

## Arquitetura em Camadas

A plataforma é organizada em seis camadas com fluxo de dependência de fora para dentro — nenhuma camada interna conhece as externas.

```mermaid
flowchart TB
    subgraph INBOUND["Adaptadores de Entrada"]
        CLI2["CLI (argparse)\nadapters/inbound/cli"]
    end

    subgraph BOOTSTRAP["Bootstrap / DI"]
        BS["linkage_bootstrap\nindexing_bootstrap\ndeduplication_bootstrap"]
    end

    subgraph CONFIG["Config"]
        CFG["loader · dedup_loader\nmodels: ExecutionConfig\nLinkageSpec · IndexingSpec"]
    end

    subgraph APP["Aplicação (Use Cases)"]
        UC1["RecordLinkageUseCase"]
        UC2["IndexDatasetUseCase"]
        UC3["DeduplicateUseCase"]
    end

    subgraph DOMAIN["Domínio"]
        D1["LinkageSpecification\nMatchingRules · ScoringEngine"]
        D2["IndexingSpecification"]
        D3["DeduplicationSpecification"]
        D4["Cleaning · Column"]
    end

    subgraph PORTS["Ports (Interfaces)"]
        P1["linkage: DataIngestion · GetCandidates\nScoring · DataTransformation\nDataPersistence · Checkpoint · Telemetry"]
        P2["indexing: DataIndexing · Telemetry"]
        P3["deduplication: DataReader\nGraphProcessing · DataPersistence · Telemetry"]
    end

    subgraph OUTBOUND["Adaptadores de Saída"]
        A1["Spark: DataIngestion · DataPersistence\nDataTransformation · Scoring · DataReader"]
        A2["Elasticsearch: SparkESSearch\nSparkESIndexing"]
        A3["GraphFrames: GraphFramesAdapter"]
        A4["Telemetry: FormattedLog · JSONL\n(padrão Composite)"]
        A5["Checkpoint: JSONCheckpointAdapter"]
    end

    CLI2 -->|"invoca para montar\nas dependências"| BOOTSTRAP
    CLI2 -->|"executa o use case\njá montado"| APP
    BOOTSTRAP -->|"carrega e valida\nconfig/specs"| CONFIG
    BOOTSTRAP -->|"instancia e injeta\nno use case"| APP
    BOOTSTRAP -->|"instancia os\nadapters concretos"| OUTBOUND
    APP -->|"depende de\n(interfaces)"| PORTS
    APP -->|"orquestra regras\nde negócio"| DOMAIN
    OUTBOUND -.->|"implementa"| PORTS
```

---

## Estrutura de Pacotes

```mermaid
flowchart LR
    subgraph SRC["src/cidacsrl/"]
        direction TB
        PKG_IN["adapters/inbound/\n  cli/"]
        PKG_OUT["adapters/outbound/\n  spark · elasticsearch\n  graph · telemetry · checkpoint"]
        PKG_B["application/\n  linkage · indexing · deduplication"]
        PKG_C["bootstrap/\n  linkage · indexing · deduplication"]
        PKG_D["config/\n  loader · dedup_loader · models/"]
        PKG_E["domain/\n  linkage · indexing\n  deduplication · cleaning"]
        PKG_F["ports/\n  linkage · indexing · deduplication"]
    end

    PKG_IN -->|"chama"| PKG_C
    PKG_C -->|"instancia"| PKG_OUT
    PKG_C -->|"monta"| PKG_B
    PKG_C -->|"carrega"| PKG_D
    PKG_B -->|"depende de"| PKG_F
    PKG_B -->|"orquestra"| PKG_E
    PKG_B -->|"lê"| PKG_D
    PKG_OUT -.->|"implementa"| PKG_F
    PKG_OUT -->|"usa tipos de"| PKG_E
    PKG_OUT -->|"lê"| PKG_D
    PKG_F -->|"usa tipos de"| PKG_E
    PKG_D -->|"usa tipos de"| PKG_E
```

---

## Padrão de Injeção de Dependências

Não há contêiner de DI — o **Bootstrap** instancia e conecta todos os objetos manualmente, seguindo o padrão *Poor Man's DI*. A CLI chama o bootstrap, que retorna o use case já montado com todas as dependências injetadas.

```mermaid
sequenceDiagram
    actor User
    participant CLI
    participant Bootstrap
    participant Config
    participant Adapters
    participant UseCase

    User->>CLI: cidacsrl linkage --env-config env.yaml
    CLI->>Config: load_yaml(env_config)
    CLI->>Bootstrap: build_linkage_use_case(configs...)
    Bootstrap->>Config: parse specs & configs
    Bootstrap->>Adapters: instancia Spark, ES, Telemetry, Checkpoint
    Bootstrap->>Bootstrap: _run_preflight_validations()
    Bootstrap-->>CLI: (use_case, spec, config, spark)
    CLI->>UseCase: use_case.execute(spec, job_id, config)
    UseCase-->>CLI: resultado / exceção
    CLI->>CLI: spark.stop()
```
