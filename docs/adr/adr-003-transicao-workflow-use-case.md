# ADR 003: Padronização de Fluxos de Execução via Use Case Pattern

## Status

Aceito / Implementado

## Contexto

No CIDACS-RL 3, cada fluxo (Linkage, Deduplicação, Indexação) era um script próprio e independente (`cidacsrl_rlp/src/workflows/sequential_linkage_workflow.py`, `deduplicate_workflow.py`, `elasticsearch_indexing_workflow.py`), cada um com seu próprio `argparse` e um único `main()` procedural misturando, no mesmo lugar: parse de argumentos, criação da sessão Spark, carregamento de configuração e a própria sequência lógica do negócio. Isso tornava os fluxos difíceis de testar isoladamente e impedia reaproveitar a lógica de orquestração fora do contexto de linha de comando.

## Decisão

Cada vertical (Linkage, Indexing, Deduplication) passou a ter um **Use Case** (`RecordLinkageUseCase`, `IndexDatasetUseCase`, `DeduplicateUseCase`) como único ponto de entrada da camada de aplicação, responsável estritamente por coordenar a ordem lógica da execução — leitura, blocagem/scoring, persistência e telemetria — sem conhecer detalhes de CLI, argparse ou infraestrutura concreta.

O adapter de entrada (`src/cidacsrl/adapters/inbound/cli/main.py`) faz apenas: parseia os argumentos, delega ao **Bootstrap** correspondente (`build_linkage_use_case`, `build_indexing_use_case`, `build_deduplication_use_case`) a montagem do Use Case com todas as dependências injetadas (padrão Bootstrap / *Poor Man's DI*, sem container), e então invoca `.execute(...)` passando os parâmetros já carregados e validados — especificação (`SequentialLinkageSpecification`, `DatasetIndexingSpecification`, `DeduplicationSpecification`), `job_id` e `ExecutionConfig`, conforme o caso.

Cada Use Case define sua própria assinatura de `execute(...)`, com os parâmetros que fazem sentido para aquele fluxo:

```python
# record_linkage_use_case.py
def execute(self, specification: SequentialLinkageSpecification, job_id: str, execution_config: ExecutionConfig) -> None: ...

# index_dataset_use_case.py
def execute(self, spec: DatasetIndexingSpecification) -> None: ...

# deduplicate_use_case.py
def execute(self, spec: DeduplicationSpecification) -> None: ...
```

Os parâmetros já são objetos de valor bem tipados (Specifications e Config models validados em `__post_init__`), então não há um "saco de campos primitivos" que justificasse embrulhar tudo em um DTO adicional — a especificação e a configuração já cumprem esse papel.

## Consequências

**Positivas:** separação clara de responsabilidades (SRP) entre parsing de entrada, montagem de dependências (Bootstrap) e orquestração de negócio (Use Case). A lógica de cada fluxo é testável isoladamente com mocks dos ports, sem precisar do argparse ou de infraestrutura real.

**Negativas:** como não há uma interface comum entre os Use Cases (cada um com sua própria assinatura de `execute`), qualquer novo adapter de entrada (ex.: uma futura API Web) precisa conhecer a assinatura específica de cada Use Case e montar seus parâmetros individualmente — não existe hoje um ponto único de dispatch genérico. Se um segundo adapter de entrada for adicionado, vale reconsiderar a introdução de um Command DTO ou de uma interface `UseCase.execute(**kwargs)` comum.

## Referências

- `src/cidacsrl/adapters/inbound/cli/main.py`
- `src/cidacsrl/bootstrap/linkage_bootstrap.py`, `indexing_bootstrap.py`, `deduplication_bootstrap.py`
- `src/cidacsrl/application/linkage/record_linkage_use_case.py`
- `src/cidacsrl/application/indexing/index_dataset_use_case.py`
- `src/cidacsrl/application/deduplication/deduplicate_use_case.py`
- Relacionada: [ADR 001](adr-001-arquitetura-hexagonal.md) (camadas Application/Bootstrap)
