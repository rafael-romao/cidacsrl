# ADR 008: Bootstrap manual ("Poor Man's DI") em vez de container de injeção de dependências

## Status

Aceito / Implementado

## Contexto

No CIDACS-RL 3 não havia nenhuma camada de wiring dedicada: o `main()` de cada workflow (`sequential_linkage_workflow.py`, etc.) chamava as funções de processamento diretamente, sem um passo explícito de "montar dependências". A primeira formalização de uma função de bootstrap dedicada veio já dentro deste esforço de refactor — `linkage_runner.py` (renomeado para `linkage_bootstrapper.py` no commit `1571745`) já montava adapters concretos e injetava no Use Case à mão, sem nenhum framework de DI.

O que mudou com a ADR-001 (adoção de Ports and Adapters) foi a **quantidade** de peças a montar: cada vertical passou a ter múltiplos ports (ingestão, persistência, transformação, busca de candidatos, scoring, checkpoint, telemetria) e múltiplos adapters concretos possíveis para alguns deles (ex.: estratégia de busca `single` vs. `multisearch` no Elasticsearch, adapters de telemetria formatada vs. JSONL combinados via Composite). Isso aumentou a superfície de wiring manual dentro de cada função de bootstrap, criando pressão para introduzir algum mecanismo automatizado de injeção.

## Decisão

Cada vertical mantém uma função de bootstrap dedicada (`build_linkage_use_case`, `build_indexing_use_case`, `build_deduplication_use_case`, em `src/cidacsrl/bootstrap/`) que instancia adapters concretos e monta o Use Case correspondente manualmente — sem container de DI, sem decorators de injeção, sem resolução automática por tipo.

Quando a lógica condicional de montagem cresceu (ex.: escolher `SingleSearchExecutor` vs. `MultiSearchExecutor` conforme configuração; montar o `CompositeLinkageTelemetryAdapter` com um ou dois adapters internos conforme `audit_log_path` estar configurado), a resposta foi **extrair essa lógica em pequenas funções-fábrica privadas dentro do próprio módulo de bootstrap** (`_resolve_search_executor`, `_build_telemetry_adapter` em `linkage_bootstrap.py`, commit `085c88c`) — não introduzir um container de DI.

## Consequências

**Positivas:** o caminho de montagem de cada Use Case é explícito e rastreável linha a linha — não há "mágica" de resolução automática para debugar. Sem dependência de um framework externo de DI. A extração de fábricas privadas manteve as funções de bootstrap (`build_linkage_use_case` com 225 linhas no arquivo, incluindo fábricas) legíveis mesmo com o crescimento do número de ports/adapters.

**Negativas:** o crescimento do número de adapters por vertical tende a inflar continuamente o bootstrap correspondente (hoje `linkage_bootstrap.py` já é o maior dos três, com mais que o dobro de linhas dos outros dois) — o mecanismo de conter esse crescimento é puramente disciplinar (extrair mais fábricas privadas quando necessário), não estrutural. Cada nova vertical exige replicar manualmente o mesmo esqueleto de bootstrap (carregar config → validar → instanciar adapters → montar Use Case), sem reaproveitamento automático entre elas.

## Referências

- `src/cidacsrl/bootstrap/linkage_bootstrap.py`, `indexing_bootstrap.py`, `deduplication_bootstrap.py`
- Commits: `1571745` (linkage_runner → bootstrapper), `085c88c` (extração de dispatches para fábricas privadas)
- Relacionada: [ADR 001](adr-001-arquitetura-hexagonal.md) (o crescimento de ports/adapters que pressiona o wiring manual), [ADR 007](adr-007-reorganizacao-layer-feature.md) (Bootstrap como camada própria na árvore de diretórios)
