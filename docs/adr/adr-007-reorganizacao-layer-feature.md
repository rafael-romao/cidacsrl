# ADR 007: Reorganização da árvore de diretórios de feature → layer para layer → feature

## Status

Aceito / Implementado

## Contexto

No CIDACS-RL 3, a árvore era plana e agrupada só por feature/tecnologia, sem nenhuma camada arquitetural: `cidacsrl_rlp/src/{cleaning,config,es,linkage,utils,workflows}/`. Já dentro deste esforço de refactor, antes da reorganização final, a árvore passou por um estado intermediário ainda agrupado primeiro por feature/módulo e só depois por camada: `cleaning/` era um módulo próprio no topo, com sua própria mini-estrutura de camadas (`cleaning/application/`, `cleaning/domain/`, `cleaning/infra/`); `core/` concentrava Linkage e Indexing juntos, também com Domain/Application/Ports/Infra aninhados por dentro (`core/application/domain/`, `core/application/ports/outbound/`, `core/infra/adapters/outbound/`).

Esse layout já criava atrito antes mesmo de Deduplication existir como vertical própria: adapters de infraestrutura cross-cutting (telemetria, checkpoint) viviam misturados em `core/infra/adapters/outbound/` sem separação por vertical (um único `composite_telemetry_adapter.py`, um único `json_checkpoint_adapter.py` para tudo). Conforme a ADR-001 formalizou a separação Domain/Application/Ports/Adapters como o eixo estrutural do sistema, e novas verticais (Indexing, Deduplication) passaram a exigir a pilha de camadas completa cada uma, agrupar por feature primeiro implicava duplicar essa pilha de camadas dentro de cada módulo, ou continuar misturando as verticais dentro de um único `core/`.

## Decisão

A árvore foi reorganizada para ter a **camada arquitetural como eixo primário** no topo de `src/cidacsrl/` — `domain/`, `application/`, `ports/`, `adapters/`, `bootstrap/`, `config/` — e a **vertical/feature como eixo secundário**, aninhada dentro de cada camada:

```
src/cidacsrl/
├── domain/{linkage,indexing,deduplication,cleaning}/
├── application/{linkage,indexing,deduplication,cleaning}/
├── ports/{linkage,indexing,deduplication}/
├── adapters/{inbound,outbound}/{spark,elasticsearch,graph,telemetry,checkpoint}/
├── bootstrap/
└── config/
```

A migração foi feita incrementalmente, uma camada por vez, cada uma em seu próprio commit: `4386435` (Domain e Config), `82d570e` (Ports e Adapters), `6d1c238` (Application e Bootstrappers), `d6d4e33` (Adapters Outbound), `2a545bc` (Bootstrappers), `b29d0da` (Adapters Inbound), `b8a3e33` (Tests) — sob o guarda-chuva do commit inicial `0c535a2`.

## Consequências

**Positivas:** a camada arquitetural (o eixo que a ADR-001 elegeu como central) passa a ser a primeira coisa visível na árvore de diretórios, e não algo que só se percebe lendo o código. Auditar limites de dependência (ex.: "nada em `domain/` importa de `adapters/`") fica mais direto, já que todas as instâncias de uma camada estão sob um único prefixo. Adapters cross-cutting (telemetria, checkpoint) ficam naturalmente separados por vertical dentro de `adapters/outbound/telemetry/` (`composite_linkage_telemetry_adapter.py`, `composite_dedup_telemetry_adapter.py`, etc.), em vez de um único arquivo genérico atendendo a tudo.

**Negativas:** navegar "tudo sobre a vertical de Linkage" agora exige percorrer múltiplas pastas de topo (`domain/linkage/`, `application/linkage/`, `ports/linkage/`, `adapters/outbound/spark/` + `elasticsearch/`, `bootstrap/linkage_bootstrap.py`) em vez de uma única subárvore coesa — pior localidade para quem está fazendo uma mudança ponta-a-ponta isolada em uma vertical específica. A migração também teve custo de ondulação sobre toda a base de testes (`b8a3e33`), que precisou ser reorganizada em espelho à nova estrutura de produção.

## Referências

- `src/cidacsrl/domain/`, `src/cidacsrl/application/`, `src/cidacsrl/ports/`, `src/cidacsrl/adapters/`, `src/cidacsrl/bootstrap/`, `src/cidacsrl/config/`
- Commits: `0c535a2`, `4386435`, `82d570e`, `6d1c238`, `d6d4e33`, `2a545bc`, `b29d0da`, `b8a3e33`
- Relacionada: [ADR 001](adr-001-arquitetura-hexagonal.md) (a camada arquitetural que esta reorganização torna visível na árvore de diretórios)
- Relacionada: [ADR 008](adr-008-bootstrap-sem-container-di.md) (Bootstrap como camada própria nesta árvore)
