# ADR 004: Desacoplamento de logs por Telemetria Baseada em Eventos Estruturados

## Status

Aceito / Implementado

## Contexto

Mensagens de log (print e logging clássico) estavam espalhadas por todas as funções do motor, poluindo o código e acoplando lógicas de negócio ao destino final dos dados de log (stdout, arquivos).

## Decisão

Implementação do subsistema de telemetria baseada em eventos. O Domínio e os Casos de Uso agora emitem eventos estruturados (ex: `linkage_events.py`, `deduplication_events.py`). A `TelemetryPort` de cada vertical é implementada por um **adapter Composite** (`CompositeLinkageTelemetryAdapter`, `CompositeIndexingTelemetryAdapter`, `CompositeDedupTelemetryAdapter`) que despacha o mesmo evento simultaneamente para múltiplos adapters concretos: `FormattedLogTelemetryAdapter` (log estruturado no stdout, sempre presente) e, se `audit_log_path` estiver configurado, um adapter JSONL (`JsonlLinkageTelemetryAdapter`, etc.) para auditoria em disco.

Para o Linkage, o adapter JSONL grava três arquivos por job — `job.jsonl`, `phases.jsonl` e `units.jsonl`, em `{audit_log_path}/{project_name}/{job_id}/` — separando eventos de nível job, fase e work unit. A Indexação, por não ter fases nem work units, grava um único `indexing.jsonl` por job.

## Consequências

**Positivas:** Logs estruturados em formato JSON para análise de desempenho em tempo real. Negócio completamente limpo de detalhes de formatação e escrita de logs. Adicionar um novo destino de telemetria (ex.: um adapter para um sistema de métricas externo) não exige alterar o Domínio/Use Cases, só registrar mais um adapter no Composite.

**Negativas:** Os desenvolvedores precisam modelar explicitamente cada evento métrico em vez de simplesmente usar comandos soltos de `logger.info()`. Os três arquivos JSONL do Linkage (job/phases/units) não têm nenhuma relação declarada entre si além da convenção de path — cruzá-los exige que quem consome já saiba essa convenção.

## Referências

- `src/cidacsrl/adapters/outbound/telemetry/composite_linkage_telemetry_adapter.py` (contém `CompositeLinkageTelemetryAdapter` e `CompositeIndexingTelemetryAdapter`)
- `src/cidacsrl/adapters/outbound/telemetry/composite_dedup_telemetry_adapter.py`
- `src/cidacsrl/adapters/outbound/telemetry/jsonl_telemetry_adapter.py`, `formatted_log_telemetry_adapter.py`
- `src/cidacsrl/adapters/outbound/telemetry/events/linkage_events.py`, `indexing_events.py`, `deduplication_events.py`
- `src/cidacsrl/bootstrap/linkage_bootstrap.py` (`_build_telemetry_adapter`, montagem condicional do Composite)

