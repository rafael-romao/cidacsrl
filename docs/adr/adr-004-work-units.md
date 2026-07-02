# ADR 004: Gerenciamento Resiliente de Estado com Checkpoints Baseados em Work Units

## Status

Aceito / Implementado

## Contexto

O processo de Record Linkage sequencial lida com volumes massivos de dados divididos em lotes (Work Units) processados por indexação no Elasticsearch. Se o processo falhar no lote 50 de 100, reiniciar tudo do zero geraria desperdício massivo de tempo e poder computacional.

## Decisão

A menor unidade de trabalho rastreada é a **Work Unit**: um valor da coluna de particionamento configurada (`ExecutionConfig.partitioning.partition_column`), ou uma única unidade `"global"` quando nenhum particionamento é configurado (`WorkUnitFactory.create_execution_scope`). Se a coluna está definida mas a lista de valores (`filter_partitions`) vem vazia, o `WorkUnitOrchestrator` descobre as partições dinamicamente varrendo o storage físico via `DataIngestionPort.discover_partitions`, em vez de exigir que o usuário liste manualmente partições Hive já existentes em disco.

O estado de cada Work Unit (`PENDING`/`PROCESSING`/`COMPLETED`/`FAILED`) é persistido em um único arquivo `state.json` por job (`CheckpointPort` / `JsonCheckpointAdapter`), com escrita atômica (`tmp` + `os.replace`). Ao reiniciar um job, unidades presas em `PROCESSING` (indício de crash no meio do processamento) são resetadas para `PENDING`, e todas as unidades não `COMPLETED` — incluindo as `FAILED` — são reprocessadas automaticamente na próxima execução.

## Consequências

**Positivas:** Resiliência extrema a falhas de infraestrutura. Permite pausa e retomada de rotinas massivas de linkage de forma nativa e sem corrupção de dados, sem exigir que o usuário liste partições manualmente.

**Negativas:**
- O checkpoint rastreia o estado por Work Unit inteira, não por fase — retomar uma unidade após falha na fase N recomputa as fases 1..N-1 já persistidas. Isso só não duplica registros porque a camada de persistência sobrescreve dinamicamente cada partição `phase_match=<fase>` em vez de fazer append, conforme o layout descrito na [ADR 009](adr-009-layout-particionamento-phase-match.md).
- Toda unidade `FAILED` é reprocessada automaticamente a cada restart, sem limite de tentativas nem backoff — um erro determinístico (ex.: dado malformado numa partição específica) faz o job falhar sempre no mesmo ponto indefinidamente.
- O estado de todas as Work Units de um job vive em um único arquivo `state.json`; jobs com milhares de partições reescrevem esse arquivo inteiro a cada atualização de status de uma única unidade, e uma futura operação distribuída do orquestrador (hoje centralizado no nó driver) exigiria resolver a concorrência de escrita nesse arquivo.

## Referências

- `src/cidacsrl/application/linkage/work_unit_orchestrator.py`
- `src/cidacsrl/adapters/outbound/checkpoint/json_checkpoint_adapter.py`
- `src/cidacsrl/ports/linkage/checkpoint_port.py`
- Relacionada: [ADR 009](adr-009-layout-particionamento-phase-match.md) (o layout de saída particionado por `phase_match` é o que torna segura a retomada por Work Unit inteira)

