# ADR 011: `indexed_dataset_filter` — restrições de candidatos fora do score

## Status

Aceito / Implementado

## Contexto

As `rules` de uma `BlockingPhase` (`ComparisonRule`) acoplam duas responsabilidades por natureza: comparar um campo fonte com um campo alvo e, com isso, sempre contribuir para o score do CIDACS-RL via `similarity`/`weight`/`penalty`. O `es_clause_type` (`must`/`should`/`filter`/`must_not`) só determina o papel do campo na recuperação de candidatos no Elasticsearch e o `_score` interno do ES — nunca isola a regra do `match_score` por si só. A única forma de neutralizar essa contribuição é zerar `weight` explicitamente; não há um jeito direto de declarar "isso é só um filtro". Isso é aceitável quando a restrição é, ela mesma, uma comparação relevante entre fonte e alvo, mas não cobre casos comuns de blocagem:

- Restringir o índice alvo a um subconjunto fixo, sem relação com nenhum campo da fonte (ex: `status: "active"`, um recorte por ano de referência).
- Exigir igualdade entre fonte e alvo em um campo que não participa do score (ex: mesma UF), sem precisar declarar uma `ComparisonRule` inteira com `similarity`/`weight` fictícios só para ganhar uma cláusula `filter`.
- Combinar cláusulas Elasticsearch arbitrárias (`range`, buscas geoespaciais, etc.) que não se encaixam no vocabulário limitado de `query_type` suportado pelas regras de comparação (`match`, `term`, `match_phrase`, `prefix`).

## Decisão

Foi introduzido `indexed_dataset_filter` como uma lista de restrições desacopladas das `ComparisonRule` — a maioria estática, com uma forma dinâmica (`column`) — aplicada à seção `filter` da query booleana do Elasticsearch em toda busca de candidatos de uma fase. Nenhuma delas participa do cálculo de score; todas apenas restringem o universo de candidatos retornado pelo ES.

Estrutura e validação centralizadas em `IndexedDatasetFilterItem` (`config/models/indexed_dataset_filter.py`), que exige exatamente uma entre quatro formas por item:

- `term`: cláusula `term` com valor fixo definido na configuração.
- `range`: cláusula `range` com limites fixos definidos na configuração.
- `column`: cláusula `term` **dinâmica**, resolvida em tempo de execução pelo `ElasticsearchQueryBuilder` a partir do valor do registro fonte — cobre o caso de "exigir igualdade em campo que não pontua" sem precisar de uma `ComparisonRule` fictícia. Aceita uma string (`"uf"`, mesmo nome nos dois lados) ou um dict `{"source_column": ..., "target_column": ...}` quando o nome do campo diverge entre fonte e índice (`IndexedDatasetFilterItem.column_source_name`/`column_target_name` resolvem os dois formatos).
- `query`: lista de cláusulas ES arbitrárias, repassadas como estão — escape hatch para combinações não previstas nas outras três formas.

O filtro pode ser declarado em dois níveis, resolvidos em `SequentialLinkageSpecification.build_blocking_phase_context`:

- No nível do workflow (`SequentialLinkageSpecification.indexed_dataset_filter`), aplicado a todas as fases.
- No nível da fase (`BlockingPhase.indexed_dataset_filter`), com itens **somados** aos do workflow (merge aditivo) — a fase que precisa de uma restrição extra só declara o que quer adicionar, sem precisar repetir o filtro do workflow.

A validação (exatamente uma chave por item, tipos corretos) ocorre em `IndexedDatasetFilterItem.from_dict`, chamada por `build_blocking_phase_context`. Na prática isso acontece no preflight do bootstrap (`_run_preflight_validations`), como efeito colateral de `get_required_target_columns()` — usada para validar o mapeamento do índice ES, não para validar filtros — e não durante o parsing da especificação em si (`SequentialLinkageSpecification.from_dict` não valida o conteúdo do `indexed_dataset_filter`). Um filtro malformado falha, portanto, antes do `RecordLinkageUseCase.execute` processar qualquer Work Unit ou buscar candidatos no Elasticsearch.

## Consequências

**Positivas:** separa claramente "o que participa do score" (`ComparisonRule`) de "o que apenas restringe o universo de busca" (`indexed_dataset_filter`), evitando regras de comparação artificiais só para obter uma cláusula `filter`. A validação centralizada em `IndexedDatasetFilterItem` garante um erro descritivo já no preflight do bootstrap, em vez de um erro genérico (ou uma query malformada aceita silenciosamente) apenas na construção da query ES, já durante o processamento de uma Work Unit.

**Negativas:** o merge é só aditivo — não há como uma fase *remover* um filtro herdado do workflow. Se uma restrição não deve valer para todas as fases, ela não pode ficar no nível do workflow; precisa ser repetida individualmente em cada fase que precisa dela, o que gera alguma duplicação em specs com muitas fases. A forma `query` continua sendo puramente estática (sem acesso ao registro fonte sendo processado) — não é um substituto para igualdade dinâmica; use `column` (string ou dict) para isso.

À parte disso, vale registrar por que **evitar `es_clause_type: "filter"` sozinho (sem `weight: 0`) em uma `ComparisonRule`** como alternativa ao `indexed_dataset_filter`: a única diferença real entre `must` e `filter` é o `_score` interno do Elasticsearch (exposto como `es_score` na saída) — e nenhum ponto do pipeline usa `es_score` para decidir, filtrar ou rankear pares (`data_transformation_adapter.py::filter_matches_by_threshold` usa exclusivamente `match_score`). Ou seja, `filter` isolado não isola a regra do `match_score`; para restringir candidatos sem que o campo participe do score, `indexed_dataset_filter` é a ferramenta correta, não `ComparisonRule`.

## Referências

- `src/cidacsrl/config/models/indexed_dataset_filter.py`
- `src/cidacsrl/domain/linkage/linkage_specification.py` (`SequentialLinkageSpecification.build_blocking_phase_context`)
- `src/cidacsrl/domain/linkage/matching_rules.py` (`BlockingPhase.indexed_dataset_filter`)
- `src/cidacsrl/adapters/outbound/elasticsearch/query_builder.py` (`ElasticsearchQueryBuilder._build_filters`)
- `src/cidacsrl/domain/linkage/scoring_engine.py` (toda `ComparisonRule` participa do score, independentemente do `es_clause_type`)
- `src/cidacsrl/adapters/outbound/spark/data_transformation_adapter.py` (`filter_matches_by_threshold` decide pares só por `match_score`, nunca por `es_score`)
- `src/cidacsrl/bootstrap/linkage_bootstrap.py` (`_run_preflight_validations` → `get_required_target_columns` é o gatilho real da validação do filtro, no preflight)
- `tests/unit/config/test_config_models.py` (`TestIndexedDatasetFilterItemColumn`), `tests/unit/adapters/outbound/elasticsearch/test_query_builder.py`
- `docs/user-guide/linkage.md` (seção "`indexed_dataset_filter`: restringindo o universo de candidatos no ES")
- Commits de origem: `3eed895` (feat: add new DTO IndexedDatasetFilterItem), `c256112` (feat: add parsing with indexed_dataset_filter), `fe6d7cb` (feat: add support to static filters)
