# ADR 010: Layout de saída particionado uniformemente por `phase_match`

## Status

Aceito / Implementado

## Contexto

Antes desta decisão, `SparkDataPersistenceAdapter.save_phase_output` gravava o resultado de cada fase em um diretório físico próprio: `output_path/project_name/phase_name/`. O particionamento Hive por `partitionOverwriteMode=dynamic` só era aplicado quando havia uma coluna de particionamento de dados configurada (`partition_column`); sem ela, a escrita era um `overwrite` simples de todo o diretório da fase.

Esse layout fragmentava a saída de um projeto de linkage em N diretórios independentes — um por fase — sem relação Hive entre eles. Consumir o resultado completo (todas as fases) exigia unir manualmente N datasets Parquet distintos, e não havia validação de que os esquemas entre fases eram compatíveis (schemas podem variar por fase, já que cada uma pode aplicar regras/colunas de comparação diferentes).

## Decisão

A escrita passou a usar `phase_match` como **primeira chave de particionamento Hive**, sempre presente, com a coluna de particionamento de dados (`partition_column`, se configurada) como chave secundária:

```python
partition_cols = ["phase_match"]
if partition_column:
    partition_cols.append(actual_col)

writer = (
    df.write.format(self.config.output_format)
    .mode("overwrite")
    .option("partitionOverwriteMode", "dynamic")
    .partitionBy(*partition_cols)
)
writer.save(str(base_path))  # base_path = output_path/project_name
```

O caminho base deixou de incluir `phase_name` como segmento de diretório — o projeto inteiro agora é gravado em `output_path/project_name/`, como um único dataset Hive particionado por `phase_match=<fase>[/<partition_column>=<valor>]`, consultável com um único `read` filtrando por partição (ex.: `WHERE phase_match = 'fase_2'`), em vez de N leituras + união manual.

`partitionOverwriteMode=dynamic` também passou a ser aplicado incondicionalmente, mesmo sem `partition_column` configurada — cada escrita sobrescreve apenas a partição `phase_match=<fase>` correspondente, nunca o dataset inteiro do projeto.

## Consequências

**Positivas:** o resultado de um projeto de linkage passa a ser um único dataset Hive coerente, filtrável por fase e (opcionalmente) por partição de origem, sem exigir merge manual a jusante. A validação de schema entre fases foi endereçada na mesma janela de trabalho (ver commit companheiro abaixo), reduzindo o risco de leitura inconsistente entre partições.

Esse layout é também o que sustenta, na prática, a garantia de idempotência do checkpoint por *Work Unit* (não por fase) descrito na [ADR 004](adr-004-work-units.md): como cada escrita de fase sobrescreve dinamicamente apenas sua própria partição `phase_match=<fase>[/<coluna>=<valor>]`, reprocessar uma Work Unit inteira após uma falha na fase N (recomputando as fases 1..N-1 já persistidas) não duplica registros — apenas sobrescreve as mesmas partições com o resultado recalculado, assumindo que a fonte não mudou entre as tentativas e que o processamento é determinístico. Essa dependência cruzada entre ADR-004 e ADR-010 não estava registrada em nenhum dos dois documentos antes desta revisão.

**Negativas:** a garantia de sobrescrita dinâmica por partição depende de `self.config.output_format` continuar sendo um formato de arquivo suportado pelo `FileFormatWriter` do Spark (Parquet, ORC, JSON, CSV) com suporte a `partitionOverwriteMode`. `output_format` é hoje uma string livre em `StorageConfig` (`src/cidacsrl/config/models/storage_config.py:28`), sem validação de que o valor configurado é compatível com esse modo de escrita — se o adapter de persistência for um dia estendido para gravar em um sink não baseado em arquivo (ex.: JDBC, um banco), a garantia de idempotência por partição deixa de valer silenciosamente, quebrando o pressuposto usado pela ADR-004.

## Referências

- `src/cidacsrl/adapters/outbound/spark/data_persistence_adapter.py`
- `src/cidacsrl/config/models/storage_config.py`
- Commits de origem: `7c4e852` (feat: adopt uniform Hive partition structure using phase_match as first key), `3613800` (fix(e2e): update validation for Hive partitions and merge schemas across phases)
- Relacionada: [ADR 004](adr-004-work-units.md) (checkpoint por Work Unit depende deste layout para ser seguro)
