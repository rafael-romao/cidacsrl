# ADR 008: Salvamento incremental por fase, com layout particionado por `phase_match`

## Status

Aceito / Implementado

## Contexto

O pipeline de linkage é multifases: dentro de cada *Work Unit*, as fases rodam em sequência e cada fase produz um conjunto de pares casados. O caso de uso (`RecordLinkageUseCase.execute`) persiste o resultado de **cada fase imediatamente** e carrega adiante apenas o resíduo — os registros ainda não pareados, obtidos por um `left-anti join` (`exclude_records`). O acumulado de pares casados de todas as fases **nunca precisa ser materializado em memória**: assim que a fase termina, seus pares são escritos em disco e descartados (`matched_pairs.unpersist()` no fim do escopo), e só o `df_remaining` (que tende a encolher a cada fase) segue vivo.

Faltava, porém, um layout de saída que tornasse essas escritas incrementais um resultado **coeso**. Já no CIDACS-RL 3 (`cidacsrl_rlp/src/workflows/sequential_linkage_workflow.py`), o caminho de cada fase usava uma sintaxe parecida com particionamento Hive: `output_base_path/linkage_{source}_vs_{target}/linkage_phase_name={phase_name}/`. Mas era só convenção de nome de pasta — a escrita era um `df.write.mode("overwrite").parquet(path)` comum, sem `partitionBy` nem `partitionOverwriteMode`. Cada fase virava um dataset Parquet fisicamente isolado, não uma partição de um dataset Hive real.

Isso se manteve na primeira versão do adapter hexagonal (`SparkDataPersistenceAdapter.save_phase_output`), que gravava o resultado de cada fase em `output_path/project_name/phase_name/`. O particionamento Hive por `partitionOverwriteMode=dynamic` só era aplicado quando havia uma coluna de particionamento de dados configurada (`partition_column`); sem ela, a escrita era um `overwrite` simples de todo o diretório da fase.

Em ambos os casos, a saída de um projeto de linkage ficava fragmentada em N diretórios independentes — um por fase — sem relação Hive real entre eles. Consumir o resultado completo (todas as fases) exigia unir manualmente N datasets Parquet distintos, e não havia validação de que os esquemas entre fases eram compatíveis (schemas podem variar por fase, já que cada uma pode aplicar regras/colunas de comparação diferentes).

## Decisão

A saída passou a ser um **único dataset Hive por projeto**, escrito incrementalmente — uma partição por fase, gravada no momento em que a fase termina. `phase_match` é a **primeira chave de particionamento Hive**, sempre presente, com a coluna de particionamento de dados (`partition_column`, se configurada) como chave secundária:

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

O caminho base deixou de incluir `phase_name` como segmento de diretório — o projeto inteiro é gravado em `output_path/project_name/`, como um único dataset Hive particionado por `phase_match=<fase>[/<partition_column>=<valor>]`, consultável com um único `read` filtrando por partição (ex.: `WHERE phase_match = 'fase_2'`), em vez de N leituras + união manual.

`partitionOverwriteMode=dynamic` também passou a ser aplicado incondicionalmente, mesmo sem `partition_column` configurada — cada escrita sobrescreve apenas a partição `phase_match=<fase>` correspondente, nunca o dataset inteiro do projeto. É justamente essa sobrescrita por partição que torna seguro o padrão de **persistir-e-descartar** de cada fase: o caso de uso não guarda em memória os pares já escritos, e reescrever a mesma partição num reprocessamento não gera append nem duplicação.

## Consequências

**Positivas:** o acumulado de pares casados nunca precisa viver em memória — cada fase escreve seu resultado e sai de cena, sobrando apenas o resíduo `left-anti`, que decresce ao longo do pipeline. Ainda assim, o resultado final é um único dataset Hive coerente, filtrável por fase e (opcionalmente) por partição de origem, sem exigir merge manual a jusante. A validação de schema entre fases foi endereçada na mesma janela de trabalho (ver commit companheiro abaixo), reduzindo o risco de leitura inconsistente entre partições.

Esse layout é também o que sustenta, na prática, a garantia de idempotência do checkpoint por *Work Unit* (não por fase) descrito na [ADR 003](adr-003-work-units.md): como cada escrita de fase sobrescreve dinamicamente apenas sua própria partição `phase_match=<fase>[/<coluna>=<valor>]`, reprocessar uma Work Unit inteira após uma falha na fase N (recomputando as fases 1..N-1 já persistidas) não duplica registros — apenas sobrescreve as mesmas partições com o resultado recalculado, assumindo que a fonte não mudou entre as tentativas e que o processamento é determinístico.

**Negativas:** a garantia de sobrescrita dinâmica por partição depende de `self.config.output_format` continuar sendo um formato de arquivo suportado pelo `FileFormatWriter` do Spark (Parquet, ORC, JSON, CSV) com suporte a `partitionOverwriteMode`. `output_format` é hoje uma string livre em `StorageConfig` (`src/cidacsrl/config/models/storage_config.py:28`), sem validação de que o valor configurado é compatível com esse modo de escrita — se o adapter de persistência for um dia estendido para gravar em um sink não baseado em arquivo (ex.: JDBC, um banco), tanto a garantia de idempotência por partição quanto o padrão de persistir-e-descartar por fase deixam de valer silenciosamente, quebrando o pressuposto usado pela ADR-003.

## Referências

- `src/cidacsrl/application/linkage/record_linkage_use_case.py` (persistência imediata por fase + `exclude_records` que carrega adiante apenas o resíduo)
- `src/cidacsrl/adapters/outbound/spark/data_persistence_adapter.py`
- `src/cidacsrl/config/models/storage_config.py`
- Commits de origem: `7c4e852` (feat: adopt uniform Hive partition structure using phase_match as first key), `3613800` (fix(e2e): update validation for Hive partitions and merge schemas across phases)
- Relacionada: [ADR 003](adr-003-work-units.md) (checkpoint por Work Unit depende deste layout para ser seguro)
