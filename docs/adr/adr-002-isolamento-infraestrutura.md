# ADR 002: Mitigação do acoplamento do Spark através do padrão de mapeamento lógico

## Status

Aceito / Implementado

## Contexto

O PySpark é o motor central de processamento para os grandes volumes de dados. Se tentássemos converter cada DataFrame do Spark em entidades puras de domínio escritas em Python puro (Data Classes, etc.) para seguir a Arquitetura Hexagonal de forma estrita, haveria uma perda catastrófica de desempenho (overhead de serialização e processamento na JVM).

## Decisão

Os DataFrames do PySpark continuam a transitar nas Portas e Casos de Uso, mas apenas como estruturas de dados lógicas ("data bags"). O acoplamento é mitigado ao mover toda a lógica de manipulação direta de funções do Spark para os Adaptadores Outbound (`src/cidacsrl/adapters/outbound/spark/`).

## Consequências

**Positivas:** Desempenho nativo e escalável do Spark é totalmente preservado. Os Casos de Uso permanecem limpos de chamadas explícitas a funções internas do Spark.

**Negativas:** A não conversão de objetos do Spark em modelos de domínio físicos exige um mapeamento estritamente abstrato e lógico de esquemas na camada de Adaptadores, demandando maior rigor no design para evitar o vazamento de tipos do Spark para o núcleo da aplicação.

## Referências

- `src/cidacsrl/ports/linkage/scoring_port.py`, `get_candidates_port.py` (assinaturas que recebem/retornam DataFrame como "data bag", sem depender de tipos internos do Spark além do próprio DataFrame)
- `src/cidacsrl/adapters/outbound/spark/` (toda a manipulação concreta de funções/colunas do Spark vive aqui, não nos Ports ou Use Cases)
- Relacionada: [ADR 001](adr-001-arquitetura-hexagonal.md) (esta ADR documenta uma exceção deliberada à pureza de domínio que a ADR-001 estabelece)

