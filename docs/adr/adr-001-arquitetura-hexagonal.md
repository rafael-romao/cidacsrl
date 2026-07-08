# ADR 001: Adoção de Arquitetura Hexagonal (Ports and Adapters)

## Status

Aceito / Implementado

## Contexto

No CIDACS-RL 3 a base de código estava organizada por agrupamentos de funcionalidades e tecnologias diretas (como src/workflows, src/es, e src/linkage) que criava um forte acoplamento entre as regras de negócio de linkage e as bibliotecas ou frameworks externos. Isso tornava testes unitários lentos (pois frequentemente dependiam de infraestrutura real) e dificultava a evolução do produto sem causar efeitos colaterais severos.

## Decisão

Toda a árvore de diretórios e o design de software foram refatorados, adotando princípios de Clean Architecture combinados com Ports and Adapters (Arquitetura Hexagonal). O sistema foi dividido em quatro anéis centrais de regra de negócio, mais duas camadas de suporte que os conectam ao mundo exterior:

- **Domain:** regras de negócio puras, como `scoring_engine` e `similarity_functions`, apenas com Python puro.
- **Application:** orquestra os fluxos através de Casos de Uso (Use Cases) — ver [ADR 002](adr-002-transicao-workflow-use-case.md).
- **Ports:** interfaces abstratas (contratos) que ditam como a aplicação deve interagir com sistemas de I/O.
- **Adapters:** implementa as portas para o mundo exterior.
- **Bootstrap:** monta e injeta manualmente as dependências de cada Use Case — ver [ADR 007](adr-007-bootstrap-sem-container-di.md).
- **Config:** carrega e valida a configuração de execução e de storage.

### Exceção deliberada: DataFrames do Spark como *data bags*

O PySpark é o motor de processamento dos grandes volumes de dados. Converter cada DataFrame em entidades puras de domínio (Data Classes) para satisfazer a Arquitetura Hexagonal de forma estrita traria perda catastrófica de desempenho (overhead de serialização e de processamento na JVM). Por isso os DataFrames do PySpark continuam a transitar pelas Portas e Casos de Uso, mas apenas como estruturas de dados lógicas (*data bags*): toda a lógica de manipulação direta de funções do Spark é mantida nos Adaptadores Outbound (`src/cidacsrl/adapters/outbound/spark/`), e o núcleo permanece livre de chamadas explícitas a funções internas do Spark.

## Consequências

**Positivas:** Testabilidade total isolada por meio de mocks, desacoplamento de frameworks de Big Data e facilidade em trocar componentes externos. O desempenho nativo e escalável do Spark é integralmente preservado, já que os DataFrames não são convertidos em modelos físicos de domínio (ver exceção dos *data bags* acima).

**Negativas:** Aumento inicial da complexidade do código e quantidade de ficheiros (exige criação explícita de classes abstratas de portas e adaptadores concretos). Além disso, tratar os DataFrames como *data bags* — sem convertê-los em modelos físicos de domínio — exige um mapeamento estritamente lógico de esquemas na camada de Adaptadores, demandando rigor no design para evitar o vazamento de tipos do Spark para o núcleo da aplicação.

## Referências

- `src/cidacsrl/ports/linkage/scoring_port.py`, `get_candidates_port.py` (assinaturas que recebem/retornam DataFrame como *data bag*, sem depender de tipos internos do Spark além do próprio DataFrame)
- `src/cidacsrl/adapters/outbound/spark/` (toda a manipulação concreta de funções/colunas do Spark vive aqui, não nos Ports ou Use Cases)
- Relacionada: [ADR 002](adr-002-transicao-workflow-use-case.md) (padrão usado na camada Application)
- Relacionada: [ADR 006](adr-006-reorganizacao-layer-feature.md) (a reorganização da árvore de diretórios que torna estes anéis visíveis na estrutura de pastas)
- Relacionada: [ADR 007](adr-007-bootstrap-sem-container-di.md) (o crescimento de ports/adapters resultante desta decisão é o que pressiona o wiring manual do Bootstrap)

