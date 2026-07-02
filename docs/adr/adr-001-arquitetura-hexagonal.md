# ADR 001: Adoção de Arquitetura Hexagonal (Ports and Adapters)

## Status

Aceito / Implementado

## Contexto

No CIDACS-RL 3 a base de código estava organizada por agrupamentos de funcionalidades e tecnologias diretas (como src/workflows, src/es, e src/linkage) que criava um forte acoplamento entre as regras de negócio de linkage e as bibliotecas ou frameworks externos. Isso tornava testes unitários lentos (pois frequentemente dependiam de infraestrutura real) e dificultava a evolução do produto sem causar efeitos colaterais severos.

## Decisão

Toda a árvore de diretórios e o design de software foram refatorados, adotando princípios de Clean Architecture combinados com Ports and Adapters (Arquitetura Hexagonal). O sistema foi dividido em quatro anéis centrais de regra de negócio, mais duas camadas de suporte que os conectam ao mundo exterior:

- **Domain:** regras de negócio puras, como `scoring_engine` e `similarity_functions`, apenas com Python puro.
- **Application:** orquestra os fluxos através de Casos de Uso (Use Cases) — ver [ADR 003](adr-003-transicao-workflow-use-case.md).
- **Ports:** interfaces abstratas (contratos) que ditam como a aplicação deve interagir com sistemas de I/O.
- **Adapters:** implementa as portas para o mundo exterior.
- **Bootstrap:** monta e injeta manualmente as dependências de cada Use Case — ver [ADR 008](adr-008-bootstrap-sem-container-di.md).
- **Config:** carrega e valida a configuração de execução e de storage.

## Consequências

**Positivas:** Testabilidade total isolada por meio de mocks, desacoplamento de frameworks de Big Data e facilidade em trocar componentes externos.

**Negativas:** Aumento inicial da complexidade do código e quantidade de ficheiros (exige criação explícita de classes abstratas de portas e adaptadores concretos).

## Referências

- Relacionada: [ADR 002](adr-002-isolamento-infraestrutura.md) (exceção deliberada à pureza de domínio, para DataFrames do Spark)
- Relacionada: [ADR 003](adr-003-transicao-workflow-use-case.md) (padrão usado na camada Application)
- Relacionada: [ADR 007](adr-007-reorganizacao-layer-feature.md) (a reorganização da árvore de diretórios que torna estes anéis visíveis na estrutura de pastas)
- Relacionada: [ADR 008](adr-008-bootstrap-sem-container-di.md) (o crescimento de ports/adapters resultante desta decisão é o que pressiona o wiring manual do Bootstrap)

