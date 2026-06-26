# CIDACS-RL

[![Python Version](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Version](https://img.shields.io/badge/version-3.1.0a1-orange.svg)](https://github.com/rafael-romao/cidacsrl)
[![code style: blue](https://img.shields.io/badge/code%20style-blue-blue.svg)](https://github.com/grantjenks/blue)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Engine de **linkage probabilístico de registros** para conectar bases de dados que se referem à mesma entidade — por exemplo, um mesmo paciente em sistemas de saúde distintos — mesmo sem identificador único universal.

Projetada para escala: usa **Apache Spark** para processamento distribuído e **Elasticsearch** como motor de blocagem, operando sobre dezenas de milhões de registros com controle fino de performance e rastreabilidade.

## Como Funciona

O pipeline é composto por quatro etapas. As três últimas são orquestradas pela CLI `cidacsrl`:

```
Dados Brutos (CSV / Parquet)
         │
         ▼
  [ Limpeza e Harmonização ]   ← pré-requisito externo (script / notebook)
         │
         ▼
  Dados Limpos (Parquet)
         │
         ├──────────────────────────────────────────┐
         ▼                                          ▼
  cidacsrl indexing                         cidacsrl linkage
  (indexa a base alvo                       (consulta o índice,
   no Elasticsearch)                         compara e pontua pares)
                                                     │
                                                     ▼
                                          Pares Linkados (Parquet)
                                                     │
                                                     ▼
                                          cidacsrl deduplication
                                          (resolve cadeias de pares
                                           em grupos únicos)
```

## Pré-requisitos

- Python 3.12+
- Java 11+ (requerido pelo Apache Spark)
- Elasticsearch 9+
- [Poetry](https://python-poetry.org/) (para desenvolvimento)

## Instalação

```bash
git clone https://github.com/rafael-romao/cidacsrl.git
cd cidacsrl
poetry install
```

Para instalar apenas as dependências de produção (sem ferramentas de dev):

```bash
poetry install --only main
```

## Uso Rápido

Cada etapa do pipeline é um subcomando da CLI `cidacsrl`:

```bash
# 1. Indexar a base alvo no Elasticsearch
cidacsrl indexing --env-config configs/env.yaml --spec-config configs/indexing_spec.yaml

# 2. Executar o linkage probabilístico
cidacsrl linkage --env-config configs/env.yaml --spec-config configs/linkage_spec.yaml

# 3. Deduplicar os pares linkados em grupos únicos
cidacsrl deduplication --config-path configs/deduplication_config.yaml
```

O flag `--log-level` é global e aceita `DEBUG`, `INFO` (padrão), `WARNING`, `ERROR`, `CRITICAL`:

```bash
cidacsrl --log-level DEBUG linkage --env-config configs/env.yaml
```

## Documentação

A documentação completa inclui guias de uso passo a passo, referência de configuração e diagramas de arquitetura. Para visualizá-la localmente:

```bash
poetry run mkdocs serve
```

Acesse `http://127.0.0.1:8000` no navegador.

Guias disponíveis:
- [Limpeza de Dados](./docs/user-guide/cleaning.md)
- [Indexação no Elasticsearch](./docs/user-guide/elasticsearch_indexing.md)
- [Linkage de Dados](./docs/user-guide/linkage.md)
- [Deduplicação](./docs/user-guide/deduplicate.md)
- [Visão Geral da Arquitetura](./docs/architecture/overview.md)

## Arquitetura

O projeto segue o padrão **Hexagonal (Ports & Adapters)** com três verticais independentes: Linkage, Indexing e Deduplication. Cada vertical possui seu próprio conjunto de ports (interfaces) e adapters (implementações), orquestrado por um use case na camada de aplicação. A injeção de dependências é feita manualmente via camada de Bootstrap.

## Contribuição

1. Faça um fork do projeto.
2. Crie uma nova branch (`git checkout -b feature/nova-feature`).
3. Faça commit de suas alterações (`git commit -m 'feat: descrição'`).
4. Envie para a branch (`git push origin feature/nova-feature`).
5. Abra um Pull Request.

## Licença

Este projeto está licenciado sob a [Licença MIT](./LICENSE).
