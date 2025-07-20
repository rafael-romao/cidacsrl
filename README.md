# CIDACSRL Record Linkage Platform (CIDACSRL-RLP)

[![Python Version](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Bem-vindo à **Plataforma de Linkage de Registros do CIDACS (CIDACSRL-RLP)**. Este projeto fornece um conjunto de ferramentas robusto, flexível e escalável para executar processos de linkage de registros probabilístico em grandes bases de dados, utilizando PySpark e Elasticsearch.

O objetivo é permitir que pesquisadores e analistas de dados conectem registros de diferentes fontes que se referem à mesma entidade (por exemplo, um paciente), mesmo na ausência de um identificador único e universal.

## Principais Etapas do Processo

A plataforma divide o complexo processo de linkage em três etapas principais, cada uma com seu próprio workflow configurável:

1.  **[Limpeza de Dados](./docs/user-guide/cleaning.md)**: A etapa inicial e fundamental, onde os dados brutos são padronizados, limpos e transformados em um formato consistente e de alta qualidade.
2.  **[Indexação no Elasticsearch](./docs/user-guide/elasticsearch_indexing.md)**: Os dados limpos são carregados em um motor de busca (Elasticsearch) para permitir consultas rápidas e eficientes, que são a base para a etapa de comparação.
3.  **[Linkage de Dados](./docs/user-guide/linkage.md)**: Utilizando uma estratégia de blocagem sequencial, esta etapa compara os registros de forma inteligente para encontrar pares potenciais e calcular scores de similaridade.

## Instalação

Para configurar o ambiente de desenvolvimento, você precisará do [Poetry](https://python-poetry.org/).

1.  Clone o repositório:
    ```bash
    git clone https://github.com/rafael-romao/cidacsrl-rlp.git
    cd cidacsrl-rlp
    ```

2.  Instale as dependências do projeto:
    ```bash
    poetry install
    ```

## Como Usar

Cada etapa do processo (limpeza, indexação, linkage) é executada como um workflow a partir da linha de comando. A lógica de cada workflow é controlada por arquivos de configuração YAML, permitindo alta customização sem alterar o código-fonte.

### Exemplo de Estrutura de Comando (Hipotético)

```bash
# Executar o workflow de limpeza
poetry run python -m cidacsrl_rlp.workflows.cleaning --data-path /path/to/data.csv --config-path /path/to/cleaning_config.yaml

# Executar o workflow de indexação
poetry run python -m cidacsrl_rlp.workflows.indexing --data-path /path/to/cleaned_data.parquet --config-path /path/to/index_config.yaml

# Executar o workflow de linkage
poetry run python -m cidacsrl_rlp.workflows.linkage --config-path /path/to/linkage_config.yaml
```

Para instruções detalhadas, consulte os **[Guias de Uso](./docs/user-guide/cleaning.md)**.

## Documentação

A documentação completa do projeto, incluindo guias de uso e referência técnica da API, está disponível. Para visualizá-la localmente, execute:

```bash
poetry run mkdocs serve
```

Em seguida, acesse `http://127.0.0.1:8000` no seu navegador.

## Contribuição

Contribuições são bem-vindas! Se você deseja contribuir, por favor:

1.  Faça um fork do projeto.
2.  Crie uma nova branch (`git checkout -b feature/nova-feature`).
3.  Faça commit de suas alterações (`git commit -m 'Adiciona nova feature'`).
4.  Envie para a branch (`git push origin feature/nova-feature`).
5.  Abra um Pull Request.

## Licença

Este projeto está licenciado sob a Licença MIT. Veja o arquivo `LICENSE` para