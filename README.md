# CIDACS-RL 3

[![Python Version](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![code style: blue](https://img.shields.io/badge/code%20style-blue-blue.svg)](https://github.com/grantjenks/blue)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


O projeto **CIDACS-RL 3**  apresenta uma nova versão para a reconhecida ferramenta de integração de grandes bases de dados.
Esta implementação adiciona um conjunto de ferramentas para que pesquisadores, engenheiros de dados ou demais interessados automatizem as etapas do processo de linkage com CIDACS-RL.

## Principais Etapas do Processo

A plataforma CIDACS-RL 3 oferece fluxo automatizado para quatro etapas de integração de dados com CIDACS-RL:

1.  **[Limpeza e padronização de dados](./docs/user-guide/cleaning.md)**
2.  **[Indexação no Elasticsearch](./docs/user-guide/elasticsearch_indexing.md)**
3.  **[Linkage de Dados](./docs/user-guide/linkage.md)**
4.  **[Deduplicação de Pares](./docs/user-guide/deduplicate.md)**

## Instalação

1. **Clone o repositório**:
```bash
git clone https://github.com/rafael-romao/cidacsrl-rlp.git
cd cidacsrl-rlp
```

2. **(Opcional, mas recomendado) Crie e ative um ambiente virtual**:
```bash
python -m venv .venv
source .venv/bin/activate
```
3. **Instale a ferramenta de build**:
```bash
pip install build
```

Caso o ambiente de execução tenha os requisitos do projeto instalados, você pode pular esta etapa.

4. **Build o pacote e suas dependências**: Execute o make abaixo na raiz do projeto. Isso criará um pacote `wheel` do projeto, além de baixar o `wheels` de todas as suas dependências para um único diretório.
```bash
make build
```

## Executando fluxos com `spark-submit`

Para executar a biblioteca em um job Spark, é necessário enviar os pacotes gerados no passo anterior (spark_packages) para os executores do cluster usando a flag `--py-files`.

1. **Prepare a lista de arquivos**: O comando a seguir cria uma lista separa por vírgulas de todos os pacotes no diretório `spark_packages`.

```bash
PY_FILES=$(ls spark_packages/*.whl | tr '\n' ',')
```

2. **Execute o fluxo desejado com `spark-submit` incluindo a variável `PY_FILES`**:

```bash
spark-submit \
  --master set_your_master \
  --deploy-mode set_your_deploy_mode \
  --py-files "$PY_FILES" \
  cidacsrl_rlp/src/workflows/sequential_linkage_workflow.py \
  --config-path /caminho/completo/no/cluster/para/linkage_config_workflow.yaml
```

Importante: O caminho para o arquivo de configuração deve ser acessível por todos os nós do cluster.

2.1 **Fluxo de deduplicação**

O fluxo de deduplicação tem o pacote `graphframes` como dependência. Como não é possível incluir pacotes adicionais na flag `--py-files`, é necessário usar a flag `--packages` do Spark para baixar o pacote diretamente do repositório Maven ou `--jars` com o caminho para o arquivo JAR.

```bash
GRAPHFRAMES_VERSION="0.8.2"
```


```bash
spark-submit \
  --master set_your_master \
  --deploy-mode set_your_deploy_mode \
  --packages "graphframes:graphframes:${GRAPHFRAMES_VERSION}" \
  --py-files "$PY_FILES" \
  cidacsrl_rlp/src/workflows/deduplicate_workflow.py \
  --config-path /caminho/completo/no/cluster/para/configs/deduplicate_config.yaml
```

## Executando fluxos com PySpark em modo local
Como todas as dependências instaladas localmente, você pode executar os fluxos diretamente com o Python.

Para executar os fluxos em modo local, adicione a raiz do projeto ao PYTHONPATH:
```bash
export PYTHONPATH=$(pwd):$PYTHONPATH
```
Agora é possível executar o módulo desejado:
```bash
python3 -m cidacsrl_rlp.src.workflows.deduplicate_workflow --config-path ...
```

## Exemplos de Uso

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
3.  Faça commit de suas alterações (`git commit -m 'feat: adiciona nova feature'`).
4.  Envie para a branch (`git push origin feature/nova-feature`).
5.  Abra um Pull Request.

## Licença

Este projeto está licenciado sob a Licença MIT.