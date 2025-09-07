# Guia de Uso: Indexação no Elasticsearch

O processo de busca e comparação do CIDACS-RL ocorre a partir da requisição a índices Elasticsearch. O módulo **elasticsearch_indexing** é responsável por carregar  dados no motor de busca.

## Visão Geral do Processo

O módulo **elasticsearch_indexing** executa as seguintes etapas:

1.  **Leitura dos dados**: Carrega um conjunto de dados de entrada no formato Parquet.
2.  **Criação do índice**: Conecta-se ao Elasticsearch e cria um novo índice (semelhante a uma tabela em um banco de dados) com a estrutura pré-definida no arquivo de configuração
3.  **Ingestão dos dados**: Transfere os registros do arquivo Parquet para o índice recém-criado no Elasticsearch.

## Configurando o Índice

A estrutura do índice no Elasticsearch, também conhecida como  **mapeamento (mapping)**, é definida em um arquivo de configuração YAML. No mapeamento é especificado o tipo de cada campo (ex: texto, data, número) e outras configurações de análise que otimizem as buscas.


### Exemplo de Configuração de Índice

Abaixo, um exemplo de como definir regras para indexar colunas de nome, nome da mãe, município e UF:

```yaml
# Exemplo de arquivo index_config.yaml
index_name: "pacientes"
source_table: "cleaned_pacientes"
columns:
  - name: nome_completo
    type: string
    index_as: text
    index: true
  - name: nome_da_mae
    type: string
    index_as: text
    index: true
  - name: municipio_nascimento
    type: integer
    index: true
  - name: uf_nascimento
    type: integer
    index: true
```

## Como Executar

A execução do módulo **elasticsearch_indexing** é configurável a partir de um arquivo YAML. A exemplo:

```yaml
# Exemplo de arquivo cleaning_config.yaml
spark_config_path: "/path/to/spark_config.yaml"
columns_config_path: "/path/to/columns_config.yaml"
source_data_path: "/path/to/raw_data.parquet"
log_level: "INFO"
```

Para executar o módulo **elasticsearch_indexing**, é necessário fornecer o caminho para o arquivo de configuração.

```bash
python -m cidacsrl_rlp.src.workflows.elasticsearch_indexing_workflow --config-path /path/to/elasticsearch_indexing_config.yaml
```


Para obter instruções detalhadas sobre os comandos e todos os argumentos disponíveis, consulte a [Referência Técnica do Workflow de Indexação](../reference/elasticsearch_indexing_workflow.md).

## Próximos Passos

Com os dados devidamente indexados no Elasticsearch, é possível seguir para o [linkage](./linkage.md) entre uma fonte e o novo índice.