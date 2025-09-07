# Guia de Uso: Limpeza de Dados

O módulo **cleaning** foi projetado para tratar da etapa de limpeza de dados brutos - com tratamento de inconsistência, valores ausentes, padronização das formatações variadas -  e harmonização das variáveis.

## Visão Geral do Processo

O módulo **cleaning** segue as seguintes tarefas:

1.  **Leitura dos dados brutos**: Carrega um conjunto de dados de entrada (por exemplo, um arquivo CSV ou Parquet).
2.  **Aplicação de regras de limpeza**: Executa uma série de transformações pré-definidas em colunas específicas, que pode incluir a remoção de acentos, a padronização de campos de texto para maiúsculas, a padronização de datas e a extração de inconsistências de nomes.
3.  **Seleção e Renomeação**: Mantém apenas as colunas necessárias para o linkage e as renomeia para nomes padronizados.
4.  **Salvamento dos dados limpos**: Grava o resultado em um novo arquivo (geralmente no formato Parquet), pronto para ser usado na próxima etapa.

## Configurando as Regras de Limpeza

Todas as regras de limpeza são definidas em um arquivo de configuração YAML. Dessa forma é possível documentar e versionar o processo para as características específicas do conjunto de dados sem precisar alterar o código.

### Exemplo de Configuração de Limpeza

Abaixo, um exemplo de como definir regras para limpar colunas de nome, nome da mãe, município e UF:

```yaml
# Exemplo de arquivo database_cleaning.yaml
columns:
  - name: NOME_PACIENTE
    cleaned_name: nome_completo
    chars_to_remove: '[^\w\s]'
    standardize_case: upper
    normalize_chars: true
  - name: NOME_MAE
    cleaned_name: nome_da_mae
    chars_to_remove: '[^\w\s]'
    standardize_case: upper
    normalize_chars: true
  - name: ID_IBGE_MUNICIPIO
    cleaned_name: municipio_nascimento
    truncate_length: 6
    invalid_value: "NÃO INFORMADO"
  - name: SIGLA_IBGE_UF
    cleaned_name: uf_nascimento
    truncate_length: 2
    invalid_value: 99
```

## Como Executar

A execução do módulo **cleaning_workflow** é configurável a partir de um arquivo YAML. A exemplo:

```yaml
# Exemplo de arquivo cleaning_config.yaml
spark_config_path: "/path/to/spark_config.yaml"
columns_config_path: "/path/to/columns_config.yaml"
source_data_path: "/path/to/raw_data.parquet"
log_level: "INFO"
```

Para executar o módulo **cleaning_workflow**, é necessário fornecer o caminho para o arquivo de configuração:

```bash
python -m cidacsrl_rlp.src.workflows.cleaning_workflow --config-path /path/to/cleaning_config.yaml
```

## Próximos Passos

Com os dados devidamente limpos e padronizados, é possível seguir para a [indexação no Elasticsearch](./elasticsearch_indexing.md) ou para o [linkage de dados](./linkage.md) com uma base já indexada.