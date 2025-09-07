# Guia de Uso: Linkage de Dados

O módulo **sequential_linkage_workflow** utiliza uma fonte de dados (em parquet), preferencialmente [limpos e harmonizados](./cleaning.md) para buscar candidatos em um [índice](./indexing.md) e comparar seus registros com o objetivo de conceder uma pontuação (score) e, assim, identificar quais registros se referem à mesma entidade (por exemplo, a mesma pessoa).

## Visão Geral do Processo

O módulo **sequential_linkage_workflow** executa as seguintes etapas:

1.  **Carregamento de Dados e Configurações**: Lê os dados da fonte (em formato Parquet) e carrega os arquivos de configuração do workflow, do Spark e do Elasticsearch.
2.  **Amostragem (Opcional)**: Se configurado, seleciona uma fração dos dados da fonte para processamento, útil para testes e depuração.
3.  **Execução em Fases (Sequential Blocking)**: Itera através das "fases de bloqueio" definidas no arquivo de configuração.
4.  **Busca de Candidatos**: Para cada registro da fonte, busca por registros candidatos em um índice do Elasticsearch com base nas regras da fase atual.
5.  **Cálculo de Score**: Compara os registros da fonte com os candidatos encontrados e calcula um score de similaridade.
6.  **Identificação de Pares Fortes**: Filtra os pares que atingem um limiar de pontuação (`strong_match_score_threshold`), considerando-os como correspondências.
7.  **Remoção de Pares Encontrados**: Os registros da fonte que encontram uma correspondência forte em uma fase são removidos do conjunto de dados para as fases seguintes, otimizando o processo.
8.  **Salvamento dos Resultados**: Os pares encontrados em cada fase são salvos em um diretório de saída, particionados pelo nome da fase.

## Configurando o Linkage

A configuração do workflow de linkage é feita através de um arquivo YAML, onde são especificados os caminhos de entrada e saída, além de configurações do Spark.

### Exemplo de Configuração de Linkage

Abaixo, um exemplo simplificado de linkage com duas fases:

```yaml
# Exemplo de arquivo linkage_pacientes.yaml
source_table: "cleaned_pacientes"
id_source_table: "id_table"

target_es_index: "pacientes"
id_target_table: "id_table"

blocking_phases: 
  - phase_name: "exact"
    phase_description: "Fase exata"
    enabled: true
    candidate_limit: 5
    strong_match_score_threshold: 0.95

    rules: 
      - source_column: "nome_completo"
        target_column: "nome_completo"
        es_clause_type: "must"
        similarity: "jaro_winkler"
        weight: 3.0 
        penalty: 0.1 
      - source_column: "nome_da_mae"
        target_column: "nome_da_mae"
        es_clause_type: "must"
        similarity: "jaro_winkler"
        weight: 3.0 
        penalty: 0.1 
      - source_column: "municipio_nascimento"
        target_column: "municipio_nascimento"
        es_clause_type: "must"
        query_type: "term"
        similarity: "exact"
        weight: 1.0
        penalty: 0.1
      - source_column: "uf_nascimento"
        target_column: "uf_nascimento"
        es_clause_type: "must"
        query_type: "term"
        similarity: "exact"
        weight: 1.0
        penalty: 0.1
  
  - phase_name: "non_exact"
    phase_description: "Fase não exata"
    enabled: false
    candidate_limit: 500
    strong_match_score_threshold: 0.60

    rules: 
      - source_column: "nome_completo"
        target_column: "nome_completo"
        es_clause_type: "must"
        similarity: "jaro_winkler"
        weight: 3.0 
        penalty: 0.1 
      - source_column: "nome_da_mae"
        target_column: "nome_da_mae"
        es_clause_type: "should"
        similarity: "jaro_winkler"
        weight: 3.0 
        penalty: 0.1 
      - source_column: "municipio_nascimento"
        target_column: "municipio_nascimento"
        es_clause_type: "should"
        query_type: "term"
        similarity: "exact"
        weight: 1.0
        penalty: 0.1
      - source_column: "uf_nascimento"
        target_column: "uf_nascimento"
        es_clause_type: "should"
        query_type: "term"
        similarity: "exact"
        weight: 1.0
        penalty: 0.1
      
```

## Como Executar

A execução do módulo **sequential_linkage_workflow** é configurável a partir de um arquivo YAML. A exemplo:


```yaml
# Exemplo de arquivo cleaning_config.yaml
# Caminhos para os arquivos de configuração específicos
linkage_config_path: "/path/to/linkage_workflow.yaml"
es_config_path: "/path/to/elasticsearch_config.yaml"
spark_config_path: "/path/to/spark_config.yaml"
output_data_path: "/path/to/output/data"
source_data_path: "/path/to/source_data"

# Opcional
sample_fraction: 0.1  # Fração dos dados a serem processados (0.1 = 10%)
sample_seed: 42       # Semente para amostragem reproduzível
```
Para executar o módulo **sequential_linkage_workflow** é necessário fornecer o caminho para o arquivo de configuração.

```bash
python -m cidacsrl_rlp.src.workflows.sequential_linkage_workflow --config-path /path/to/sequential_linkage_config.yaml
```

## Próximos Passos

É possível submeter resultado do linkage a [deduplicação](./deduplicate.md) de registros.