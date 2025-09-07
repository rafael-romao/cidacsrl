# Guia de Uso: Deduplicação de Dados

O módulo **deduplicate_workflow** é responsável por resolver as cadeias que ligam múltiplos pares após uma operação de [linkage](./linkage.md), agrupando todos os registros que se referem à mesma entidade em um único grupo.

## Visão Geral do Processo

O processo de deduplicação utiliza uma abordagem baseada em grafos para identificar os grupos de registros. As etapas são:

1.  **Leitura dos Dados de Linkage**: Carrega o resultado do processo de linkage, que contém os pares de registros identificados.
2.  **Construção do Grafo**: Cria um grafo onde cada registro é um nó (vértice) e cada par de linkage é uma aresta que conecta dois nós.
3.  **Identificação de Componentes Conectados**: Aplica um algoritmo para encontrar todos os "componentes conectados" no grafo. Cada componente representa um grupo de registros que, direta ou indiretamente, estão ligados entre si e, portanto, são considerados a mesma entidade.
4.  **Atribuição de IDs de Grupo**: Adiciona uma nova coluna (`group_id`) ao conjunto de dados, onde cada registro recebe o identificador do componente ao qual pertence.
5.  **Salvamento do Resultado**: Grava o resultado final em formato Parquet, com os registros enriquecidos com o `group_id`.

## Configurando a Deduplicação

A configuração do workflow de deduplicação é feita através de um arquivo YAML, onde são especificados os caminhos de entrada e saída, além de configurações do Spark.

### Exemplo de Configuração

```yaml
# Exemplo de arquivo deduplication_config.yaml
spark_config_path: "/path/to/spark_config.yaml"
source_data_path: "/path/to/linked_data.parquet" 
output_data_path: "/path/to/deduplicated_data"
log_level: "INFO"
app_name: "DeduplicationApp"
```

## Como Executar

Para executar o workflow de deduplicação, basta chamar o script `deduplicate_workflow.py` e fornecer o caminho para o arquivo de configuração.

```bash
python cidacsrl_rlp/src/workflows/deduplicate_workflow.py --config-path /path/to/deduplication_config.yaml
```
