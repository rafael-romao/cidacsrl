# Guia de Uso: Indexação no Elasticsearch

Após a [limpeza dos dados](./cleaning.md), o próximo passo é prepará-los para o processo de busca e comparação em larga escala. O **Workflow de Indexação** é responsável por carregar os dados limpos em um motor de busca especializado, o Elasticsearch.

Indexar os dados é como criar o índice de um livro: em vez de ler o livro inteiro toda vez que você procura por um termo, você consulta o índice para encontrar rapidamente onde ele aparece. Da mesma forma, o Elasticsearch organiza os dados para permitir consultas quase em tempo real, o que é fundamental para a eficiência do processo de linkage.

## Visão Geral do Processo

O workflow de indexação executa as seguintes etapas:

1.  **Leitura dos Dados Limpos**: Carrega o conjunto de dados já tratado (geralmente um arquivo Parquet).
2.  **Criação do Índice**: Conecta-se ao Elasticsearch e cria um novo "índice" (semelhante a uma tabela em um banco de dados) com uma estrutura pré-definida.
3.  **Ingestão dos Dados**: Transfere os registros do arquivo Parquet para o índice recém-criado no Elasticsearch.

Uma vez que os dados estão no Elasticsearch, eles podem ser consultados de forma extremamente rápida pela etapa de linkage.

## Configurando o Índice

Para que o Elasticsearch saiba como armazenar e pesquisar seus dados de forma eficiente, você precisa definir a "estrutura" do índice. Isso é feito através de um arquivo de configuração YAML, conhecido como **mapeamento (mapping)**.

No mapeamento, você especifica o tipo de cada campo (ex: texto, data, número) e outras configurações de análise que otimizam as buscas.

### Exemplo de Configuração de Índice

Abaixo, um exemplo simplificado de como definir um índice para dados de pessoas:

```yaml
# Exemplo de arquivo index_config.yaml
index_name: "cidadaos_v1"
settings:
  number_of_shards: 1
  number_of_replicas: 0
mappings:
  properties:
    nome_tratado:
      type: "text"
    data_nascimento:
      type: "date"
      format: "yyyy-MM-dd"
    id_registro:
      type: "keyword" # Usado para identificadores exatos
```

## Como Executar

O workflow de indexação é executado através de um script de linha de comando. Você precisa fornecer os caminhos para os seus dados limpos, o arquivo de configuração do índice e as credenciais do Elasticsearch.

Para obter instruções detalhadas sobre os comandos e todos os argumentos disponíveis, consulte a [Referência Técnica do Workflow de Indexação](../reference/elasticsearch_indexing_workflow.md).

## Próximos Passos

Com os dados devidamente limpos e indexados no Elasticsearch, a plataforma está pronta para a etapa mais importante: o [Workflow de Linkage de Dados](./linkage.md).// filepath: /home/romao/workspace/cidacsrl-rlp/docs/user-guide/indexing.md
# Guia de Uso: Indexação no Elasticsearch

Após a [limpeza dos dados](./cleaning.md), o próximo passo é prepará-los para o processo de busca e comparação em larga escala. O **Workflow de Indexação** é responsável por carregar os dados limpos em um motor de busca especializado, o Elasticsearch.

Indexar os dados é como criar o índice de um livro: em vez de ler o livro inteiro toda vez que você procura por um termo, você consulta o índice para encontrar rapidamente onde ele aparece. Da mesma forma, o Elasticsearch organiza os dados para permitir consultas quase em tempo real, o que é fundamental para a eficiência do processo de linkage.

## Visão Geral do Processo

O workflow de indexação executa as seguintes etapas:

1.  **Leitura dos Dados Limpos**: Carrega o conjunto de dados já tratado (geralmente um arquivo Parquet).
2.  **Criação do Índice**: Conecta-se ao Elasticsearch e cria um novo "índice" (semelhante a uma tabela em um banco de dados) com uma estrutura pré-definida.
3.  **Ingestão dos Dados**: Transfere os registros do arquivo Parquet para o índice recém-criado no Elasticsearch.

Uma vez que os dados estão no Elasticsearch, eles podem ser consultados de forma extremamente rápida pela etapa de linkage.

## Configurando o Índice

Para que o Elasticsearch saiba como armazenar e pesquisar seus dados de forma eficiente, você precisa definir a "estrutura" do índice. Isso é feito através de um arquivo de configuração YAML, conhecido como **mapeamento (mapping)**.

No mapeamento, você especifica o tipo de cada campo (ex: texto, data, número) e outras configurações de análise que otimizam as buscas.

### Exemplo de Configuração de Índice

Abaixo, um exemplo simplificado de como definir um índice para dados de pessoas:

```yaml
# Exemplo de arquivo index_config.yaml
index_name: "cidadaos_v1"
settings:
  number_of_shards: 1
  number_of_replicas: 0
mappings:
  properties:
    nome_tratado:
      type: "text"
    data_nascimento:
      type: "date"
      format: "yyyy-MM-dd"
    id_registro:
      type: "keyword" # Usado para identificadores exatos
```

## Como Executar

O workflow de indexação é executado através de um script de linha de comando. Você precisa fornecer os caminhos para os seus dados limpos, o arquivo de configuração do índice e as credenciais do Elasticsearch.

Para obter instruções detalhadas sobre os comandos e todos os argumentos disponíveis, consulte a [Referência Técnica do Workflow de Indexação](../reference/elasticsearch_indexing_workflow.md).

## Próximos Passos

Com os dados devidamente limpos e indexados no Elasticsearch, a plataforma está pronta para a etapa mais importante: o [Workflow de Linkage de Dados](./linkage.md).