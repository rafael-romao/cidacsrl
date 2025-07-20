# Operações de Indexação no Elasticsearch

Este módulo fornece funções de alto nível para orquestrar operações de indexação no Elasticsearch. A principal função lida com a criação de índices (se necessário) e a ingestão de dados de um DataFrame Spark.

## Orquestração da Indexação

A função a seguir é o principal ponto de entrada para criar um índice e ingerir dados.

::: cidacsrl_rlp.src.es.indexing_operations.create_es_index_and_ingest_data