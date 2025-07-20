# Workflow de Indexação no Elasticsearch

Este módulo contém o fluxo de trabalho principal para ler dados de uma fonte Parquet e indexá-los em um índice do Elasticsearch. Ele é projetado para ser executado como um script de linha de comando, orquestrando o carregamento de configurações, a inicialização do Spark e a ingestão de dados.

## Função Principal

A função `main` é o ponto de entrada do script e contém toda a lógica de orquestração. A documentação abaixo detalha seus argumentos de linha de comando e um exemplo de uso.

::: cidacsrl_rlp.src.workflows.elasticsearch_indexing_workflow.main