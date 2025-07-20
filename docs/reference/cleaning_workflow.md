# Workflow de Limpeza de Dados

Este módulo contém o fluxo de trabalho principal para a limpeza de dados. Ele é projetado para ser executado como um script de linha de comando, orquestrando o carregamento de configurações, a inicialização do Spark, a aplicação de um pipeline de limpeza e o salvamento dos dados resultantes.

## Função Principal

A função `main` é o ponto de entrada do script. A documentação abaixo detalha seus argumentos de linha de comando e fornece um exemplo de como executá-lo.

::: cidacsrl_rlp.src.workflows.cleaning_workflow.main