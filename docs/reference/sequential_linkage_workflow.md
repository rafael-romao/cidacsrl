# Workflow de Linkage Sequencial

Este módulo contém o fluxo de trabalho principal para executar um processo de linkage de dados em múltiplas fases ("Sequential Blocking"). Ele orquestra a leitura de dados, a execução de cada fase de bloqueio contra o Elasticsearch e o salvamento dos resultados.

## Função Principal (Ponto de Entrada)

A função `main` é o ponto de entrada do script quando executado pela linha de comando. Ela gerencia o carregamento de configurações, a inicialização do Spark e a orquestração das fases de linkage. A documentação abaixo detalha seus argumentos e um exemplo de uso.

::: cidacsrl_rlp.src.workflows.sequential_linkage_workflow.main

## Execução de Fase de Linkage

Esta função encapsula a lógica para executar uma única fase de bloqueio (blocking phase). Ela processa um subconjunto de dados da fonte, consulta o Elasticsearch para encontrar candidatos e calcula os scores de similaridade.

::: cidacsrl_rlp.src.workflows.sequential_linkage_workflow.execute_linkage_phase

## Funções Utilitárias

Funções de suporte utilizadas dentro do workflow.

### Sanitização de Nomes de Arquivo

Esta função garante que os nomes de arquivos e diretórios gerados sejam seguros para o sistema de arquivos.

::: cidacsrl_rlp.src.workflows.sequential_linkage_workflow.make_safe_filename_component