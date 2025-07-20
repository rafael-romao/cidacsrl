# Carregador de Configurações (loader)

Este módulo centraliza as funções responsáveis por carregar, validar e analisar arquivos de configuração YAML. Cada função é especializada em um tipo específico de configuração, convertendo o conteúdo do arquivo em objetos Python estruturados (dataclasses) ou dicionários para uso em outras partes da aplicação.

## Carregador Genérico de YAML
Esta é a função base que lê qualquer arquivo YAML e realiza validações essenciais.

::: cidacsrl_rlp.src.config.loader.load_yaml

## Carregador de Configuração de Colunas
Esta função carrega as regras para limpeza e transformação de colunas.

::: cidacsrl_rlp.src.config.loader.load_column_config

## Carregador de Configuração de Índice Elasticsearch
Esta função carrega a definição de um índice Elasticsearch, incluindo mapeamentos e configurações.

::: cidacsrl_rlp.src.config.loader.load_index_config

## Carregador de Configuração de Workflow de Linkage
Esta função carrega a configuração para um workflow de linkage de dados sequencial.

::: cidacsrl_rlp.src.config.loader.load_sequential_blocking_workflow_config

## Carregador de Configuração de Serviço
Esta função carrega configurações genéricas para serviços como Spark.

::: cidacsrl_rlp.src.config.loader.load_service_config