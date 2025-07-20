# CIDACSRL Record Linkage Platform (CIDACSRL-RLP)

Bem-vindo à documentação da **Plataforma de Linkage de Registros do CIDACS (CIDACSRL-RLP)**. Este projeto foi desenvolvido para fornecer um conjunto de ferramentas robusto, flexível e escalável para executar processos de linkage de registros probabilístico em grandes bases de dados, uma tarefa comum e crítica em pesquisas de saúde e sociais.

O objetivo é permitir que pesquisadores e analistas de dados conectem registros de diferentes fontes que se referem à mesma entidade (por exemplo, um paciente), mesmo na ausência de um identificador único e universal.

## Principais Etapas do Processo

A plataforma divide o complexo processo de linkage em três etapas principais, cada uma com seu próprio workflow configurável:

1.  **[Limpeza de Dados](./user-guide/cleaning.md)**: A etapa inicial e fundamental, onde os dados brutos são padronizados, limpos e transformados em um formato consistente e de alta qualidade.
2.  **[Indexação no Elasticsearch](./user-guide/elasticsearch_indexing.md)**: Os dados limpos são carregados em um motor de busca (Elasticsearch) para permitir consultas rápidas e eficientes, que são a base para a etapa de comparação.
3.  **[Linkage de Dados](./user-guide/linkage.md)**: Utilizando uma estratégia de blocagem sequencial, esta etapa compara os registros de forma inteligente para encontrar pares potenciais e calcular scores de similaridade, resultando em um conjunto de dados de pares ligados.

## Como Navegar na Documentação

Esta documentação está organizada em duas seções principais para melhor atender às suas necessidades:

*   **[Guias de Uso](./user-guide/cleaning.md)**: Se você é novo na plataforma ou quer aprender a executar um workflow do início ao fim, comece aqui. Estes guias fornecem uma visão geral conceitual e instruções passo a passo para cada etapa do processo.
*   **[Referência Técnica](./reference/cleaning_workflow.md)**: Contém a documentação detalhada da API, descrições de funções, argumentos de linha de comando e exemplos de configuração para cada componente da plataforma. Ideal para usuários avançados que precisam de informações específicas ou desejam personalizar os workflows.

## Primeiros Passos

Para começar a usar a plataforma, o primeiro passo é preparar seus dados.

**➡️ Prossiga para o [Guia de Uso: Limpeza de Dados](./user-guide/cleaning.md)**.