# Guia de Uso: Linkage de Dados

Esta é a etapa central de todo o processo. O **Workflow de Linkage de Dados** utiliza os dados que foram previamente [limpos](./cleaning.md) e [indexados](./indexing.md) para encontrar e comparar registros entre diferentes fontes de dados, com o objetivo de identificar quais registros se referem à mesma entidade (por exemplo, a mesma pessoa).

## O Método de Blocagem Sequencial (Sequential Blocking)

Comparar cada registro de uma fonte de dados com todos os registros de outra é computacionalmente inviável para grandes volumes de dados. Para resolver isso, utilizamos uma estratégia chamada **blocagem (blocking)**.

A ideia é simples: em vez de comparar tudo com tudo, agrupamos os registros em "blocos" com base em características comuns e só comparamos os registros dentro do mesmo bloco. Por exemplo, um bloco pode conter todos os indivíduos que nasceram no mesmo ano e cujo primeiro nome começa com a letra 'J'.

O **Workflow de Linkage Sequencial** aprimora essa ideia executando o processo em múltiplas **fases**. Cada fase usa uma chave de blocagem diferente.

*   **Fase 1**: Pode agrupar por `(primeira_letra_do_nome, ano_de_nascimento)`.
*   **Fase 2**: Pode agrupar por `(código_do_município, mês_de_nascimento)`.

Essa abordagem aumenta a chance de encontrar um par verdadeiro, mesmo que a informação usada em uma das fases esteja incorreta ou ausente.

## Configurando o Workflow de Linkage

Toda a lógica do workflow — as fases, as chaves de blocagem e como as colunas devem ser comparadas — é definida em um arquivo de configuração YAML.

### Exemplo de Configuração de Linkage

Abaixo, um exemplo simplificado de um workflow com duas fases:

```yaml
# Exemplo de arquivo linkage_config.yaml
workflow_name: "linkage_cidadaos_basico"
phases:
  - phase_name: "bloco_por_nome_e_ano"
    blocking_keys: ["primeira_letra_nome", "ano_nasc"]
    comparison_fields:
      - field_name: "nome_completo"
        metric: "jaro_winkler" # Algoritmo para comparar similaridade de strings
        threshold: 0.85
      - field_name: "nome_da_mae"
        metric: "jaro_winkler"
        threshold: 0.85

  - phase_name: "bloco_por_municipio_e_data"
    blocking_keys: ["cod_municipio", "data_nasc_completa"]
    comparison_fields:
      - field_name: "nome_completo"
        metric: "jaro_winkler"
        threshold: 0.90
```

## O Resultado do Processo

O resultado final do workflow é um conjunto de dados contendo os pares de registros identificados como potenciais correspondências. Para cada par, a plataforma calcula um **score de similaridade** que indica a força da correspondência.

Este resultado permite que pesquisadores e analistas tomem decisões informadas sobre quais pares devem ser considerados uma ligação verdadeira.

## Como Executar

O workflow é executado através de um script de linha de comando, que orquestra a execução de cada fase de forma sequencial.

Para obter instruções detalhadas sobre os comandos e todos os argumentos disponíveis, consulte a [Referência Técnica do Workflow de Linkage Sequencial](../reference/sequential_linkage_workflow.md).