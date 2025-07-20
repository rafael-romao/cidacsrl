# Guia de Uso: Limpeza de Dados

A primeira e mais fundamental etapa no processo de linkage de registros é a **limpeza e padronização dos dados**. Dados brutos, provenientes de diferentes fontes, quase sempre contêm erros, inconsistências, formatações variadas e valores ausentes.

O **Workflow de Limpeza** foi projetado para tratar esses problemas, transformando os dados brutos em um formato consistente e de alta qualidade, essencial para que as etapas seguintes de indexação e comparação funcionem de maneira precisa e eficiente.

## Visão Geral do Processo

O workflow de limpeza automatiza as seguintes tarefas:

1.  **Leitura dos Dados Brutos**: Carrega um conjunto de dados de entrada (por exemplo, um arquivo CSV ou Parquet).
2.  **Aplicação de Regras de Limpeza**: Executa uma série de transformações pré-definidas em colunas específicas. Isso pode incluir a remoção de acentos, a conversão de texto para maiúsculas, a padronização de datas e a extração de componentes de nomes.
3.  **Seleção e Renomeação**: Mantém apenas as colunas necessárias para o linkage e as renomeia para nomes padronizados.
4.  **Salvamento dos Dados Limpos**: Grava o resultado em um novo arquivo (geralmente no formato Parquet), pronto para ser usado na próxima etapa.

## Configurando as Regras de Limpeza

Todas as regras de limpeza são definidas em um arquivo de configuração YAML. Isso permite que você personalize o processo para as características específicas do seu conjunto de dados sem precisar alterar o código.

### Exemplo de Configuração de Limpeza

Abaixo, um exemplo de como definir regras para limpar colunas de nome, nome da mãe e data de nascimento:

```yaml
# Exemplo de arquivo cleaning_config.yaml
columns:
  - input_name: "NOME_PACIENTE"
    output_name: "nome_completo"
    operations:
      - "remove_special_characters"
      - "to_uppercase"
      - "trim"
  - input_name: "NOME_MAE"
    output_name: "nome_da_mae"
    operations:
      - "remove_special_characters"
      - "to_uppercase"
      - "trim"
  - input_name: "DATA_NASC"
    output_name: "data_nasc"
    operations:
      - "parse_date:['dd/MM/yyyy', 'yyyy-MM-dd']"
```

## Como Executar

O workflow de limpeza é executado através de um script de linha de comando. Você precisa fornecer o caminho para os seus dados brutos e para o arquivo de configuração de limpeza.

Para obter instruções detalhadas sobre os comandos e todos os argumentos disponíveis, consulte a [Referência Técnica do Workflow de Limpeza](../reference/cleaning_workflow.md).

## Próximos Passos

Com os dados devidamente limpos e padronizados, o próximo passo é prepará-los para a busca em larga escala. Prossiga para a [Indexação no Elasticsearch](./elasticsearch_indexing.md).