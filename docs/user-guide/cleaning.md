# Guia de Uso: Limpeza de Dados

A limpeza de dados é um **pré-requisito externo** ao pipeline da CIDACS-RL: a CLI não expõe um subcomando de limpeza. Esta etapa é de responsabilidade do usuário e pode ser realizada com qualquer ferramenta (script PySpark, notebook, dbt, etc.).

Este guia explica o que precisa ser feito nessa etapa e como o pacote `cidacsrl` pode ajudar com utilitários de configuração e execução.

---

## Por que a limpeza afeta diretamente a qualidade do linkage

O score de similaridade calculado no linkage compara os valores dos campos **da fonte** com os valores dos campos **do índice Elasticsearch** caractere a caractere. Se os dois lados não estiverem harmonizados da mesma forma, o score vai penalizar diferenças que não representam entidades diferentes.

Exemplos concretos de como a falta de limpeza derruba o score:

| Campo | Fonte (não limpa) | Índice (limpo) | Score Jaro-Winkler |
|---|---|---|---|
| nome | `Maria José Silva` | `MARIA JOSE SILVA` | ~0.92 (penaliza caixa e acento) |
| nome | `MARIA JOSE SILVA` | `MARIA JOSE SILVA` | 1.0 |
| município | `123456-7` | `123456` | 0.0 (exact) |
| município | `123456` | `123456` | 1.0 |

**A regra prática:** aplique as mesmas transformações nos dois lados — na base que será indexada e na base que será usada como fonte no linkage.

---

## O que fazer nesta etapa

1. **Remover acentos e caracteres especiais** de campos de texto usados nas regras de comparação
2. **Padronizar caixa** (recomendado: `upper`) em campos de nome
3. **Truncar ou reformatar** campos de código (ex: código IBGE de 7 para 6 dígitos)
4. **Substituir valores inválidos** por nulo (ex: `"NÃO INFORMADO"`, `"99"`, `""`)
5. **Renomear colunas** para nomes padronizados que serão usados nas regras de linkage
6. **Salvar em Parquet** — o formato esperado tanto para a indexação quanto para a fonte do linkage

---

## Utilitários disponíveis no pacote

O pacote oferece dois componentes para uso em scripts e notebooks:

- **`ColumnConfig`** — dataclass para declarar as transformações de cada coluna
- **`SparkCleaningAdapter`** — aplica as configurações declaradas a um DataFrame Spark

### Exemplo de uso

```python
from cidacsrl.domain.cleaning import ColumnConfig
from cidacsrl.adapters.outbound.spark.cleaning_adapter import SparkCleaningAdapter

configs = [
    ColumnConfig(
        name="NOME_PACIENTE",
        cleaned_name="nome_completo",
        trim_whitespace=True,
        normalize_chars=True,
        standardize_case="upper",
    ),
    ColumnConfig(
        name="NOME_MAE",
        cleaned_name="nome_da_mae",
        trim_whitespace=True,
        normalize_chars=True,
        standardize_case="upper",
    ),
    ColumnConfig(
        name="ID_IBGE_MUNICIPIO",
        cleaned_name="municipio_nascimento",
        truncate_length=6,
        invalid_values=["NÃO INFORMADO", "9999999"],
    ),
    ColumnConfig(
        name="SIGLA_IBGE_UF",
        cleaned_name="uf_nascimento",
        truncate_length=2,
        invalid_values=["99"],
        replace_empty_with_null=True,
    ),
]

df_limpo = SparkCleaningAdapter().apply(df, configs)
df_limpo.write.parquet("hdfs://.../<dataset>_limpo")
```

---

## Referência: `ColumnConfig`

| Campo | Tipo | Descrição |
|---|---|---|
| `name` | `str` | Nome original da coluna no DataFrame |
| `cleaned_name` | `str` (opcional) | Nome da coluna no output; usa `name` se omitido |
| `invalid_values` | `List[str]` | Valores a serem substituídos por nulo (ex: `["99", "NÃO INFORMADO"]`) |
| `replace_empty_with_null` | `bool` | Substitui strings vazias (`""`) por nulo |
| `trim_whitespace` | `bool` | Remove espaços em branco no início e fim da string |
| `normalize_chars` | `bool` | Remove acentos e diacríticos (ex: `"ção"` → `"cao"`) |
| `chars_to_remove` | `str` (opcional) | String com os caracteres a remover (ex: `".-/"` remove pontos, hífens e barras) |
| `standardize_case` | `str` (opcional) | `"upper"`, `"lower"` ou `"title"` |
| `truncate_length` | `int` (opcional) | Comprimento máximo da string |
| `cast_to` | `str` (opcional) | Tipo Spark destino (ex: `"integer"`) |

### Ordem de aplicação

As transformações são aplicadas nesta ordem, independentemente da ordem em que os campos são declarados:

1. `trim_whitespace`
2. `invalid_values`
3. `replace_empty_with_null`
4. `chars_to_remove`
5. `normalize_chars`
6. `standardize_case`
7. `truncate_length`
8. `cast_to`
9. Renomeação (`cleaned_name`)

---

## Próximos passos

Com os dados limpos e salvos em Parquet, siga para a indexação da base alvo:

**➡️ [Indexação no Elasticsearch](./elasticsearch_indexing.md)**
