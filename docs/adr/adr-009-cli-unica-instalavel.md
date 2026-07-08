# ADR 009: Empacotamento como CLI única instalável em vez de scripts `spark-submit` individuais

## Status

Aceito / Implementado

## Contexto

No CIDACS-RL 3, cada fluxo era um script Spark independente, sem nenhum pacote instalável: `pyproject.toml` não tinha seção `[project.scripts]`. A execução exigia `spark-submit` apontando diretamente para o arquivo do workflow, com empacotamento manual de dependências via `--py-files` a partir de um diretório `spark_packages` gerado por um passo de build próprio:

```bash
spark-submit \
  --master set_your_master \
  --deploy-mode set_your_deploy_mode \
  --py-files "$PY_FILES" \
  cidacsrl_rlp/src/workflows/sequential_linkage_workflow.py \
  --config-path /caminho/completo/no/cluster/para/linkage_config_workflow.yaml
```

Cada workflow (`sequential_linkage_workflow.py`, `elasticsearch_indexing_workflow.py`, `deduplicate_workflow.py`, `cleaning_workflow.py`) tinha seu próprio `argparse` e seu próprio `if __name__ == "__main__":` — quatro pontos de entrada distintos, cada um exigindo repetir o mesmo ritual de `--py-files`/`spark_packages`.

## Decisão

O projeto passou a se distribuir como um pacote pip instalável, com um único ponto de entrada declarado em `pyproject.toml`:

```toml
[project.scripts]
cidacsrl = "cidacsrl.adapters.inbound.cli:main"
```

`cidacsrl.adapters.inbound.cli.main` cria sua própria `SparkSession` internamente (`create_spark_session`, configurada a partir do bloco `spark:` do YAML de ambiente) e expõe os três fluxos como subcomandos de uma única CLI:

```bash
poetry install
cidacsrl indexing --env-config env.yaml --spec-config indexing_spec.yaml
cidacsrl linkage --env-config env.yaml --spec-config linkage_spec.yaml
cidacsrl deduplication --config-path deduplication_config.yaml
```

Não há mais menção a `spark-submit`, `--py-files` ou `spark_packages` na documentação de uso atual.

## Consequências

**Positivas:** um único artefato distribuível (`poetry install`) em vez de quatro scripts com empacotamento manual próprio. Interface de invocação consistente entre verticais (`--log-level` global, subcomandos com suas próprias flags), em vez de quatro `argparse` divergentes. Elimina o passo de build `spark_packages` e o ritual de montar `--py-files` a cada execução.

**Negativas:** o `[project.scripts]` resolve o empacotamento e a UX de invocação, mas não substitui o orquestrador de cluster. A chamada direta `cidacsrl <subcomando>` só cobre o cenário `client` mode, em que o driver roda na própria máquina que dispara o comando (tipicamente um *edge node*). Quando é preciso rodar o driver dentro do cluster (`--deploy-mode cluster`) ou controlar alocação de recursos no nível do orquestrador (filas do YARN, namespaces do Kubernetes), a CLI passa a ser invocada **como módulo** por um `spark-submit`, e não como o console-script `cidacsrl`:

```bash
spark-submit --master yarn --deploy-mode cluster \
  --packages org.elasticsearch:elasticsearch-spark-30_2.12:9.1.8 \
  -m cidacsrl.adapters.inbound.cli \
  linkage --env-config /caminho/no/cluster/env.yaml --spec-config /caminho/no/cluster/spec.yaml
```

Ou seja, o mesmo ponto de entrada (`cidacsrl.adapters.inbound.cli:main`) serve tanto ao console-script quanto ao `-m` do `spark-submit`; o que muda é apenas quem cria o processo do driver. O detalhamento operacional (client vs. cluster mode, storage compartilhado, ambientes air-gapped) vive no guia de execução em cluster.

## Referências

- `pyproject.toml` (`[project.scripts]`)
- `src/cidacsrl/adapters/inbound/cli/main.py`
- `src/cidacsrl/adapters/outbound/spark/spark_factory.py` (`create_spark_session`)
- `README.md` (seção "Uso Rápido")
- Comparação: CIDACS-RL 3 (branch `main`) — `cidacsrl_rlp/src/workflows/*.py`, `README.md` (seção "Executando fluxos com `spark-submit`")
- Guia: [Execução em Cluster Spark](../user-guide/cluster_execution.md) (client mode vs. `spark-submit --deploy-mode cluster` via `-m cidacsrl.adapters.inbound.cli`)
- Relacionada: [ADR 002](adr-002-transicao-workflow-use-case.md) (o adapter de entrada único que este empacotamento expõe)
