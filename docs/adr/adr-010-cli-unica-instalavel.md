# ADR 010: Empacotamento como CLI única instalável em vez de scripts `spark-submit` individuais

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

**Negativas:** em deploys de cluster reais (YARN/K8s), ainda pode ser necessário envolver o comando `cidacsrl` em um `spark-submit` ou equivalente para controlar `deploy-mode`/alocação de recursos no nível do cluster — a CLI resolve o empacotamento e a UX de invocação local, não substitui o orquestrador de cluster onde ele for exigido pela infraestrutura de destino.

## Referências

- `pyproject.toml` (`[project.scripts]`)
- `src/cidacsrl/adapters/inbound/cli/main.py`
- `src/cidacsrl/adapters/outbound/spark/spark_factory.py` (`create_spark_session`)
- `README.md` (seção "Uso Rápido")
- Comparação: CIDACS-RL 3 (branch `main`) — `cidacsrl_rlp/src/workflows/*.py`, `README.md` (seção "Executando fluxos com `spark-submit`")
- Relacionada: [ADR 003](adr-003-transicao-workflow-use-case.md) (o adapter de entrada único que este empacotamento expõe)
