# Configurações de Executáveis e Variáveis
PYTHON     := python
COMPOSE    := docker compose -f cidacsrl_rlp/tests/enviroment/docker-compose.yml
SPARK_PKG  := spark_packages

.PHONY: all build clean help env-check clean-docker prepare-dirs stop-all stop
.PHONY: up up-es up-ui up-jupyter down restart ps logs logs-engine logs-es logs-cerebro logs-jupyter shell-engine shell-es shell-jupyter
.PHONY: test test-integration test-unit run-e2e-pipeline run-linkage-pipeline

all: help

# ─── 1. GERENCIAMENTO DO LABORATÓRIO E2E (DOCKER COMPOSE) ───────────────────────

up: env-check
	@echo "--> Subindo o ecossistema base de laboratório (Elasticsearch + Engine)..."
	$(COMPOSE) --profile elasticsearch --profile runner up -d --remove-orphans
	@echo "Aguardando Elasticsearch ficar saudável..."
	@timeout=60; \
	while ! docker inspect --format='{{.State.Health.Status}}' cidacsrl_elasticsearch 2>/dev/null | grep -q healthy; do \
		sleep 2; \
		timeout=$$((timeout-2)); \
		if [ $$timeout -le 0 ]; then \
			echo "Elasticsearch não ficou saudável a tempo!"; \
			exit 1; \
		fi; \
	done
	@echo "\n✅ Laboratório Core pronto!"
	@echo "   - Elasticsearch : http://localhost:9200"

up-es: env-check
	@echo "--> Subindo apenas o serviço Elasticsearch..."
	$(COMPOSE) --profile elasticsearch up -d --remove-orphans
	@echo "Aguardando Elasticsearch ficar saudável..."
	@timeout=60; \
	while ! docker inspect --format='{{.State.Health.Status}}' cidacsrl_elasticsearch 2>/dev/null | grep -q healthy; do \
		sleep 2; \
		timeout=$$((timeout-2)); \
		if [ $$timeout -le 0 ]; then \
			echo "Elasticsearch não ficou saudável a tempo!"; \
			exit 1; \
		fi; \
	done
	@echo "\n✅ Elasticsearch pronto!"
	@echo "   - Elasticsearch : http://localhost:9200"

prepare-dirs:
	@echo "--> Garantindo existência e permissões das pastas necessárias..."
	mkdir -p ./tests/enviroment/.es_data ./tests/enviroment/logs/ ./tests/data/output
	chmod -R 777 ./tests/enviroment/.es_data ./tests/enviroment/logs/ ./tests/data/

up-ui: env-check
	@echo "--> Subindo laboratório completo com painéis de monitoramento (Kibana + Cerebro)..."
	$(COMPOSE) --profile elasticsearch --profile runner --profile kibana --profile cerebro up -d --remove-orphans
	@echo "\n✅ Laboratório Analítico pronto!"
	@echo "   - Elasticsearch : http://localhost:9200"
	@echo "   - Kibana        : http://localhost:5601"
	@echo "   - Cerebro       : http://localhost:9000"

up-jupyter: env-check
	@echo "--> Subindo o ecossistema completo incluindo o ambiente Jupyter Dev..."
	$(COMPOSE) --profile jupyter up -d --remove-orphans
	@echo "\n✅ Ambiente Jupyter pronto!"
	@echo "   - Jupyter       : http://localhost:8888"
	@echo "   - Elasticsearch : http://localhost:9200"
	@echo "   - Cerebro       : http://localhost:9000"

down:
	@echo "--> Derrubando os contêineres do laboratório e perfis ativos..."
	$(COMPOSE) --profile elasticsearch --profile kibana --profile cerebro --profile runner --profile jupyter down

restart: down up

stop-all:
	@echo "--> Parando todos os contêineres do laboratório (sem remover)..."
	docker stop $$(docker ps -qa)

stop-test-e2e:
	@echo "--> Parando apenas o container cidacsrl_runner..."
	docker stop cidacsrl_runner || echo "Container cidacsrl_runner não está em execução."
	@echo "Container cidacsrl_runner parado."

stop-jupyter:
	@echo "--> Parando apenas o container cidacsrl_jupyter..."
	docker stop cidacsrl_jupyter || echo "Container cidacsrl_jupyter não está em execução."
	@echo "Container cidacsrl_jupyter parado."

ps:
	$(COMPOSE) --profile jupyter ps

# ─── 2. MONITORAMENTO E LOGS ──────────────────────────────────────────────────

logs:
	$(COMPOSE) --profile jupyter --profile elasticsearch --profile cerebro --profile runner logs -f

logs-engine:
	$(COMPOSE) logs -f cidacsrl_runner

logs-es:
	$(COMPOSE) logs -f elasticsearch

logs-cerebro:
	$(COMPOSE) logs -f cerebro

logs-jupyter:
	$(COMPOSE) logs -f jupyter

shell-engine:
	$(COMPOSE) exec cidacsrl_runner bash

shell-es:
	$(COMPOSE) exec elasticsearch bash

shell-jupyter:
	$(COMPOSE) exec jupyter bash

# ─── 3. EXECUÇÃO DE WORKFLOWS E PIPELINES E2E ─────────────────────────────────

run-e2e-pipeline: up
	@echo "--> Disparando o Pipeline E2E Completo consumindo os samples de input..."
	$(COMPOSE) exec cidacsrl_runner \
		poetry run python -m cidacsrl_rlp.tests.e2e.run_e2e_pipeline

run-linkage-pipeline: up-es
	@echo "--> Executando o pipeline de teste (apenas linkage)..."
	$(COMPOSE) run --rm --service-ports cidacsrl_runner python /app/cidacsrl_rlp/tests/e2e/run_linkage_pipeline.py
	@make down

# ─── 4. AUTOMAÇÃO DA SUÍTE DE TESTES (PYTEST) ──────────────────────────────────

test: up
	@echo "--> Executando toda a suíte de testes com isolamento síncrono da JVM..."
	$(COMPOSE) exec cidacsrl_runner pytest -v

test-integration: up
	@echo "--> Executando apenas os testes de integração..."
	$(COMPOSE) exec cidacsrl_runner pytest cidacsrl_rlp/tests/integration/ -v

test-unit: up
	@echo "--> Executando apenas os testes unitários..."
	$(COMPOSE) exec cidacsrl_runner pytest cidacsrl_rlp/tests/unit/ -v

# ─── 5. UTILITÁRIOS E BUILD DE ARTEFATOS ───────────────────────────────────────

build:
	@echo "--> Construindo o pacote wheel do projeto..."
	$(PYTHON) -m build
	@echo "--> Preparando diretório de distribuição Spark para cluster físico..."
	mkdir -p $(SPARK_PKG)
	$(PYTHON) -m pip download . -d $(SPARK_PKG)/
	cp dist/*.whl $(SPARK_PKG)/
	@echo "\n✅ Artefatos compilados para deploy em '$(SPARK_PKG)/'"

clean: down
	@echo "--> Limpando artefatos locais, caches de teste e volumes..."
	rm -rf build/ dist/ ./*.egg-info/ $(SPARK_PKG)/ .pytest_cache/ .htmlcov/ .coverage
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	@echo "⚠️  Nota: A pasta .es_data e data/ não foram removidas para preservar os índices e samples."
	@echo "   Para limpá-las totalmente, execute: sudo rm -rf .es_data data/output"

# ─── 6. LIMPEZA COMPLETA DO AMBIENTE DOCKER ─────────────────────────────────────

clean-docker: down
	@echo "--> Removendo volumes, redes e artefatos Docker associados ao ambiente..."
	$(COMPOSE) down -v --remove-orphans
	docker volume prune -f
	docker network prune -f
	@echo "✅ Ambiente Docker limpo!"

env-check:
	@command -v docker >/dev/null 2>&1 || (echo "❌ docker não encontrado no host" && exit 1)
	@docker compose version >/dev/null 2>&1 || (echo "❌ docker compose não encontrado no host" && exit 1)
	@echo "--> Dependências de infraestrutura validadas com sucesso."

help:
	@echo "========================================================================"
	@echo "                CIDACS-RL ENGINE - LAB COMMANDS MARKET                  "
	@echo "========================================================================"
	@echo "Infraestrutura Local (Docker Compose):"
	@echo "  make up                - Inicializa o laboratório padrão (ES, Kibana, Cerebro, Engine)"
	@echo "  make up-es             - Sobe apenas o serviço Elasticsearch"
	@echo "  make up-jupyter        - Inicializa o laboratório completo incluindo o Jupyter Dev"
	@echo "  make down              - Para e remove todos os contêineres e perfis"
	@echo "  make restart           - Reinicializa o ecossistema padrão"
	@echo "  make ps                - Lista os serviços ativos no ecossistema"
	@echo ""
	@echo "Diagnóstico e Monitoramento:"
	@echo "  make logs              - Acompanha o log consolidado de todos os serviços"
	@echo "  make logs-jupyter      - Logs exclusivos do servidor Jupyter"
	@echo "  make logs-engine       - Logs exclusivos da Engine de processamento"
	@echo "  make shell-jupyter     - Abre terminal bash dentro do container Jupyter"
	@echo "  make shell-engine      - Abre terminal bash dentro do container Engine"
	@echo ""
	@echo "Execução de Pipelines e Testes:"
	@echo "  make run-e2e-pipeline  - Roda a esteira fim-a-fim indexando e linkando os samples reais"
	@echo "  make run-linkage-pipeline - Roda a esteira de linkage (requer dados indexados)"
	@echo "  make test              - Roda todos os testes (Unitários e Integração) via contêiner"
	@echo "  make test-integration  - Executa apenas os testes da camada de integração"
	@echo "  make test-unit         - Executa apenas as validações unitárias em memória"
	@echo ""
	@echo "Compilação e Faxina:"
	@echo "  make build             - Prepara pacote Wheel e dependências para o cluster de prod"
	@echo "  make clean             - Remove containers, redes e arquivos temporários de compilação"
	@echo "  make clean-docker      - Limpa volumes, redes e artefatos Docker do ambiente"
	@echo "  make stop-all          - Para todos os contêineres. Use com cautela!"
	@echo "  make stop-test-e2e     - Para apenas o container da Engine de Linkage"
	@echo "========================================================================"