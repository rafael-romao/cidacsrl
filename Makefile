# Configurações de Executáveis e Variáveis
PYTHON     := python
COMPOSE    := docker compose -f core/tests/enviroment/docker-compose.yml
SPARK_PKG  := spark_packages
VENV_PYTHON := $(shell poetry env list --full-path 2>/dev/null | awk 'NR==1{print $$1}')/bin/python

.PHONY: all build clean help env-check clean-docker prepare-dirs stop-all stop generate-data
.PHONY: up up-es up-ui up-jupyter down restart ps logs logs-engine logs-es logs-cerebro logs-jupyter shell-engine shell-es shell-jupyter
.PHONY: test test-integration test-unit run-e2e-pipeline run-e2e-indexing-only
.PHONY: test-unit-dedup run-e2e-dedup

all: help

# ─── 1. GERENCIAMENTO DO LABORATÓRIO E2E (DOCKER COMPOSE) ───────────────────────

up: env-check
	@echo "--> Subindo o ambiente de teste (Elasticsearch + Engine)..."
	$(COMPOSE) --profile elasticsearch --profile runner up -d --remove-orphans
	@echo "Verificando disponibilidade do Elasticsearch..."
	@timeout=90; \
	while ! docker inspect --format='{{.State.Health.Status}}' cidacsrl_elasticsearch 2>/dev/null | grep -q healthy; do \
		sleep 2; \
		timeout=$$((timeout-2)); \
		if [ $$timeout -le 0 ]; then \
			echo "Elasticsearch não ficou saudável a tempo!"; \
			exit 1; \
		fi; \
	done
	@echo "\n✅ Ambiente de teste disponível!"
	@echo "\n- Elasticsearch rodando em: http://localhost:9200. Use 'make up-ui' para acessar o Cerebro."
	@echo "\n- Use 'make run-e2e-pipeline ENV=<arquivo.yml>' para rodar o pipeline de teste completo."
	@echo "\n- Use 'make up-jupyter' para subir o ambiente interativo Jupyter Notebook."
	@echo "\n- Use 'make logs' para acompanhar os logs em tempo real."
	@echo "\n- Use 'make down' para derrubar o ambiente quando terminar."
	@echo "\n- Use 'make help' para ver todos os comandos disponíveis.\n"

up-es: env-check
	@echo "\n --> Subindo apenas o serviço do Elasticsearch..."
	$(COMPOSE) up -d elasticsearch

up-ui: env-check
	@echo "\n --> Subindo ferramentas de monitoria Visual (Cerebro)..."
	$(COMPOSE) --profile ui up -d

up-jupyter: env-check
	@echo "\n --> Subindo ambiente interativo Jupyter Notebook..."
	@echo "Acessar via http://localhost:8888 (token disponível nos logs do container jupyter-notebook)"
	$(COMPOSE) --profile jupyter up -d

down:
	@echo "--> Derrubando os contêineres locais do ambiente de teste..."
	$(COMPOSE) --profile elasticsearch --profile runner --profile ui --profile jupyter down -v --remove-orphans

restart: down up

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f

logs-engine:
	$(COMPOSE) logs -f runner

logs-es:
	$(COMPOSE) logs -f elasticsearch

logs-cerebro:
	$(COMPOSE) logs -f cerebro

logs-jupyter:
	$(COMPOSE) logs -f jupyter-notebook

shell-engine:
	docker exec -it cidacsrl_runner bash

shell-es:
	docker exec -it cidacsrl_elasticsearch bash

shell-jupyter:
	docker exec -it cidacsrl_jupyter_notebook bash

# ─── 2. EXECUÇÃO DE PIPELINES E TESTES ─────────────────────────────────────────

generate-data: up
	@echo "Gerando dados de teste..."
	$(COMPOSE) exec cidacsrl_runner python core/tests/e2e/generate_e2e_data_acidentes.py

run-e2e-pipeline: up
	@if [ -z "$(ENV)" ]; then echo "❌ Erro: Variável ENV é obrigatória. Uso: make run-e2e-pipeline ENV=nome_do_arquivo.yml"; exit 1; fi
	@echo "Pipeline executando: verificação de índice + linkage usando $(ENV)"
	$(COMPOSE) exec cidacsrl_runner \
		python core/tests/e2e/run_e2e_pipeline.py --env-name $(ENV)

run-e2e-indexing-only: up
	@if [ -z "$(ENV)" ]; then echo "❌ Erro: Variável ENV é obrigatória. Uso: make run-e2e-indexing-only ENV=nome_do_arquivo.yml"; exit 1; fi
	@echo "Pipeline executando apenas com indexação usando $(ENV)"
	$(COMPOSE) exec cidacsrl_runner \
		python core/tests/e2e/run_e2e_pipeline.py --env-name $(ENV) --skip-linkage

ES_CONNECTOR_PKG := org.elasticsearch:elasticsearch-spark-30_2.12:9.1.8
PYSPARK_ENV     := -e CIDACSRL_ES_URL=http://elasticsearch:9200 -e "PYSPARK_SUBMIT_ARGS=--packages $(ES_CONNECTOR_PKG) pyspark-shell"

test: up
	@echo "--> Executando toda a suíte de testes..."
	$(COMPOSE) exec $(PYSPARK_ENV) cidacsrl_runner pytest core/tests/ -v

test-integration: up
	@echo "--> Executando testes de integração..."
	$(COMPOSE) exec $(PYSPARK_ENV) cidacsrl_runner pytest core/tests/integration/ -m integration -v

test-unit: up
	@echo "--> Executando testes unitários..."
	$(COMPOSE) exec cidacsrl_runner pytest core/tests/unit/ -m unit -v

test-unit-dedup:
	@echo "--> Executando testes unitários do módulo deduplicating (local)..."
	@if [ -z "$(VENV_PYTHON)" ]; then echo "❌ Erro: virtualenv Poetry não encontrado em ~/.cache/pypoetry/virtualenvs/"; exit 1; fi
	$(VENV_PYTHON) -m pytest deduplicating/tests/unit/ -v --tb=short -m unit

run-e2e-dedup:
	@echo "--> Executando pipeline E2E de deduplicação com dados locais de teste..."
	@if [ -z "$(VENV_PYTHON)" ]; then echo "❌ Erro: virtualenv Poetry não encontrado em ~/.cache/pypoetry/virtualenvs/"; exit 1; fi
	$(VENV_PYTHON) deduplicating/tests/e2e/run_e2e_deduplication.py $(if $(CONFIG),--config-path $(CONFIG),)

# ─── 3. COMPILAÇÃO, EMPACOTAMENTO E LIMPEZA ────────────────────────────────────

build:
	@echo "--> Preparando empacotamento via Poetry..."
	poetry build

build-docker:
	@echo "--> Reconstruindo TODAS as imagens do ecossistema (PIP Global + Profiles)..."
	$(COMPOSE) --profile elasticsearch --profile runner --profile ui --profile jupyter build
	@echo "✅ Imagens reconstruídas com sucesso!"

clean:
	@echo "--> Limpando arquivos temporários e binários locais..."
	rm -rf dist/ .pytest_cache/ .coverage htmlcov/ .isort_cache/
	
	@echo "--> Limpando __pycache__ com permissões adequadas..."
	@if docker ps --format '{{.Names}}' | grep -q "^cidacsrl_runner$$"; then \
		echo "   [Docker ativo] Removendo via container (root)..."; \
		docker exec -t cidacsrl_runner find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true; \
	else \
		echo "   [Docker inativo] Tentando remoção local (pode solicitar sudo se houver arquivos de root)..."; \
		find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || \
		(echo "⚠️ Arquivos presos detectados! Executando com sudo para limpar resíduos do Docker..." && sudo find . -type d -name "__pycache__" -exec rm -rf {} +); \
	fi
	@echo "✅ Faxina concluída com sucesso!"

clean-docker: down
	@echo "--> Removendo volumes e faxinando o ambiente Docker..."
	$(COMPOSE) down -v --remove-orphans
	docker system prune -f --volumes

stop-cidacsrl-runner:
	@echo "--> Parando container cidacsrl_runner..."
	docker stop cidacsrl_runner || echo "Container cidacsrl_runner não está em execução."
	@echo "Container cidacsrl_runner parado."

stop-jupyter:
	@echo "--> Parando container cidacsrl_jupyter..."
	docker stop cidacsrl_jupyter || echo "Container cidacsrl_jupyter não está em execução."
	@echo "Container cidacsrl_jupyter parado."

stop-es:
	@echo "--> Parando container cidacsrl_elasticsearch..."
	docker stop cidacsrl_elasticsearch || echo "Container cidacsrl_elasticsearch não está em execução."
	@echo "Container cidacsrl_elasticsearch parado."

stop-all:
	@echo "⚠️ Parando TODOS os contêineres em execução no Docker..."
	docker stop $$(docker ps -a -q) 2>/dev/null || true

# ─── 4. AUXILIARES INTERNOS ────────────────────────────────────────────────────

env-check:
	@if [ ! -d "core/tests/enviroment" ]; then \
		echo "❌ Erro: Diretório 'core/tests/enviroment' não encontrado. Certifique-se de estar na raiz do repositório."; \
		exit 1; \
	fi

help:
	@echo "========================================================================="
	@echo "                   CIDACS-RL ENGINE - LABORATÓRIO DE DESENVOLVIMENTO      "
	@echo "========================================================================="
	@echo "Comandos de Infraestrutura (Docker Compose):"
	@echo "  make up                - Inicializa Elasticsearch + Runner Node (Recomendado)"
	@echo "  make up-es             - Sobe estritamente o banco Elasticsearch"
	@echo "  make up-ui             - Sobe Cerebro para inspeção visual de índices"
	@echo "  make up-jupyter        - Abre servidor do Jupyter Notebook para análises"
	@echo "  make down              - Desliga todos os serviços locais"
	@echo "  make restart           - Reinicializa o ambiente"
	@echo "  make ps                - Lista o status dos contêineres"
	@echo "  make logs              - Exibe logs agregados de todos os serviços"
	@echo "  make logs-jupyter      - Logs exclusivos do servidor Jupyter"
	@echo "  make logs-engine       - Logs exclusivos da Engine de processamento"
	@echo "  make shell-jupyter     - Abre terminal bash dentro do container Jupyter"
	@echo "  make shell-engine      - Abre terminal bash dentro do container Engine"
	@echo ""
	@echo "Execução de Pipelines e Testes:"
	@echo "  make run-e2e-pipeline ENV=<arquivo.yml>    - Roda a esteira fim-a-fim (pula indexação se já populado)"
	@echo "  make run-e2e-indexing-only ENV=<arquivo.yml> - Roda apenas a indexação"
	@echo "  make test                                  - Roda todos os testes (Unitários e Integração) via contêiner"
	@echo "  make test-integration                      - Executa apenas os testes da camada de integração"
	@echo "  make test-unit                             - Executa apenas as validações unitárias em memória"
	@echo ""
	@echo "Deduplicação (local, sem Docker):"
	@echo "  make test-unit-dedup                       - Testes unitários do módulo deduplicating (venv local)"
	@echo "  make run-e2e-dedup                         - Pipeline E2E de deduplicação com dados locais de teste"
	@echo "  make run-e2e-dedup CONFIG=<path.yml>       - Pipeline E2E com config customizado"
	@echo ""
	@echo "Compilação e Faxina:"
	@echo "  make build             - Prepara pacote Wheel e dependências via Poetry"
	@echo "  make clean             - Remove arquivos temporários de compilação"
	@echo "  make clean-docker      - Limpa volumes, redes e artefatos Docker do ambiente"
	@echo "  make stop-all          - Para todos os contêineres do seu daemon Docker."