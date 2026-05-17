PYTHON := python
SPARK_PKG_DIR := spark_packages
COMPOSE     := docker compose -f cidacsrl_rlp/docker-compose.yml
COMPOSE_DEV := docker compose -f cidacsrl_rlp/docker-compose.yml -f docker-compose.dev.yml


.PHONY: all build clean help
.PHONY: up-jupyter up-es stop down
.PHONY: dev rebuild env-check notebook ps shell-spark logs-spark logs-jupyter logs-es
.PHONY: build-jupyter build-spark build-es rebuild-jupyter rebuild-spark rebuild-es
.PHONY: shell-spark shell-jupyter shell-es
.PHONY: run-cleaning-dev run-deduplication-dev run-es-indexing-dev run-linkage-dev
.PHONY: remove-orphans


all: build

build:
	@echo "--> 1/4: Construindo o pacote wheel do projeto..."
	$(PYTHON) -m build
	@echo "--> 2/4: Criando o diretório de pacotes: $(SPARK_PKG_DIR)/"
	mkdir -p $(SPARK_PKG_DIR)
	@echo "--> 3/4: Baixando as dependências do projeto para $(SPARK_PKG_DIR)/"
	$(PYTHON) -m pip download . -d $(SPARK_PKG_DIR)/
	@echo "--> 4/4: Copiando o wheel do projeto para $(SPARK_PKG_DIR)/"
	cp dist/*.whl $(SPARK_PKG_DIR)/
	@echo "\n✅ Pacote para Spark criado com sucesso em '$(SPARK_PKG_DIR)/'"
	@echo "   Use o conteúdo deste diretório com a flag --py-files do spark-submit."

dev: build up-jupyter

build-jupyter:
	$(COMPOSE) --profile jupyter build jupyter

build-spark:
	$(COMPOSE) --profile spark build spark

rebuild-jupyter:
	$(COMPOSE) --profile jupyter down
	$(COMPOSE) --profile jupyter build --no-cache jupyter

rebuild-spark:
	$(COMPOSE) --profile cleaning --profile deduplication --profile es-indexing --profile linkage down
	$(COMPOSE) --profile cleaning --profile deduplication --profile es-indexing --profile linkage build --no-cache

rebuild-es:
	$(COMPOSE) --profile elasticsearch down
	$(COMPOSE) --profile elasticsearch pull elasticsearch

remove-orphans:
	$(COMPOSE) down --remove-orphans

rebuild:
	$(COMPOSE) --profile jupyter --profile spark --profile elasticsearch --profile es-spark down
	$(COMPOSE) --profile jupyter --profile spark build --no-cache

env-check:
	@command -v docker   >/dev/null 2>&1 || (echo "❌ docker não encontrado" && exit 1)
	@docker compose version >/dev/null 2>&1 || (echo "❌ docker compose não encontrado" && exit 1)
	@command -v $(PYTHON) >/dev/null 2>&1 || (echo "❌ python não encontrado" && exit 1)
	@echo "✅ Todas as dependências encontradas."

up-jupyter:
	$(COMPOSE) --profile jupyter up -d jupyter

up-es:
	$(COMPOSE) --profile elasticsearch up -d elasticsearch

run-cleaning:
	$(COMPOSE) --profile cleaning run --rm cidacsrl-cleaning

run-deduplication:
	$(COMPOSE) --profile deduplication run --rm cidacsrl-deduplication

run-es-indexing:
	$(COMPOSE) --profile es-indexing up --abort-on-container-exit --exit-code-from cidacsrl-es-indexing

run-linkage:
	$(COMPOSE) --profile linkage up --abort-on-container-exit --exit-code-from cidacsrl-linkage

# Dev targets: source code mounted as volume, no rebuild needed
run-cleaning-dev:
	$(COMPOSE_DEV) --profile cleaning run --rm cidacsrl-cleaning

run-deduplication-dev:
	$(COMPOSE_DEV) --profile deduplication run --rm cidacsrl-deduplication

run-es-indexing-dev:
	$(COMPOSE_DEV) --profile es-indexing up --abort-on-container-exit --exit-code-from cidacsrl-es-indexing

run-linkage-dev:
	$(COMPOSE_DEV) --profile linkage up

stop-jupyter:
	$(COMPOSE) --profile jupyter stop jupyter

stop-es:
	$(COMPOSE) --profile elasticsearch stop elasticsearch

stop:
	$(COMPOSE) --profile jupyter --profile elasticsearch stop

down-jupyter:
	$(COMPOSE) --profile jupyter down

down-es:
	$(COMPOSE) --profile elasticsearch down

down:
	$(COMPOSE) --profile jupyter --profile elasticsearch down

ps:
	$(COMPOSE) --profile jupyter --profile elasticsearch ps

logs-spark:
	$(COMPOSE) --profile spark logs -f spark

logs-jupyter:
	$(COMPOSE) --profile jupyter logs -f jupyter

logs-es:
	$(COMPOSE) --profile elasticsearch logs -f elasticsearch

shell-spark:
	$(COMPOSE) exec spark bash

shell-jupyter:
	$(COMPOSE) --profile jupyter exec jupyter bash

shell-es:
	$(COMPOSE) --profile elasticsearch exec elasticsearch bash

notebook: up-jupyter
	@echo "--> Aguardando Jupyter iniciar..."
	@sleep 2
	@xdg-open http://localhost:8888 2>/dev/null || open http://localhost:8888 2>/dev/null || echo "Acesse: http://localhost:8888"

clean:
	@echo "--> Limpando artefatos de build e containers..."
	$(COMPOSE) --profile jupyter --profile spark --profile elasticsearch --profile es-spark down --volumes --remove-orphans
	rm -rf build/ dist/ ./*.egg-info/ $(SPARK_PKG_DIR)/
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	@echo "Limpeza concluída."

help:
	@echo "Comandos disponíveis:"
	@echo "  make build               - Cria o pacote wheel e baixa suas dependências para uso com Spark."
	@echo "  make build-jupyter       - Build da imagem Docker do Jupyter."
	@echo "  make build-spark         - Build da imagem Docker do Spark."
	@echo "  make build-es            - (informativo) Elasticsearch usa imagem oficial."
	@echo "  make rebuild             - Derruba tudo e reconstrói todas as imagens sem cache."
	@echo "  make rebuild-jupyter     - Derruba e reconstrói a imagem do Jupyter sem cache."
	@echo "  make rebuild-spark       - Derruba e reconstrói a imagem do Spark sem cache."
	@echo "  make rebuild-es          - Derruba e atualiza a imagem do Elasticsearch."
	@echo "  make remove-orphans       - Remove containers órfãos do projeto."
	@echo "  make clean               - Remove todos os artefatos de build e containers."
	@echo "  make up-jupyter          - Sobe apenas o Jupyter (Spark local no container)."
	@echo "  make up-es               - Sobe apenas o Elasticsearch."
	@echo "  make run-cleaning        - Executa o workflow de limpeza."
	@echo "  make run-deduplication   - Executa o workflow de deduplicação."
	@echo "  make run-es-indexing     - Executa o workflow de indexação no Elasticsearch."
	@echo "  make run-linkage         - Executa o workflow de linkage."
	@echo "  make stop                - Para os containers sem removê-los."
	@echo "  make down                - Para e remove os containers."
	@echo "  make dev                 - Build do pacote + sobe Jupyter."
	@echo "  make rebuild             - Derruba tudo e sobe perfil es-spark com rebuild."
	@echo "  make env-check           - Verifica se as dependências estão instaladas."
	@echo "  make ps                  - Lista os containers em execução."
	@echo "  make logs-spark          - Acompanha os logs do serviço spark."
	@echo "  make logs-jupyter        - Acompanha os logs do serviço jupyter."
	@echo "  make logs-es             - Acompanha os logs do serviço elasticsearch."
	@echo "  make shell-spark         - Abre um shell bash no container spark."
	@echo "  make shell-jupyter       - Abre um shell bash no container jupyter."
	@echo "  make shell-es            - Abre um shell bash no container elasticsearch."
	@echo "  make notebook            - Abre o Jupyter no navegador."
	@echo "  make help                - Mostra esta mensagem de ajuda."