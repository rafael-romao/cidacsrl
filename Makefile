PYTHON := python
SPARK_PKG_DIR := spark_packages


.PHONY: all build clean help


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

clean:
	@echo "--> Limpando artefatos de build..."
	rm -rf build/ dist/ ./*.egg-info/ $(SPARK_PKG_DIR)/
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	@echo "Limpeza concluída."

help:
	@echo "Comandos disponíveis:"
	@echo "  make build               - Cria o pacote wheel e baixa suas dependências para uso com Spark."
	@echo "  make clean               - Remove todos os artefatos de build gerados."
	@echo "  make help                - Mostra esta mensagem de ajuda."