# ─── 1. BUILD DO PACOTE PYTHON (STAGE INICIAL) ────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build
RUN pip install --no-cache-dir build

# Copia os manifestos do Poetry
COPY pyproject.toml poetry.lock* ./

# Copia os módulos mapeados diretamente na raiz do projeto
COPY core ./core
COPY cleaning ./cleaning
COPY deduplicating ./deduplicating
COPY cli.py ./cli.py
COPY README.md ./README.md

# Constrói o pacote Wheel usando o backend do Poetry configurado
RUN python -m build && \
    mkdir -p spark_packages && \
    python -m pip download . -d spark_packages/ && \
    cp dist/*.whl spark_packages/


# ─── 2. DOWNLOAD DOS JARS ADICIONAIS DE INFRAESTRUTURA ────────────────────────
FROM python:3.12-slim AS es-jar-downloader
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*
RUN curl -fL "https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/9.2.8/elasticsearch-spark-30_2.12-9.2.8.jar" \
    -o /elasticsearch-spark-30_2.12-9.2.8.jar

FROM python:3.12-slim AS graphframes-jar-downloader
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*
RUN curl -fL "https://repos.spark-packages.org/graphframes/graphframes/0.8.2-spark3.2-s_2.12/graphframes-0.8.2-spark3.2-s_2.12.jar" \
    -o /graphframes-0.8.2-spark3.2-s_2.12.jar


# ─── 3. AMBIENTE DE RUNTIME SPARK BASE (PYTHON 3.12) ──────────────────────────
FROM apache/spark:3.5.0 AS runtime-spark
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.12 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

ENV PYSPARK_PYTHON=python3.12
ENV PYSPARK_DRIVER_PYTHON=python3.12

COPY --from=graphframes-jar-downloader /graphframes-0.8.2-spark3.2-s_2.12.jar /opt/spark/jars/
RUN pip install --no-cache-dir graphframes
USER spark


# ─── 4. AMBIENTE DE RUNTIME SPARK + ELASTICSEARCH ─────────────────────────────
FROM runtime-spark AS runtime-spark-es
USER root
COPY --from=es-jar-downloader /elasticsearch-spark-30_2.12-9.2.8.jar /opt/spark/jars/
USER spark


# ─── 5. IMAGEM BASE PARA LABORATÓRIO (COMPARTILHADA) ──────────────────────────
FROM python:3.12-slim AS lab-base
WORKDIR /app

# Instala o JRE headless (mais leve) e utilitários comuns ao Runner e ao Jupyter
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PYTHONPATH=/app:$PYTHONPATH


# ─── 6. RUNNER PARA DESENVOLVIMENTO LOCAL E TESTES (LAB - PIP GLOBAL) ─────────
FROM lab-base AS runner-dev

# Instala o Poetry apenas para ferramentas auxiliares se necessário, mas não para o ambiente
RUN pip install --no-cache-dir poetry

# Copia os manifestos para a raiz temporária do container
COPY pyproject.toml poetry.lock* ./

# EXPORTAÇÃO E INSTALAÇÃO VIA PIP: 
RUN poetry config virtualenvs.create false && \
    poetry install --no-root --with dev,doc

# Copia todo o código-fonte mapeado na raiz para o container
COPY core ./core
COPY cleaning ./cleaning
COPY deduplicating ./deduplicating
COPY cli.py ./cli.py
COPY README.md ./README.md

# Instala o próprio projeto em modo editável via pip para registrar o mapeamento de pacotes
RUN pip install -e .

CMD ["tail", "-f", "/dev/null"]


# ─── 7. AMBIENTE INTERATIVO JUPYTER NOTEBOOK (LAB - PYTHON 3.12) ──────────────
FROM lab-base AS jupyter-dev

# Recupera o pacote wheel gerado no estágio builder
COPY --from=builder /build/spark_packages /tmp/spark_packages

# Instala as dependências de runtime e do ecossistema de análise interativa
RUN pip install --no-cache-dir \
        /tmp/spark_packages/*.whl \
        jupyterlab \
        pyspark==3.5.8 && \
    rm -rf /tmp/spark_packages

# Copia os mesmos módulos para que fiquem disponíveis para import no Jupyter
COPY core ./core
COPY cleaning ./cleaning
COPY deduplicating ./deduplicating
COPY cli.py ./cli.py

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]