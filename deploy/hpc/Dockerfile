# syntax=docker/dockerfile:1
#
# Imagem de execução OFFLINE (air-gapped) para HPC — pensada para ser
# convertida em Singularity/Apptainer (.sif) e rodar em nós sem internet.
#
# Diferença para tests/environment/Dockerfile (ambiente de laboratório local):
# aqui NÃO existe nenhuma etapa que resolva jars em runtime. Todos os jars da
# JVM são baixados no build (máquina COM internet) e embutidos em
# /opt/spark/jars, de onde o Spark os carrega automaticamente. Assim o
# artefato final não depende de `spark.jars.packages` (resolução Ivy/Maven).
#
# Build (numa máquina com internet, a partir da raiz do repositório):
#   docker build -f deploy/hpc/Dockerfile --target full -t cidacsrl:offline .
#
# Alvos disponíveis:
#   linkage        -> linkage / indexing  (jar do conector Elasticsearch)
#   deduplication  -> deduplication       (jar do GraphFrames)
#   full           -> todas as etapas num único artefato (recomendado p/ HPC)


ARG SPARK_IMAGE=apache/spark:3.5.8-scala2.12-java17-ubuntu
ARG ES_CONNECTOR_VERSION=9.1.8
ARG GRAPHFRAMES_VERSION=0.8.3-spark3.5-s_2.12

# ---------------------------------------------------------------------------
# Stage 1: baixa os jars da JVM (única etapa que precisa de internet)
# ---------------------------------------------------------------------------
FROM curlimages/curl:8.11.0 AS jar-downloader
ARG ES_CONNECTOR_VERSION
ARG GRAPHFRAMES_VERSION
WORKDIR /jars

RUN curl -fL "https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/${ES_CONNECTOR_VERSION}/elasticsearch-spark-30_2.12-${ES_CONNECTOR_VERSION}.jar" \
        -o "elasticsearch-spark-30_2.12-${ES_CONNECTOR_VERSION}.jar" && \
    curl -fL "https://repos.spark-packages.org/graphframes/graphframes/${GRAPHFRAMES_VERSION}/graphframes-${GRAPHFRAMES_VERSION}.jar" \
        -o "graphframes-${GRAPHFRAMES_VERSION}.jar"

# ---------------------------------------------------------------------------
# Stage 2: build do wheel do cidacsrl
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS python-builder
WORKDIR /build
RUN pip install --no-cache-dir build
COPY pyproject.toml README.md ./
COPY src/cidacsrl ./src/cidacsrl
RUN python -m build --wheel

# ---------------------------------------------------------------------------
# Stage 3: runtime base (Spark + cidacsrl instalado, ainda SEM jars)
# ---------------------------------------------------------------------------
FROM ${SPARK_IMAGE} AS runtime
USER root
WORKDIR /app

# Instala Python 3.12 (deadsnakes) — precisa de internet, só nesta etapa de
# build. O artefato final (.sif) não depende mais de rede.
RUN apt-get update && \
    apt-get install -y --no-install-recommends software-properties-common gnupg curl ca-certificates && \
    add-apt-repository -y ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y --no-install-recommends python3.12 python3.12-venv && \
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3.12 && \
    ln -sf /usr/bin/python3.12 /usr/local/bin/python3 && \
    ln -sf /usr/bin/python3.12 /usr/local/bin/python && \
    apt-get purge -y software-properties-common gnupg && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

ENV PYSPARK_PYTHON=/usr/bin/python3.12
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3.12
ENV PATH=/usr/local/bin:$PATH
ENV PYTHONUNBUFFERED=1
# pyspark usa o Spark embutido em SPARK_HOME; qualquer jar em /opt/spark/jars
# é adicionado ao classpath automaticamente, sem spark.jars.packages.
ENV SPARK_HOME=/opt/spark

COPY --from=python-builder /build/dist/*.whl /tmp/wheels/
RUN pip install --no-cache-dir /tmp/wheels/*.whl graphframes && \
    rm -rf /tmp/wheels

USER spark

# ---------------------------------------------------------------------------
# Alvo: linkage / indexing (conector Elasticsearch)
# ---------------------------------------------------------------------------
FROM runtime AS linkage
ARG ES_CONNECTOR_VERSION
USER root
COPY --from=jar-downloader /jars/elasticsearch-spark-30_2.12-${ES_CONNECTOR_VERSION}.jar /opt/spark/jars/
USER spark

# ---------------------------------------------------------------------------
# Alvo: deduplication (GraphFrames)
# ---------------------------------------------------------------------------
FROM runtime AS deduplication
ARG GRAPHFRAMES_VERSION
USER root
COPY --from=jar-downloader /jars/graphframes-${GRAPHFRAMES_VERSION}.jar /opt/spark/jars/
USER spark

# ---------------------------------------------------------------------------
# Alvo: full — um único artefato serve para linkage, indexing e deduplication.
# É o alvo recomendado para HPC: gera um único .sif para todo o pipeline.
# ---------------------------------------------------------------------------
FROM runtime AS full
USER root
COPY --from=jar-downloader /jars/*.jar /opt/spark/jars/
USER spark
