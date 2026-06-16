# Build do pacote Python
FROM python:3.10-slim AS builder

WORKDIR /build
RUN pip install --no-cache-dir build

COPY pyproject.toml ./
COPY cidacsrl_rlp ./cidacsrl_rlp

RUN python -m build && \
    mkdir -p spark_packages && \
    python -m pip download . -d spark_packages/ && \
    cp dist/*.whl spark_packages/

# Download do JAR do Elasticsearch Spark Connector
FROM python:3.10-slim AS es-jar-downloader

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fL "https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/9.2.8/elasticsearch-spark-30_2.12-9.2.8.jar" \
    -o /elasticsearch-spark-30_2.12-9.2.8.jar


# Download do JAR do GraphFrames
FROM python:3.10-slim AS graphframes-jar-downloader

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fL "https://repos.spark-packages.org/graphframes/graphframes/0.8.2-spark3.2-s_2.12/graphframes-0.8.2-spark3.2-s_2.12.jar" \
    -o /graphframes-0.8.2.jar


# Runtime Spark
FROM apache/spark:3.5.8-python3 AS runtime-spark

USER root
WORKDIR /app

ENV PYTHONPATH=/app:$PYTHONPATH
ENV PATH=/opt/spark/bin:$PATH

COPY --from=builder /build/spark_packages /opt/spark/pyfiles
RUN pip install --no-cache-dir /opt/spark/pyfiles/*.whl

COPY cidacsrl_rlp/ ./cidacsrl_rlp/
COPY pyproject.toml ./

EXPOSE 4040
USER spark


# Runtime Spark + GraphFrames (deduplication)
FROM runtime-spark AS runtime-spark-graphframes

USER root
COPY --from=graphframes-jar-downloader /graphframes-0.8.2.jar /opt/spark/jars/
RUN pip install --no-cache-dir graphframes
USER spark

# Runtime Spark + ES JAR (indexing, linkage)
FROM runtime-spark AS runtime-spark-es

USER root
COPY --from=es-jar-downloader /elasticsearch-spark-30_2.12-9.2.8.jar /opt/spark/jars/
USER spark


# Runner para Lab
# Runner para desenvolvimento (com Poetry e dependências de dev)
FROM python:3.12-slim AS runner-dev

WORKDIR /app

RUN apt-get update && apt-get install -y default-jdk procps curl

COPY pyproject.toml poetry.lock* ./


RUN pip install --no-cache-dir poetry


RUN poetry install --no-root


COPY . .

ENV PYTHONPATH=/app

CMD ["tail", "-f", "/dev/null"]


# Jupyter
FROM python:3.10-slim AS jupyter-dev

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PYTHONPATH=/app:$PYTHONPATH

COPY --from=builder /build/spark_packages /tmp/spark_packages

RUN pip install --no-cache-dir \
        /tmp/spark_packages/*.whl \
        jupyterlab \
        pyspark==3.5.8 && \
    rm -rf /tmp/spark_packages

COPY cidacsrl_rlp/ ./cidacsrl_rlp/
COPY pyproject.toml ./
COPY --from=es-jar-downloader /elasticsearch-spark-30_2.12-9.2.8.jar /opt/spark/jars/

EXPOSE 8888

CMD ["jupyter", "lab", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--allow-root", \
     "--ServerApp.token=''", \
     "--ServerApp.password=''"]