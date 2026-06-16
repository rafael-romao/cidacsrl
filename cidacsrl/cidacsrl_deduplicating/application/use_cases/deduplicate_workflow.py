import os
import argparse
import logging
import time
from typing import Dict, Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from graphframes import GraphFrame

from cidacsrl.shared.infra.config_loader import load_deduplicate_workflow_config
from cidacsrl.src.utils.logging_config import setup_logging
from cidacsrl.cidacsrl.infra.spark.utils import create_spark_session

logger = logging.getLogger(__name__)


def build_graph(df_source: DataFrame) -> GraphFrame:
    """Build a graph from the source DataFrame."""
    df_source_ids = df_source.select(F.col('id_table').alias("id"))
    df_target_ids = df_source.select(F.col('candidate_id_table').alias("id"))

    df_vertices = df_source_ids.union(df_target_ids).distinct()
    logger.info(f"Number of vertices: {df_vertices.count():,}")

    df_edges = df_source.select(
        F.col('id_table').alias("src"),
        F.col('candidate_id_table').alias("dst")
    )
    logger.info(f"Number of edges: {df_edges.count():,}")

    return GraphFrame(df_vertices, df_edges)


def find_connected_components(graph: GraphFrame) -> DataFrame:
    """Find connected components in the graph."""
    df_components_result = graph.connectedComponents()
    distinct_component_count = df_components_result.select("component").distinct().count()
    logger.info(f"Found {distinct_component_count:,} distinct connected components.")
    return df_components_result


def join_components_and_save(df_source: DataFrame, df_components: DataFrame, output_path: str):
    """Join source data with component results and save to Parquet."""
    df_identified_components = df_source.join(
        df_components,
        on=[df_source.id_table == df_components.id],
        how="left_outer"
    ).drop('id').withColumnRenamed("component", "group_id")

    logger.info(f"Saving deduplicated data to: {output_path}")
    df_identified_components.write.mode("overwrite").parquet(output_path)
    logger.info("Deduplication workflow finished successfully.")


def main():
    """Ponto de entrada principal para o fluxo de trabalho de deduplicação.

    Este script executa um processo de deduplicação de dados usando GraphFrames
    para encontrar componentes conectados. Ele é projetado para ser executado
    a partir da linha de comando e realiza as seguintes operações:

    1.  Carrega as configurações do workflow a partir de um arquivo YAML central.
    2.  Inicializa uma sessão Spark com as configurações fornecidas.
    3.  Carrega os dados linkados da fonte especificada.
    4.  Constrói um grafo a partir dos dados linkados.
    5.  Encontra componentes conectados no grafo para identificar grupos de duplicatas.
    6.  Salva os resultados da deduplicação.

    Args:
        --config-path (str): Caminho para o arquivo de configuração YAML principal
            do workflow que contém os caminhos para todos os outros arquivos de
            configuração.
        --log-level (str): Nível de logging para a aplicação (padrão: "INFO").

    Example:
        Para executar o fluxo de trabalho a partir do terminal:

        .. code-block:: bash

            python -m cidacsrl_rlp.src.workflows.deduplicate_workflow \\
                --config-path /path/to/deduplicate_workflow_config.yaml \\
                --log-level DEBUG

        O arquivo de configuração do workflow deve conter:

        .. code-block:: yaml

            spark_config_path: "/path/to/spark_config.yaml"
            source_data_path: "/path/to/linked_data.parquet"
            output_data_path: "/path/to/deduplicated_data.parquet"
            app_name: "DeduplicationApp"
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Deduplicate datasets from a linked source.")
    parser.add_argument(
        "--config-path",
        required=True,
        help="The path to the workflow configuration YAML file containing all necessary paths and settings.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level for the application (default: INFO).",
    )
    args = parser.parse_args()
    
    setup_logging(level=getattr(logging, args.log_level.upper()))
    logger.info(f"Starting deduplication workflow with config: {args.config_path}")
    
    # Load workflow configuration
    try:
        logger.info("Loading workflow configuration...")
        workflow_config = load_deduplicate_workflow_config(args.config_path)
        logger.info(f"Workflow configuration loaded successfully for source: {workflow_config.source_data_path}")
    except (FileNotFoundError, ValueError, IOError) as e:
        logger.error(f"Failed to load workflow configuration: {e}")
        exit(1)
    
    workflow_start_time = time.time()
    
    # Initialize Spark session
    try:
        spark = create_spark_session(
            app_name=workflow_config.app_name,
            spark_config_path=workflow_config.spark_config_path,
        )
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        exit(1)

    try:
        # Load source data
        logger.info(f"Loading source data from: {workflow_config.source_data_path}")
        df_source = spark.read.format('parquet').load(workflow_config.source_data_path)
        logger.info(f"Source data loaded with {df_source.count():,} records.")

        # Build graph and find connected components
        graph = build_graph(df_source)
        df_components_result = find_connected_components(graph)

        # Join components and save results
        logger.info(f"Saving deduplicated data to: {workflow_config.output_data_path}")
        join_components_and_save(df_source, df_components_result, workflow_config.output_data_path)
        
        workflow_duration = time.time() - workflow_start_time
        logger.info(f"Deduplication workflow finished successfully in {workflow_duration:.2f} seconds.")

    except Exception as e:
        logger.critical(f"Critical error in deduplication workflow: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()