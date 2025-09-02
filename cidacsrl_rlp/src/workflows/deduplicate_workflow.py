import os
import argparse
import logging
from typing import Dict, Any

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from graphframes import GraphFrame

from cidacsrl_rlp.src.config.loader import load_yaml
from cidacsrl_rlp.src.utils.logging_config import setup_logging
from cidacsrl_rlp.src.utils.spark_utils import create_spark_session

logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Deduplicate datasets from a linked source."
    )
    parser.add_argument(
        "--config-path",
        required=True,
        help="The path to the main YAML configuration file for the workflow.",
    )
    return parser.parse_args()


def load_and_setup_config(config_path: str) -> Dict[str, Any]:
    """Load YAML configuration and set up logging."""
    logger.info(f"Loading main workflow configuration from: {config_path}")
    config = load_yaml(config_path)
    log_level = config.get("log_level", "INFO").upper()
    setup_logging(level=getattr(logging, log_level))
    return config


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
    """Main workflow execution."""
    args = parse_arguments()
    config = load_and_setup_config(args.config_path)
    logger.info(f"Starting deduplication workflow with configuration: {config}")

    spark = create_spark_session(
        app_name=config.get("app_name", "DeduplicationApp"),
        spark_config_path=config.get("spark_config_path"),
    )

    source_data_path = config.get("source_data_path")
    df_source = spark.read.format('parquet').load(source_data_path)

    graph = build_graph(df_source)
    df_components_result = find_connected_components(graph)

    output_path = config.get("output_data_path")
    join_components_and_save(df_source, df_components_result, os.path.join(output_path))

    spark.stop()


if __name__ == "__main__":
    main()