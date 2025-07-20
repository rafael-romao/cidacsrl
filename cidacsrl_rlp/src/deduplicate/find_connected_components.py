# src/deduplicate/find_components.py

import argparse
import logging
from pathlib import Path
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# GraphFrames is a Spark package and needs to be available in the Spark environment
try:
    from graphframes import GraphFrame
except ImportError as e:
    # This error will be more informative if GraphFrames is not installed/configured
    logging.error("GraphFrames library not found. Please ensure it's installed and "
                  "configured with Spark (e.g., --packages graphframes:graphframes:your_version).")
    raise e # Re-raise to stop execution if GraphFrames is essential

# Use the project's standard logging setup
from geo_cidacsrl.src.utils.logging_config import setup_logging

logger = logging.getLogger(__name__)

def find_connected_components( # Renamed for clarity from find_components
    spark: SparkSession,
    input_parquet_path: str,
    output_parquet_path: str,
    id_source_column: str,
    id_target_column: str, # This should be the *prefixed* target ID column name from linkage results
    component_id_column_name: str = "component_id"
) -> None:
    """
    Encontra componentes conectados em resultados de linkage (pares de IDs) usando GraphFrames.

    Os componentes conectados agrupam todos os IDs que estão direta ou indiretamente
    ligados entre si.

    Args:
        spark (SparkSession): A sessão Spark ativa.
        input_parquet_path (str): Caminho para o diretório Parquet de entrada contendo os pares
                                  de linkage (saída de um workflow de linkage).
                                  Espera-se que contenha pelo menos `id_source_column` e `id_target_column`.
        output_parquet_path (str): Caminho para salvar o DataFrame de vértices com uma nova coluna
                                   contendo os IDs dos componentes.
        id_source_column (str): Nome da coluna de ID da fonte nos dados de entrada.
        id_target_column (str): Nome da coluna de ID do alvo (geralmente prefixado) nos dados de entrada.
        component_id_column_name (str): Nome para a nova coluna que armazenará o ID do componente
                                        atribuído a cada vértice (ID original). (Padrão: "component_id").
    """
    logger.info(f"Starting connected components analysis for data in: {input_parquet_path}")
    start_time = time.time()

    try:
        logger.info("Reading linkage results from Parquet...")
        df_linked_pairs = spark.read.parquet(input_parquet_path)
        linked_pairs_count = df_linked_pairs.count()
        logger.info(f"Read {linked_pairs_count:,} linked pairs.")

        if linked_pairs_count == 0:
            logger.warning("Input DataFrame of linked pairs is empty. Cannot find components.")
            # Optionally, write an empty DataFrame with the expected output schema if required downstream,
            # or simply return. For now, returning.
            return

        # --- Create Vertices DataFrame ---
        # Vertices are all unique IDs present in either the source or target ID columns.
        # GraphFrames expects a DataFrame with at least an 'id' column for vertices.
        df_source_ids = df_linked_pairs.select(F.col(id_source_column).alias("id"))
        df_target_ids = df_linked_pairs.select(F.col(id_target_column).alias("id"))

        # Union all IDs and get distinct ones to form the vertices set
        df_vertices = df_source_ids.union(df_target_ids).distinct()
        vertex_count = df_vertices.count()
        logger.info(f"Created {vertex_count:,} unique vertices (nodes) for the graph.")
        if logger.isEnabledFor(logging.DEBUG):
            df_vertices.show(5, truncate=False)

        if vertex_count == 0: # Should not happen if linked_pairs_count > 0, but as a safeguard
            logger.warning("No vertices could be derived from the linked pairs. Aborting component finding.")
            return

        # --- Create Edges DataFrame ---
        # Edges represent the links between source and target IDs.
        # GraphFrames expects 'src' and 'dst' columns for edges.
        # Keep other relevant columns from linkage (e.g., scores) if they are needed for context or filtering.
        df_edges = df_linked_pairs.select(
            F.col(id_source_column).alias("src"),
            F.col(id_target_column).alias("dst")
            # Add other columns from df_linked_pairs if needed, e.g.:
            # "score", "match_type", "es_score"
        )
        edge_count = df_edges.count() # Should be same as linked_pairs_count
        logger.info(f"Created {edge_count:,} edges (links) for the graph.")
        if logger.isEnabledFor(logging.DEBUG):
            df_edges.show(5, truncate=False)


        # --- Create GraphFrame ---
        logger.info("Creating GraphFrame object...")
        graph = GraphFrame(df_vertices, df_edges)

        # Optional: Cache graph data if performing multiple operations or graph is very large
        # graph.vertices.persist(StorageLevel.MEMORY_AND_DISK_SER) # Choose appropriate storage level
        # graph.edges.persist(StorageLevel.MEMORY_AND_DISK_SER)

        # --- Find Connected Components ---
        # This operation can be computationally intensive for very large graphs.
        logger.info("Calculating connected components... This may take a while for large graphs.")
        # A checkpoint directory must be set for connectedComponents on large graphs
        # to prevent StackOverflowErrors. This is typically set when SparkSession is created
        # or globally via spark.sparkContext.setCheckpointDir().
        # The main() function handles setting this from an argument.
        df_components_result = graph.connectedComponents()
        # df_components_result will contain the original vertex 'id' and a new 'component' column (long type).

        distinct_component_count = df_components_result.select("component").distinct().count()
        logger.info(f"Found {distinct_component_count:,} distinct connected components.")

        # Rename the 'component' column to the desired output name
        df_vertices_with_components = df_components_result.withColumnRenamed("component", component_id_column_name)
        logger.info(f"Output schema for vertices with components: {df_vertices_with_components.schema.simpleString()}")
        if logger.isEnabledFor(logging.DEBUG):
             df_vertices_with_components.show(10, truncate=False)

        # --- Save Results ---
        # Save the vertices DataFrame with their assigned component IDs.
        # This DataFrame maps each original ID to a component ID.
        logger.info(f"Saving vertices with component IDs to: {output_parquet_path}")
        df_vertices_with_components.write.mode("overwrite").parquet(output_parquet_path)
        logger.info("Vertices with component IDs saved successfully.")

        # Optional: Unpersist cached graph data if persisted earlier
        # if graph.vertices.is_cached: graph.vertices.unpersist()
        # if graph.edges.is_cached: graph.edges.unpersist()

    except ImportError:
         # This catch is a fallback if the top-level import check didn't prevent execution
         logger.error("GraphFrames library not found. Please ensure it's installed and "
                      "configured with Spark (e.g., using --packages graphframes:graphframes:your_version).")
         # Re-raise as this is a critical dependency for this function
         raise
    except Exception as e:
        logger.error(f"Connected components analysis failed: {e}", exc_info=True)
        raise # Re-raise the exception to indicate failure

    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info(f"Connected components analysis finished in {elapsed_time:.2f} seconds.")


def main():
    parser = argparse.ArgumentParser(description="Finds connected components in linkage results using GraphFrames.")
    parser.add_argument("--input-path", required=True,
                        help="Path to the input Parquet directory (output of a linkage workflow).")
    parser.add_argument("--output-path", required=True,
                        help="Path to save the output Parquet file (vertices with component IDs).")
    parser.add_argument("--id-source-col", required=True,
                        help="Name of the source ID column in the input linkage data.")
    parser.add_argument("--id-target-col", required=True,
                        help="Name of the (prefixed) target ID column in the input linkage data.")
    parser.add_argument("--component-col-name", default="component_id",
                        help="Name for the output column containing the component ID (default: component_id).")
    parser.add_argument("--spark-app-name", default="ConnectedComponentsFinder",
                        help="Name for the Spark application (default: ConnectedComponentsFinder).")
    parser.add_argument("--checkpoint-dir", default=None,
                        help="Optional Spark checkpoint directory, required for large graphs to prevent "
                             "StackOverflowErrors during connected components calculation.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Logging level (default: INFO).")

    args = parser.parse_args()

    # Setup logging using the project's standard utility
    setup_logging(level=getattr(logging, args.log_level.upper()))

    logger.info(f"Starting script: {Path(__file__).name} with arguments: {args}")

    # --- Path Validation ---
    input_path_obj = Path(args.input_path) # Use new var name for Path object
    output_path_obj = Path(args.output_path) # Use new var name for Path object

    if not input_path_obj.exists() or not input_path_obj.is_dir():
        logger.error(f"Input path '{input_path_obj}' does not exist or is not a directory.")
        raise FileNotFoundError(f"Input path '{input_path_obj}' does not exist or is not a directory.")

    # Ensure parent directory for output exists
    output_path_obj.parent.mkdir(parents=True, exist_ok=True)

    # --- Initialize Spark Session ---
    logger.info("Initializing SparkSession for GraphFrames...")
    try:
        # GraphFrames package configuration is typically done via spark-submit:
        # e.g., --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 (ensure version compatibility)
        spark = SparkSession.builder \
            .appName(args.spark_app_name) \
            .getOrCreate()
        logger.info("SparkSession initialized.")

        # Set checkpoint directory if provided (essential for connectedComponents on large graphs)
        if args.checkpoint_dir:
             # Check if the path exists and is writable, or let Spark handle creation/errors
             checkpoint_path = Path(args.checkpoint_dir)
             try:
                 checkpoint_path.mkdir(parents=True, exist_ok=True) # Ensure it exists
                 spark.sparkContext.setCheckpointDir(str(checkpoint_path.resolve()))
                 logger.info(f"Spark checkpoint directory set to: {spark.sparkContext.getCheckpointDir()}")
             except Exception as e_ckpt:
                 logger.error(f"Failed to create or set Spark checkpoint directory '{checkpoint_path}': {e_ckpt}. "
                              "Connected components might fail for large graphs.", exc_info=True)
                 # Depending on strictness, could raise an error here.
        elif not spark.sparkContext.getCheckpointDir(): # If not set by arg and not set by default-spark.conf
            logger.warning("Spark checkpoint directory is not set. "
                           "The connectedComponents algorithm might fail for large graphs due to StackOverflowError. "
                           "It is highly recommended to set --checkpoint-dir.")


    except Exception as e_spark:
        logger.critical(f"Failed to initialize SparkSession: {e_spark}", exc_info=True)
        raise # Re-raise critical error

    # --- Run Component Finding ---
    try:
        find_connected_components( # Use new function name
            spark=spark,
            input_parquet_path=str(input_path_obj), # Pass string path
            output_parquet_path=str(output_path_obj), # Pass string path
            id_source_column=args.id_source_col,
            id_target_column=args.id_target_col,
            component_id_column_name=args.component_col_name
        )
    except Exception as e_process: # Catch errors from the main processing logic
        logger.critical(f"An error occurred during the connected components finding process: {e_process}", exc_info=True)
    finally:
        logger.info("Stopping SparkSession...")
        if SparkSession.getActiveSession(): # Check if there's an active session to stop
            spark.stop()
        logger.info("SparkSession stopped.")

if __name__ == "__main__":
    main()