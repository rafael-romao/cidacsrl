import argparse
import logging
import time
from pathlib import Path

from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame

# Project-specific imports
from cidacsrl_rlp.src.linkage.models import (
    SequentialBlockingWorkflow,
    BlockingPhase
)
from cidacsrl_rlp.src.config.loader import load_sequential_blocking_workflow_config, load_service_config
from cidacsrl_rlp.src.linkage.rdd_processing import process_partition_for_phase
from cidacsrl_rlp.src.utils.schema_helpers import define_phase_output_schema, define_workflow_output_schema
from cidacsrl_rlp.src.utils.logging_config import setup_logging
from cidacsrl_rlp.src.utils.utils import sanitize_string
from cidacsrl_rlp.src.utils.spark_utils import create_spark_session

# Logger for this module
logger = logging.getLogger(__name__)


def execute_linkage_phase(
    spark: SparkSession,
    df_source_this_phase: DataFrame,
    workflow_config: SequentialBlockingWorkflow,
    phase_config: BlockingPhase,
    es_settings: Dict[str, Any],
    runtime_partition_value: Optional[str] = None
) -> DataFrame:
    """
    Executa uma única fase de linkage (blocking phase).

    Esta função pega o DataFrame da fonte para a fase atual, aplica as regras de blocking
    e similaridade definidas na `phase_config` para encontrar e pontuar candidatos
    do Elasticsearch.

    Args:
        spark (SparkSession): A sessão Spark ativa.
        df_source_this_phase (DataFrame): DataFrame contendo os registros da fonte a serem processados nesta fase.
        workflow_config (SequentialBlockingWorkflow): Configuração geral do workflow.
        phase_config (BlockingPhase): Configuração específica para esta fase de linkage.
        es_settings (Dict[str, Any]): Configurações de conexão com o Elasticsearch.
        runtime_partition_value (Optional[str]): Valor da partição de runtime, se aplicável,
                                                 para filtrar dados no Elasticsearch.

    Returns:
        DataFrame: Um DataFrame contendo todos os pares fonte-candidato encontrados e
                   pontuados por esta fase. O DataFrame de saída conterá o `id_source_table`
                   da fonte, dados do candidato (prefixados) e os scores calculados.
                   Retorna um DataFrame vazio com o schema esperado se não houver dados de entrada
                   ou se nenhum candidato for encontrado.
    """
    source_df_schema_for_phase = df_source_this_phase.schema

    if df_source_this_phase.rdd.isEmpty():
        logger.warning(f"Source DataFrame for phase '{phase_config.phase_name}' is empty. Returning an empty DataFrame with defined schema.")
        # Define schema based on the (empty) source DataFrame's schema and phase config
        raw_output_schema = define_phase_output_schema(source_df_schema_for_phase, workflow_config, phase_config)
        return spark.createDataFrame([], schema=raw_output_schema)

    # Define the output schema for the results of this phase
    raw_output_schema = define_phase_output_schema(source_df_schema_for_phase, workflow_config, phase_config)

    # Prepare configurations for broadcasting (must be dicts)
    workflow_config_dict = vars(workflow_config).copy() # Convert dataclass to dict
    phase_config_dict = vars(phase_config).copy()       # Convert dataclass to dict
    # Convert nested ComparisonRule objects to dicts as well
    phase_config_dict['rules'] = [vars(rule) for rule in phase_config.rules]

    # Broadcast variables to all Spark executors
    workflow_config_dict_bcast = spark.sparkContext.broadcast(workflow_config_dict)
    phase_config_dict_bcast = spark.sparkContext.broadcast(phase_config_dict)
    es_config_dict_bcast = spark.sparkContext.broadcast(es_settings)
    source_schema_bcast = spark.sparkContext.broadcast(source_df_schema_for_phase) # Broadcast source schema
    runtime_partition_value_bcast = spark.sparkContext.broadcast(runtime_partition_value)

    # process_partition_for_phase is designed to accept these broadcasted dict configurations
    scored_candidates_rdd = df_source_this_phase.rdd.mapPartitions(
        lambda partition_iter: process_partition_for_phase(
            partition_iter,
            workflow_config_dict_bcast,
            phase_config_dict_bcast,
            es_config_dict_bcast,
            source_schema_bcast,
            runtime_partition_value_bcast
        )
    )

    if scored_candidates_rdd.isEmpty():
        logger.warning(f"Scored candidates RDD for phase '{phase_config.phase_name}' is empty. Returning an empty DataFrame.")
        return spark.createDataFrame([], schema=raw_output_schema)
    else:
        logger.info(f"RDD with scored candidates for phase '{phase_config.phase_name}' created successfully.")

    # Create DataFrame from the RDD of scored candidates
    df_phase_scored_candidates = spark.createDataFrame(scored_candidates_rdd, schema=raw_output_schema)

    return df_phase_scored_candidates


def main():
    """Ponto de entrada principal para o fluxo de trabalho de linkage sequencial.

    Este script orquestra um processo de linkage de dados em múltiplas fases,
    conhecido como "Sequential Blocking". Ele é projetado para ser executado
    a partir da linha de comando e realiza as seguintes operações:

    1.  Carrega as configurações do fluxo de trabalho, do Spark e do Elasticsearch
        a partir de arquivos YAML.
    2.  Inicializa uma sessão Spark com as configurações fornecidas.
    3.  Carrega os dados da fonte (source data) a partir de um caminho especificado
        (ex: diretório Parquet).
    4.  Opcionalmente, aplica amostragem aos dados da fonte para testes ou depuração.
    5.  Itera através de uma série de "fases de bloqueio" (blocking phases)
        definidas no arquivo de configuração do fluxo de trabalho.
    6.  Em cada fase, executa a lógica de linkage para encontrar registros
        candidatos no índice do Elasticsearch e calcular scores de similaridade.
    7.  Salva os resultados (pares encontrados) de cada fase em um diretório de saída
        intermediário.
    8.  Registros da fonte que encontram uma correspondência forte em uma fase são
        removidos do conjunto de dados para as fases subsequentes, otimizando o processo.
    9.  Gerencia o particionamento de dados para processamento distribuído e
        consultas ao Elasticsearch.

    Args:
        --workflow-config-path (str): Caminho para o arquivo de configuração YAML
            do fluxo de trabalho de linkage sequencial.
        --es-config-path (str): Caminho para o arquivo de configuração YAML da
            conexão com o Elasticsearch.
        --spark-config-path (str): Caminho para o arquivo de configuração YAML do Spark.
        --output-data-dir (str): Diretório base para salvar todos os resultados
            das fases de linkage.
        --source-data-path (str): Caminho para os dados da fonte (ex: diretório Parquet).
        --log-level (str): Nível de logging (padrão: "INFO").
        --sample-fraction (float, opcional): Fração dos dados da fonte para
            amostragem (entre 0.0 e 1.0).
        --sample-seed (int, opcional): Semente para a amostragem (padrão: 42).
        --spark-checkpoint-base-dir (str, opcional): Diretório base para os
            checkpoints do Spark, para otimizar a performance em fluxos longos.

    Example:
        Para executar o fluxo de trabalho a partir do terminal:

        .. code-block:: bash

            python -m src.workflows.sequential_linkage_workflow \\
                --workflow-config-path /path/to/your_workflow.yaml \\
                --es-config-path /path/to/your_es_config.yaml \\
                --spark-config-path /path/to/your_spark_config.yaml \\
                --output-data-dir /path/to/output/data \\
                --source-data-path /path/to/source_data.parquet \\
                --log-level DEBUG
    """
    parser = argparse.ArgumentParser(description="Executes a Sequential Blocking Linkage Workflow using Elasticsearch and Spark.")
    parser.add_argument("--workflow-config-path", required=True, help="Path to the Sequential Blocking Workflow YAML configuration file.")
    parser.add_argument("--es-config-path", required=True, help="Path to the Elasticsearch connection configuration YAML file.")
    parser.add_argument("--spark-config-path", required=True, help="Path to the Spark configuration YAML file.")
    parser.add_argument("--output-data-dir", required=True, help="Base directory for saving all linkage phase outputs and final results.")
    parser.add_argument("--source-data-path", required=True, help="Path to the source data (e.g., Parquet directory).")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Logging level for the application (default: INFO).")
    parser.add_argument("--sample-fraction", type=float, default=None,
                        help="Fraction of source data to sample for processing (0.0 to 1.0). For testing/debugging.")
    parser.add_argument("--sample-seed", type=int, default=42,
                        help="Seed for sampling if --sample-fraction is used (default: 42).")
    parser.add_argument("--spark-checkpoint-base-dir", type=str, default=None,
                        help="Base directory for Spark checkpoints. If provided, some DataFrames might be checkpointed. "
                             "A subdirectory specific to this job will be created here.")

    args = parser.parse_args()
    setup_logging(level=getattr(logging, args.log_level.upper()))
    logger.info(f"Starting CIDACS-RL Engine Sequential Linkage Workflow with workflow config: {args.workflow_config_path}")
    workflow_start_time = time.time()

    # Load configurations
    workflow_config = load_sequential_blocking_workflow_config(args.workflow_config_path)
    es_settings = load_service_config(args.es_config_path, service_name="elasticsearch")
    spark_settings = load_service_config(args.spark_config_path, service_name="spark")

    safe_source_name = sanitize_string(workflow_config.source_table)
    safe_target_name = sanitize_string(workflow_config.target_es_index) # Uses target_es_index

    # Construct a descriptive application name for Spark UI
    app_name_parts = [workflow_config.workflow_name or f"linkage-{safe_source_name}_vs_{safe_target_name}"]
    if args.current_partition_value:
        app_name_parts.append(f"partition-{sanitize_string(args.current_partition_value)}")
    app_name = "_".join(app_name_parts)

    spark = create_spark_session(
        app_name=app_name,
        spark_config_path=spark_settings,
    )

    try:
        output_data_dir_path = Path(args.output_data_dir)
        workflow_prefix_for_dir = ""
        if workflow_config.workflow_name:
            workflow_prefix_for_dir = sanitize_string(workflow_config.workflow_name) + "_"
        # Base name for directory holding all intermediate phase results for this workflow
        intermediate_base_dir_name = f"{workflow_prefix_for_dir}{safe_source_name}_vs_{safe_target_name}_intermediate_phases"
        intermediate_results_base_path = output_data_dir_path / intermediate_base_dir_name
        intermediate_results_base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Intermediate results from each linkage phase will be saved under: {intermediate_results_base_path}")

        # Load source data
        source_data_path_obj = Path(args.source_data_path)
        source_data_path_to_load = source_data_path_obj

        
        if not source_data_path_obj.exists() or not source_data_path_obj.is_dir():
            logger.error(f"Source data path does not exist or is not a directory: {source_data_path_obj}")
            raise FileNotFoundError(f"Source data path not found: {source_data_path_obj}")

        try:
            df_source_for_processing = spark.read.parquet(str(source_data_path_to_load))
            # Remove duplicates from source based on its ID to avoid redundant processing
            df_source_for_processing = df_source_for_processing.dropDuplicates([workflow_config.id_source_table])
            logger.info(f"Source DataFrame loaded successfully from '{source_data_path_to_load}'.")
            logger.info(f"Source DataFrame initially has {df_source_for_processing.rdd.getNumPartitions()} partitions.")
            # Consider making repartitioning configurable via spark_settings or workflow_config
            num_shuffle_partitions = 50
            df_source_for_processing = df_source_for_processing.repartition(num_shuffle_partitions)
            logger.info(f"Source DataFrame repartitioned to {df_source_for_processing.rdd.getNumPartitions()} partitions.")
        except Exception as e_read_source:
            logger.error(f"Failed to read source data from '{source_data_path_to_load}'. "
                         f"Verify path and format. Error: {e_read_source}", exc_info=True)
            raise

        # Apply sampling if configured
        if args.sample_fraction and 0.0 < args.sample_fraction <= 1.0:
            df_source_for_processing = df_source_for_processing.sample(
                withReplacement=False, fraction=args.sample_fraction, seed=args.sample_seed
            )
            logger.info(f"Sampled source DataFrame with fraction {args.sample_fraction} and seed {args.sample_seed}.")

        initial_source_count = df_source_for_processing.count()
        logger.info(f"{initial_source_count:,} unique source records submitted for linkage after initial load and deduplication.")

        original_source_schema = df_source_for_processing.schema # Keep original schema for final output construction

        if initial_source_count == 0:
            logger.info("No source records found to process after loading (and sampling, if any). Linkage workflow terminating.")
            return

        # Main loop through linkage phases (blocking phases)
        for phase_config in workflow_config.blocking_phases:
            phase_loop_start_time = time.time()

            if not phase_config.enabled:
                logger.info(f"Skipping disabled phase: '{phase_config.phase_name}'")
                continue

            current_source_count_for_phase = df_source_for_processing.select(workflow_config.id_source_table).count()

            if current_source_count_for_phase == 0:
                logger.info(f"No source records remaining. Stopping workflow before phase '{phase_config.phase_name}'.")
                break # No more records to process

            logger.info(f"--- Starting Phase: '{phase_config.phase_name}' ({phase_config.phase_description or 'No description'}) ---")
            logger.info(f"Source records for this phase: {current_source_count_for_phase:,}")

            phase_execution_start_time = time.time()
            df_strong_matches_this_phase = execute_linkage_phase(
                spark,
                df_source_for_processing, # Current set of source records to link
                workflow_config,
                phase_config,
                es_settings,
                args.current_partition_value
            )


            strong_matches_count_this_phase = df_strong_matches_this_phase.count()
            phase_execution_duration = time.time() - phase_execution_start_time
            logger.info(f"Execution of phase '{phase_config.phase_name}' completed in {phase_execution_duration:.2f}s.")
            logger.info(f"{strong_matches_count_this_phase:,} strong match candidates (before distinct on source ID) found in the phase '{phase_config.phase_name}'.")

            if strong_matches_count_this_phase > 0:
                safe_phase_name_for_path_component = sanitize_string(phase_config.phase_name)
                # Define output path for this phase's results, partitioned by phase name
                phase_output_path = intermediate_results_base_path / f"linkage_phase_name={safe_phase_name_for_path_component}"

                # Use define_workflow_output_schema for the schema when writing phase results
                # This schema includes source ID, candidate data (prefixed), all scores, and phase name.
                schema_for_writing_phase_results = define_workflow_output_schema(
                    original_source_schema, workflow_config, phase_config, include_phase_name=True
                )

                cols_to_select_for_writing = [field.name for field in schema_for_writing_phase_results.fields]

                # Ensure only necessary columns according to the defined schema are selected before writing
                df_strong_matches_to_write = df_strong_matches_this_phase.select(cols_to_select_for_writing)

                df_strong_matches_to_write.write.format("parquet").mode("overwrite").save(str(phase_output_path))
                logger.info(f"Strong match candidates from phase '{phase_config.phase_name}' written successfully to: {phase_output_path}")

                # Identify unique source IDs that found a strong match in this phase
                ids_matched_in_phase = df_strong_matches_this_phase.select(workflow_config.id_source_table).distinct()
                num_unique_source_ids_matched = ids_matched_in_phase.count()
                logger.info(f"{num_unique_source_ids_matched:,} unique source IDs found strong matches in phase '{phase_config.phase_name}'.")


                # Remove matched source IDs from the pool for subsequent phases
                df_source_for_processing = df_source_for_processing.join(
                    ids_matched_in_phase, # DataFrame with distinct source IDs matched in this phase
                    on=workflow_config.id_source_table,
                    how="left_anti" # Keep only records from df_source_for_processing that are NOT in ids_matched_in_phase
                )


                count_remaining_source_ids = df_source_for_processing.count() # This count is on the RDD after join
                if count_remaining_source_ids > 0:
                    logger.info(f"{count_remaining_source_ids:,} source records remaining for next phase.")
                else:
                    logger.info(f"No source records remaining after phase '{phase_config.phase_name}'. Workflow will terminate if no more phases.")

            else: # No strong matches found in this phase
                logger.info(f"No strong matches found in phase '{phase_config.phase_name}'.")

            phase_loop_total_duration = time.time() - phase_loop_start_time
            logger.info(f"--- Phase '{phase_config.phase_name}' completed (total loop time: {phase_loop_total_duration:.2f}s) ---")

            if df_source_for_processing.count() == 0 and phase_config != workflow_config.blocking_phases[-1]:
                logger.info("All source records have been matched. Terminating linkage phases early.")
                break


        logger.info("All linkage phases have been executed.")


    except Exception as e:
        logger.critical(f"Critical error in linkage workflow: {e}", exc_info=True)
        exit(1) # Exit with error code
    finally:
        workflow_duration_total = time.time() - workflow_start_time
        mins, secs = divmod(workflow_duration_total, 60)
        logger.info(f"CIDACS-RL Engine Linkage Workflow completed in {workflow_duration_total:.2f} seconds "
                    f"(approximately {int(mins):02d}m:{int(secs):02d}s).")


if __name__ == "__main__":
    main()