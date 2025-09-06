import argparse
import logging
import time
from pathlib import Path

from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame

from cidacsrl_rlp.src.linkage.models import (
    SequentialBlockingWorkflow,
    BlockingPhase
)
from cidacsrl_rlp.src.config.loader import load_sequential_blocking_workflow_config, load_service_config, load_workflow_config
from cidacsrl_rlp.src.linkage.rdd_processing import process_partition_for_phase
from cidacsrl_rlp.src.utils.schema_helpers import define_phase_output_schema, define_workflow_output_schema
from cidacsrl_rlp.src.utils.logging_config import setup_logging
from cidacsrl_rlp.src.utils.utils import sanitize_string
from cidacsrl_rlp.src.utils.spark_utils import create_spark_session
from cidacsrl_rlp.src.utils.io_manager import read_source_data, write_phase_results

# Logger for this module
logger = logging.getLogger(__name__)


def execute_linkage_phase(
    spark: SparkSession,
    df_source_this_phase: DataFrame,
    linkage_config: SequentialBlockingWorkflow,
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
    workflow_config_dict = vars(linkage_config).copy() # Convert dataclass to dict
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
        --config (str): Caminho para o arquivo de configuração YAML principal
            do fluxo de trabalho que contém todos os parâmetros necessários.
        --log-level (str): Nível de logging (padrão: "INFO").

    Example:
        Para executar o fluxo de trabalho a partir do terminal:

        .. code-block:: bash

            python -m src.workflows.sequential_linkage_workflow \\
                --config /path/to/workflow_config.yaml \\
                --log-level DEBUG

        Exemplo do arquivo de configuração YAML `workflow_config.yaml`:

        .. code-block:: yaml

            linkage_config_path: "/path/to/linkage_workflow.yaml"
            es_config_path: "/path/to/elasticsearch_config.yaml"
            spark_config_path: "/path/to/spark_config.yaml"
            output_data_dir: "/path/to/output/data"
            source_data_path: "/path/to/source_data.parquet"
            sample_fraction: 0.1  # opcional - para teste/debug
            sample_seed: 42       # opcional
            spark_checkpoint_base_dir: "/path/to/checkpoints"  # opcional
    """
    parser = argparse.ArgumentParser(description="Executes a Sequential Blocking Linkage Workflow using Elasticsearch and Spark.")
    parser.add_argument("--config", required=True, 
                        help="Path to the main workflow configuration YAML file containing all parameters.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Logging level for the application (default: INFO).")

    args = parser.parse_args()
    
    # Setup logging first
    setup_logging(level=getattr(logging, args.log_level.upper()))
    
    # Load main workflow configuration
    workflow_config = load_workflow_config(args.config)
    
    logger.info(f"Starting CIDACS-RL Engine Sequential Linkage Workflow with config: {args.config}")
    workflow_start_time = time.time()

    # Load configurations
    linkage_config = load_sequential_blocking_workflow_config(workflow_config.linkage_config_path)
    es_settings = load_service_config(workflow_config.es_config_path, service_name="elasticsearch")
    spark_settings = load_service_config(workflow_config.spark_config_path, service_name="spark")

    safe_source_name = sanitize_string(linkage_config.source_table)
    safe_target_name = sanitize_string(linkage_config.target_es_index) # Uses target_es_index

    # Construct a descriptive application name for Spark UI
    app_name_parts = [linkage_config.workflow_name or f"linkage-{safe_source_name}_vs_{safe_target_name}"]
    if workflow_config.current_partition_value:
        app_name_parts.append(f"partition-{sanitize_string(workflow_config.current_partition_value)}")
    app_name = "_".join(app_name_parts)

    spark = create_spark_session(
        app_name=app_name,
        spark_config_path=spark_settings,
    )

    try:        
        
        df_source_for_processing = read_source_data(
            spark=spark,
            source_path=workflow_config.source_data_path,
            sample_fraction=workflow_config.sample_fraction,
            sample_seed=workflow_config.sample_seed
        )
        
        initial_source_count = df_source_for_processing.count()
        logger.info(f"{initial_source_count:,} unique source records submitted for linkage after initial load and deduplication.")

        original_source_schema = df_source_for_processing.schema # Keep original schema for final output construction

        if initial_source_count == 0:
            logger.info("No source records found to process after loading (and sampling, if any). Linkage workflow terminating.")
            return

        # Main loop through linkage phases (blocking phases)
        for phase_config in linkage_config.blocking_phases:
            
            phase_loop_start_time = time.time()
            phase_name = sanitize_string(phase_config.phase_name)
            phase_results_base_path = Path(workflow_config.output_base_path) / f"linkage_{safe_source_name}_vs_{safe_target_name}"

            if not phase_config.enabled:
                logger.info(f"Skipping disabled phase: '{phase_name}'")
                continue

            current_source_count_for_phase = df_source_for_processing.select(linkage_config.id_source_table).count()

            if current_source_count_for_phase == 0:
                logger.info(f"No source records remaining. Stopping workflow before phase '{phase_config.phase_name}'.")
                break # No more records to process

            logger.info(f"--- Starting Phase: '{phase_name}' ({phase_config.phase_description or 'No description'}) ---")
            logger.info(f"Source records for this phase: {current_source_count_for_phase:,}")

            phase_execution_start_time = time.time()
            df_matches = execute_linkage_phase(
                spark, df_source_for_processing, linkage_config, 
                phase_config, es_settings, workflow_config.current_partition_value
            )


            strong_matches_count_this_phase = df_matches.count()
            phase_execution_duration = time.time() - phase_execution_start_time
            logger.info(f"Execution of phase '{phase_name}' completed in {phase_execution_duration:.2f}s.")
            logger.info(f"{strong_matches_count_this_phase:,} strong match candidates (before distinct on source ID) found in the phase '{phase_name}'.")

            if strong_matches_count_this_phase > 0:
                
                # Define output path for this phase's results, partitioned by phase name
                phase_output_path = phase_results_base_path / f"linkage_phase_name={phase_name}"

                # Use define_workflow_output_schema for the schema when writing phase results
                # This schema includes source ID, candidate data (prefixed), all scores, and phase name.
                schema_for_writing_phase_results = define_workflow_output_schema(
                    original_source_schema, linkage_config, phase_config, include_phase_name=True
                )

                cols_to_select_for_writing = [field.name for field in schema_for_writing_phase_results.fields]

                # Ensure only necessary columns according to the defined schema are selected before writing
                df_strong_matches_to_write = df_matches.select(cols_to_select_for_writing)

                # Use write_phase_results for writing
                write_phase_results(spark, df_strong_matches_to_write, phase_output_path, mode="overwrite")
                logger.info(f"Strong match candidates from phase '{phase_name}' written successfully to: {phase_output_path}")

                # Identify unique source IDs that found a strong match in this phase
                ids_matched_in_phase = df_matches.select(linkage_config.id_source_table).distinct()
                num_unique_source_ids_matched = ids_matched_in_phase.count()
                logger.info(f"{num_unique_source_ids_matched:,} unique source IDs found strong matches in phase '{phase_name}'.")


                # Remove matched source IDs from the pool for subsequent phases
                df_source_for_processing = df_source_for_processing.join(
                    ids_matched_in_phase, # DataFrame with distinct source IDs matched in this phase
                    on=linkage_config.id_source_table,
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

            if df_source_for_processing.count() == 0 and phase_config != linkage_config.blocking_phases[-1]:
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