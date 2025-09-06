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



def process_phase(
    spark: SparkSession,
    df_source: DataFrame,
    linkage_config: SequentialBlockingWorkflow,
    phase: BlockingPhase,
    es_settings: Dict[str, Any],
) -> DataFrame:
    """
    Processa uma fase do workflow de linkage.

    Args:
        spark (SparkSession): A sessão Spark ativa.
        df_source (DataFrame): DataFrame contendo os registros da fonte a serem processados.
        linkage_config (SequentialBlockingWorkflow): Configuração geral do workflow.
        phase (BlockingPhase): Configuração específica para esta fase de linkage.
        es_settings (Dict[str, Any]): Configurações de conexão com o Elasticsearch.

    Returns:
        DataFrame: Um DataFrame contendo todos os pares fonte-candidato encontrados e
                   pontuados por esta fase. O DataFrame de saída conterá o `id_source_table`
                   da fonte, dados do candidato (prefixados) e os scores calculados.
                   Retorna um DataFrame vazio com o schema esperado se não houver dados de entrada
                   ou se nenhum candidato for encontrado.
    """
    phase_name = sanitize_string(phase.phase_name)
    logger.info(f"Phase '{phase_name}': starting with {df_source.count():,} records")

    phase_execution_start_time = time.time()

    df_matches = execute_linkage(
        spark, df_source, linkage_config,
        phase, es_settings, 
    )
    
    phase_execution_duration = time.time() - phase_execution_start_time
    logger.info(f"Phase '{phase_name}': execution completed in {phase_execution_duration:.2f}s.")
    logger.info(f"Phase '{phase_name}': {df_matches.count():,} matches found")

    return df_matches

def execute_linkage(
    spark: SparkSession,
    df_source_this_phase: DataFrame,
    linkage_config: SequentialBlockingWorkflow,
    phase: BlockingPhase,
    es_settings: Dict[str, Any]
) -> DataFrame:
    """
    Executa uma única fase de linkage (blocking phase).

    Esta função pega o DataFrame da fonte para a fase atual, aplica as regras de blocking
    e similaridade definidas na `phase` para encontrar e pontuar candidatos
    do Elasticsearch.

    Args:
        spark (SparkSession): A sessão Spark ativa.
        df_source_this_phase (DataFrame): DataFrame contendo os registros da fonte a serem processados nesta fase.
        workflow_config (SequentialBlockingWorkflow): Configuração geral do workflow.
        phase (BlockingPhase): Configuração específica para esta fase de linkage.
        es_settings (Dict[str, Any]): Configurações de conexão com o Elasticsearch.

    Returns:
        DataFrame: Um DataFrame contendo todos os pares fonte-candidato encontrados e
                   pontuados por esta fase. O DataFrame de saída conterá o `id_source_table`
                   da fonte, dados do candidato (prefixados) e os scores calculados.
                   Retorna um DataFrame vazio com o schema esperado se não houver dados de entrada
                   ou se nenhum candidato for encontrado.
    """
    source_df_schema_for_phase = df_source_this_phase.schema

    if df_source_this_phase.rdd.isEmpty():
        logger.warning(f"Source DataFrame for phase '{phase.phase_name}' is empty. Returning an empty DataFrame with defined schema.")
        # Define schema based on the (empty) source DataFrame's schema and phase config
        raw_output_schema = define_phase_output_schema(source_df_schema_for_phase, linkage_config, phase)
        return spark.createDataFrame([], schema=raw_output_schema)

    # Define the output schema for the results of this phase
    raw_output_schema = define_phase_output_schema(source_df_schema_for_phase, linkage_config, phase)

    # Prepare configurations for broadcasting (must be dicts)
    linkage_config_dict = vars(linkage_config).copy() # Convert dataclass to dict
    phase_config_dict = vars(phase).copy()       # Convert dataclass to dict
    # Convert nested ComparisonRule objects to dicts as well
    phase_config_dict['rules'] = [vars(rule) for rule in phase.rules]

    # Broadcast variables to all Spark executors
    workflow_config_dict_bcast = spark.sparkContext.broadcast(linkage_config_dict)
    phase_config_dict_bcast = spark.sparkContext.broadcast(phase_config_dict)
    es_config_dict_bcast = spark.sparkContext.broadcast(es_settings)
    source_schema_bcast = spark.sparkContext.broadcast(source_df_schema_for_phase) # Broadcast source schema

    # process_partition_for_phase is designed to accept these broadcasted dict configurations
    scored_candidates_rdd = df_source_this_phase.rdd.mapPartitions(
        lambda partition_iter: process_partition_for_phase(
            partition_iter,
            workflow_config_dict_bcast,
            phase_config_dict_bcast,
            es_config_dict_bcast,
            source_schema_bcast
        )
    )

    if scored_candidates_rdd.isEmpty():
        logger.warning(f"Scored candidates RDD for phase '{phase.phase_name}' is empty. Returning an empty DataFrame.")
        return spark.createDataFrame([], schema=raw_output_schema)
    else:
        logger.info(f"RDD with scored candidates for phase '{phase.phase_name}' created successfully.")

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
    
    # Setup logging
    setup_logging(level=getattr(logging, args.log_level.upper()))
    
    # Load main workflow configuration
    workflow_config = load_workflow_config(args.config)    
    logger.info(f"Starting CIDACS-RL Workflow")
    workflow_start_time = time.time()

    # Load configurations
    linkage_config = load_sequential_blocking_workflow_config(workflow_config.linkage_config_path)
    es_settings = load_service_config(workflow_config.es_config_path, service_name="elasticsearch")
    spark_settings = load_service_config(workflow_config.spark_config_path, service_name="spark")

    source_name = sanitize_string(linkage_config.source_table)
    target_name = sanitize_string(linkage_config.target_es_index) 

    # Construct a descriptive application name for Spark UI
    app_name_parts = [f"linkage-{source_name}_vs_{target_name}"]
    if workflow_config.sample_fraction and 0 < workflow_config.sample_fraction < 1.0:
        app_name_parts.append(f"sample{int(workflow_config.sample_fraction * 100)}pct")
    if workflow_config.sample_seed is not None:
        app_name_parts.append(f"seed{workflow_config.sample_seed}")
    app_name = "_".join(app_name_parts)

    spark = create_spark_session(
        app_name=app_name,
        spark_config_path=spark_settings,
    )

    try:        
        
        df_source = read_source_data(
            spark=spark,
            source_path=workflow_config.source_data_path,
            sample_fraction=workflow_config.sample_fraction,
            sample_seed=workflow_config.sample_seed
        )
        
        source_count = df_source.count()
        logger.info(f"{source_count:,} source records submitted for linkage")

        original_source_schema = df_source.schema
        output_base_path = Path(workflow_config.output_base_path)
         

        if source_count == 0:
            logger.info("No source records found to process. Linkage workflow terminating.")
            raise ValueError("No source records found")

        # Main loop through blocking phases
        for phase in linkage_config.blocking_phases:
            phase_loop_start_time = time.time()
            phase_name = phase.phase_name           
            phase_threshold = phase.strong_match_score_threshold
            phase_output_path = output_base_path / f"linkage_{source_name}_vs_{target_name}" / f"linkage_phase_name={phase_name}"
            phase_results_schema = define_workflow_output_schema(
                original_source_schema,
                linkage_config,
                phase,
                include_phase_name=True
            )

            if not phase.enabled:
                logger.info(f"Skipping disabled phase: '{phase_name}'")
                continue           

            if df_source.count() == 0:
                logger.info(f"No source records remaining. Stopping workflow before phase '{phase_name}'.")
                break

            df_matches = process_phase(df_source, phase, spark)

            if df_matches.count() == 0:
                logger.info(f"No matches found in phase '{phase_name}'.")
                continue
            else:
                source_matched = df_matches.select(linkage_config.id_source_table).distinct()                
                logger.info(f"Phase '{phase_name}': {source_matched.count():,} source found matches above {phase_threshold}.")

                phase_result_columns = [field.name for field in phase_results_schema.fields]
                df_matches = df_matches.select(phase_result_columns)

                write_phase_results(spark, df_matches, phase_output_path, mode="overwrite")
                logger.info(f"Phase '{phase_name}': results written to {phase_output_path}")

                # Remove matched source IDs from the pool for subsequent phases
                df_source_for_processing = df_source_for_processing.join(
                    source_matched,
                    on=linkage_config.id_source_table,
                    how="left_anti"
                )


                count_remaining_source = df_source_for_processing.count()

                if count_remaining_source == 0 and phase != linkage_config.blocking_phases[-1]:
                    logger.info(f"No source records remaining after phase '{phase.phase_name}'.")
                    logger.info("All source records have been matched. Linkage workflow terminating.")
                    break
                else:
                    logger.info(f"{count_remaining_source:,} source records remaining for next phase.")
                    

            phase_loop_total_duration = time.time() - phase_loop_start_time
            logger.info(f"Phase '{phase.phase_name}': completed in {phase_loop_total_duration:.2f}s")

        logger.info("All linkage phases have been executed.")

    except Exception as e:
        logger.critical(f"Critical error in linkage workflow: {e}", exc_info=True)
        exit(1)
    finally:
        workflow_duration_total = time.time() - workflow_start_time
        mins, secs = divmod(workflow_duration_total, 60)
        logger.info(f"linkage_{source_name}_vs_{target_name} workflow completed in {workflow_duration_total:.2f} seconds "
                    f"(approximately {int(mins):02d}m:{int(secs):02d}s).")


if __name__ == "__main__":
    main()