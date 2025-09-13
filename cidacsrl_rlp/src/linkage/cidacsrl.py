from cidacsrl_rlp.src.linkage.rdd_processing import process_partition_for_phase
from cidacsrl_rlp.src.utils.schema_helpers import define_phase_output_schema, define_workflow_output_schema
from cidacsrl_rlp.src.utils.utils import sanitize_string
from cidacsrl_rlp.src.utils.log import trace_execution
from typing import Optional, Dict, Any

# Project-specific imports
from cidacsrl_rlp.src.linkage.models import (
    SequentialBlockingWorkflow,
    BlockingPhase
)

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
import time
import re

def process_phase(
    spark: SparkSession,
    df_source: DataFrame,
    linkage_config: SequentialBlockingWorkflow,
    phase: BlockingPhase,
    es_settings: Dict[str, Any],
    logger = None,
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
        spark,
        df_source,
        linkage_config,
        phase,
        es_settings, 
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
    es_settings: Dict[str, Any],
    logger = None
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

def cidacsrl(
    df,
    source_name,
    target_name,
    linkage_config,
    spark,
    es_settings,
    output_base_path,
    partition_info: dict = dict(),
    log_file: str = None,
    logger=None,
    debug: bool = True
):
    if not logger:
        import logging
        logger = logging.getLogger(__name__)

    # Backup do schema original das colunas
    original_source_schema = df.schema

    # Define o nome do linkage
    linkage_name = f"linkage-{source_name}_vs_{target_name}{'-' + str(partition_info['partition']) if partition_info.get('partition', '*') != '*' else ''}"

    # Cria um ID para a execução atual
    execution_id = datetime.now().strftime("%Y%m%d%H%M")

    if log_file:
        # Registra o início do linkage
        trace_execution(process_name=linkage_name, operation="START", caminho_csv=log_file, execution_id=execution_id)

    try:
        # Main loop through blocking phases
        for i, phase in enumerate(linkage_config.blocking_phases):
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
            logger.info(f"[#{i + 1}/{len(linkage_config.blocking_phases)}] Executing phase: '{phase.phase_name}'")

            if df_source_for_processing.isEmpty():
                logger.info(f"No source records remaining. Stopping workflow before phase '{phase_name}'.")
                break

            df_matches = process_phase(
                spark=spark,
                df_source=df_source_for_processing,
                linkage_config=linkage_config,
                phase=phase,
                es_settings=es_settings
            )

            if df_matches.isEmpty():
                logger.info(f"No matches found in phase '{phase_name}'.")
                continue
            else:
                phase_result_columns = [field.name for field in phase_results_schema.fields]
                df_matches = df_matches.select(phase_result_columns)

                # Colunas de auditoria
                df_matches = df_matches.withColumns({
                    "_LINKED_FROM": F.lit(phase_name),
                    "_DT_LINKAGE": F.from_utc_timestamp(F.current_timestamp(), "America/Sao_Paulo"),
                })

                ## Escrita dos dados
                # Define o path de escrita do resultado do linkage. Particiona a escrita caso o argumento seja informado nas configurações.
                if partition_info.get('partition', '*') == "*":
                    write_path = str(phase_output_path)
                else:
                    # Dropa a coluna de partição dos dados caso ela tenha sido mantida pois no linkage partitionado ela terá apenas o valor da partição em questão.
                    df_matches = df_matches.drop(partition_info.get('partition_by'))
                    write_path = f"{re.sub("\/$", "", str(phase_output_path))}/{partition_info.get('partition_by')}={partition_info.get('partition')}"

                logger.info(f"[Phase '{phase_name}']: Escrevendo dados em `{write_path}`...")
                # write_phase_results(spark, df_matches, phase_output_path, mode="overwrite")
                df_matches.write.format("parquet").mode("overwrite").save(write_path)
                logger.info(f"Candidates from phase '{phase_name}' written successfully to: {write_path}")

                # Identify unique source IDs that found a strong match in this phase
                source_matched = spark.read.parquet(write_path).select(linkage_config.id_source_table).distinct()

                if debug:
                    logger.info(f"[Phase '{phase_name}']: {source_matched.count()} source found matches above {phase_threshold} and writed in `{phase_output_path}`")
                else:
                    logger.info(f"[Phase '{phase_name}']: results written to `{phase_output_path}`")

                # Remove matched source IDs from the pool for subsequent phases
                df_source_for_processing = df_source_for_processing.join(
                    source_matched,
                    on=linkage_config.id_source_table,
                    how="left_anti"
                )

                # Checkpoint aqui do df_source_for_processing?

                count_remaining_source = df_source_for_processing.count()

                if count_remaining_source == 0 and phase != linkage_config.blocking_phases[-1]:
                    logger.info(f"No source records remaining after phase '{phase.phase_name}'.")
                    logger.info("All source records have been matched. Linkage workflow terminating.")
                    break
                else:
                    logger.info(f"{count_remaining_source:,} source records remaining for next phase.")

            phase_loop_total_duration = time.time() - phase_loop_start_time
            logger.info(f"Phase '{phase.phase_name}': completed in {phase_loop_total_duration:.2f}s")

        logger.info(f"Registrando o término do Linkage em `{log_file}`")
        if log_file:
            # Registra o término do linkage
            trace_execution(process_name=linkage_name, operation="END", caminho_csv=log_file, execution_id=execution_id)
    except Exception as exc:
        # Registra o evento de erro do processo de linkage nos logs desse linkage
        trace_execution(process_name=linkage_name, operation="ERROR", caminho_csv=log_file, execution_id=execution_id)
        # Lança a exceção para o fluxo que chamou essa função 
        raise Exception(exc)

