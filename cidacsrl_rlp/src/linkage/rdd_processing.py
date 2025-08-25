# src/linkage/rdd_processing.py

import logging
from typing import Dict, Any, Iterator, List, Optional

from pyspark.sql import Row  # For typing the partition iterator
from pyspark.sql.types import StructType  # For typing the broadcasted schema
from pyspark.broadcast import Broadcast  # For typing broadcast variables

# --- Project Module Imports ---
from cidacsrl_rlp.src.es.client import get_es_client
from cidacsrl_rlp.src.es.query_builder import create_es_query_for_phase
from cidacsrl_rlp.src.es.response_parser import extract_hits_from_es_response
from cidacsrl_rlp.src.linkage.scoring_engine import calculate_pair_scores_and_similarities
# Constants for field names, imported from schema_helpers
from cidacsrl_rlp.src.utils.schema_helpers import CANDIDATE_ES_DOC_ID_FIELD, ES_HIT_SCORE_FIELD, LINKAGE_PHASE_NAME_FIELD

logger = logging.getLogger(__name__)


def _prepare_candidate_data_for_scoring(
    es_hit_id: str,
    es_hit_source_fields: Dict[str, Any],
    workflow_config_dict: Dict[str, Any],
    phase_rules_dicts: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Prepara o dicionário de dados do candidato com nomes de campos prefixados,
    pronto para o processo de scoring.

    Args:
        es_hit_id (str): O ID do documento Elasticsearch do candidato.
        es_hit_source_fields (Dict[str, Any]): O conteúdo do campo `_source` do hit do Elasticsearch.
        workflow_config_dict (Dict[str, Any]): Configurações do workflow, contendo prefixos e nomes de campos ID.
        phase_rules_dicts (List[Dict[str, Any]]): Lista de regras de comparação para a fase atual,
                                                  usada para determinar quais campos do candidato extrair.

    Returns:
        Dict[str, Any]: Um dicionário com os dados do candidato, incluindo o ID do documento ES e
                        campos de dados relevantes prefixados.
    """
    candidate_data_output: Dict[str, Any] = {}
    # Use the candidate prefix defined in the workflow configuration
    candidate_prefix = workflow_config_dict.get('_candidate_prefix', 'candidate_')

    # Store the Elasticsearch document ID using the constant
    candidate_data_output[CANDIDATE_ES_DOC_ID_FIELD] = es_hit_id

    # Get the application-level ID of the candidate
    app_level_id_field_in_es_source = workflow_config_dict['id_target_table']
    prefixed_app_level_id_df_col_name = workflow_config_dict['prefixed_id_target_table']

    if app_level_id_field_in_es_source in es_hit_source_fields:
        candidate_data_output[prefixed_app_level_id_df_col_name] = \
            es_hit_source_fields[app_level_id_field_in_es_source]
    else:
        candidate_data_output[prefixed_app_level_id_df_col_name] = None
        logger.debug(f"Candidate's application ID field '{app_level_id_field_in_es_source}' "
                       f"(expected as '{prefixed_app_level_id_df_col_name}') not found in ES doc _source for _id '{es_hit_id}'. "
                       f"Setting to None.")

    # Extract other relevant candidate data fields as defined by the phase rules
    for rule_dict in phase_rules_dicts:
        original_es_field_name = rule_dict['target_column']
        # Dynamically construct the prefixed column name for candidate data in the DataFrame
        prefixed_df_col_name = f"{candidate_prefix}{original_es_field_name}"

        # Avoid reprocessing the application ID if it's also listed explicitly in rules
        if prefixed_df_col_name == prefixed_app_level_id_df_col_name:
            continue

        if original_es_field_name in es_hit_source_fields:
            candidate_data_output[prefixed_df_col_name] = es_hit_source_fields[original_es_field_name]
        else:
            candidate_data_output[prefixed_df_col_name] = None
            # Log if a field defined in rules is missing from ES source (could be intentional or an issue)
            logger.debug(f"Candidate data field '{original_es_field_name}' not found in ES doc _source for _id '{es_hit_id}'. "
                           f"Setting prefixed field '{prefixed_df_col_name}' to None.")

    return candidate_data_output


def _process_msearch_batch_responses(
    es_msearch_responses: List[Dict[str, Any]],
    source_rows_in_batch: List[Dict[str, Any]],
    workflow_config_dict: Dict[str, Any],
    phase_config_dict: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """
    Processa os resultados de um lote de `msearch` do Elasticsearch, correlaciona-os
    com os registros da fonte original do lote, e chama o motor de scoring para cada
    par fonte-candidato encontrado. Produz um iterador de dicionários, onde cada
    dicionário representa um par pontuado que atende ao limiar da fase.

    Args:
        es_msearch_responses (List[Dict[str, Any]]): Lista de respostas individuais da chamada `msearch`.
        source_rows_in_batch (List[Dict[str, Any]]): Lista dos registros da fonte que correspondem às queries `msearch`.
        workflow_config_dict (Dict[str, Any]): Configurações do workflow.
        phase_config_dict (Dict[str, Any]): Configurações da fase de linkage atual.

    Yields:
        Iterator[Dict[str, Any]]: Dicionários contendo dados do par fonte-candidato e seus scores,
                                   se o score atingir o limiar da fase.
    """
    num_expected_responses = len(source_rows_in_batch)
    if len(es_msearch_responses) != num_expected_responses:
        logger.warning(
            f"Mismatch in the number of msearch responses. Expected: {num_expected_responses}, "
            f"Received: {len(es_msearch_responses)}. The batch might be processed partially or skipped."
        )
        # Depending on strictness, one might choose to `return iter([])` here

    phase_rules_dicts = phase_config_dict['rules']
    id_source_table_field_name = workflow_config_dict['id_source_table']
    current_phase_name = phase_config_dict.get('phase_name', 'UNKNOWN_PHASE')

    strong_match_score_threshold_config = phase_config_dict.get('strong_match_score_threshold')

    valid_threshold_present = False
    parsed_threshold = 0.0 # Default threshold if not valid or not present

    if strong_match_score_threshold_config is not None:
        try:
            parsed_threshold = float(strong_match_score_threshold_config)
            valid_threshold_present = True
            logger.debug(f"Using strong_match_score_threshold: {parsed_threshold} for phase '{current_phase_name}'.")
        except (ValueError, TypeError):
            logger.warning(
                f"Value of strong_match_score_threshold ('{strong_match_score_threshold_config}') "
                f"for phase '{current_phase_name}' is not a valid number. "
                f"No score threshold filtering will be applied based on this value in this phase."
            )
    else:
        logger.info(
            f"strong_match_score_threshold not defined or is None for phase '{current_phase_name}'. "
            f"All scored pairs will be yielded from this phase (no threshold filter)."
        )

    for i, single_es_response in enumerate(es_msearch_responses):
        if i >= num_expected_responses: # Should not happen if lengths match, but as a safeguard
            logger.warning(f"Processing more msearch responses ({i+1}) than source rows in batch ({num_expected_responses}). Stopping.")
            break

        source_row_dict = source_rows_in_batch[i]
        source_id_for_log = source_row_dict.get(id_source_table_field_name, 'UNKNOWN_SOURCE_ID')

        candidate_hits_from_es = extract_hits_from_es_response(single_es_response, source_id_for_log)

        if not candidate_hits_from_es:
            logger.debug(f"No Elasticsearch candidates found for source_id '{source_id_for_log}' in phase '{current_phase_name}'.")
            continue

        for es_hit_dict in candidate_hits_from_es:
            es_hit_id = es_hit_dict['id']
            es_hit_elasticsearch_score = es_hit_dict['score'] # Raw ES score
            candidate_es_source_fields = es_hit_dict['source']

            candidate_data_dict_prefixed = _prepare_candidate_data_for_scoring(
                es_hit_id,
                candidate_es_source_fields,
                workflow_config_dict,
                phase_rules_dicts
            )

            scores_and_sim_dict = calculate_pair_scores_and_similarities(
                source_row_dict=source_row_dict,
                candidate_data_dict_prefixed=candidate_data_dict_prefixed,
                phase_rules_dicts=phase_rules_dicts, # Pass the rules for scoring
                workflow_config_dict=workflow_config_dict,
            )

            # The key for composite score from calculate_pair_scores_and_similarities is 'match_score'
            current_composite_score = scores_and_sim_dict.get("match_score", -1.0) # Default to a low score if missing

            # This function constructs the final dictionary to be yielded
            def build_result_dict():
                # Start with the source ID
                result = {id_source_table_field_name: source_row_dict.get(id_source_table_field_name)}
                # Add candidate data (which includes prefixed candidate ID and _candidate_elasticsearch_document_id)
                result.update(candidate_data_dict_prefixed)
                # Add the raw Elasticsearch hit score
                result[ES_HIT_SCORE_FIELD] = es_hit_elasticsearch_score
                # Add all scores from scoring engine (match_score and individual sim_scores)
                result.update(scores_and_sim_dict)
                # Add the linkage phase name
                result[LINKAGE_PHASE_NAME_FIELD] = current_phase_name
                return result

            # Yield the result if it meets the threshold or if no valid threshold is applied
            if valid_threshold_present:
                if current_composite_score >= parsed_threshold:
                    yield build_result_dict()
            else: # No valid threshold, so yield all scored pairs
                yield build_result_dict()


def process_partition_for_phase(
    partition_iter: Iterator[Row],
    workflow_config_dict_bcast: Broadcast[Dict[str, Any]],
    phase_config_dict_bcast: Broadcast[Dict[str, Any]],
    es_config_dict_bcast: Broadcast[Dict[str, Any]],
) -> Iterator[Dict[str, Any]]:
    """
    Processa uma partição de dados da fonte para uma dada fase de linkage (blocking phase).
    Para cada registro da fonte na partição, constrói e executa uma query `msearch` no Elasticsearch
    para encontrar registros candidatos. Em seguida, calcula scores de similaridade para os pares
    fonte-candidato e produz os resultados.

    Args:
        partition_iter (Iterator[Row]): Iterador sobre as linhas (`Row`) da partição Spark.
        workflow_config_dict_bcast (Broadcast[Dict[str, Any]]): Configurações do workflow (broadcast).
        phase_config_dict_bcast (Broadcast[Dict[str, Any]]): Configurações da fase de linkage (broadcast).
        es_config_dict_bcast (Broadcast[Dict[str, Any]]): Configurações de conexão do Elasticsearch (broadcast).

    Yields:
        Iterator[Dict[str, Any]]: Dicionários representando os pares fonte-candidato pontuados que
                                   atendem aos critérios da fase.
    """
    workflow_cfg = workflow_config_dict_bcast.value
    phase_cfg = phase_config_dict_bcast.value
    es_cfg = es_config_dict_bcast.value

    partition_id_for_log = f"phase_{phase_cfg.get('phase_name', 'UNKNOWN_PHASE')}"
    logger.info(f"Processing partition for {partition_id_for_log} started.")

    es_client = get_es_client(es_cfg)
    if not es_client:
        logger.error(f"Failed to get Elasticsearch client for {partition_id_for_log}. Skipping partition.")
        return iter([]) # Return an empty iterator if ES client fails

    msearch_operations_body: List[Dict[str, Any]] = [] # Stores msearch header/query pairs
    source_rows_in_batch: List[Dict[str, Any]] = []    # Stores corresponding source rows for correlation

    # msearch_batch_size: How many search requests to bundle in one msearch call.
    # Max is often limited by ES or HTTP request size limits.
    user_batch_size = es_cfg.get("msearch_batch_size", 100) # Configurable batch size
    msearch_batch_size = min(user_batch_size, 500) # Cap at a reasonable maximum
    logger.debug(f"Using msearch_batch_size: {msearch_batch_size} for {partition_id_for_log}")

    # Determine which fields to fetch from Elasticsearch for candidates
    target_es_fields_to_fetch_set = {workflow_cfg['id_target_table']} # Always fetch candidate ID
    for rule in phase_cfg['rules']:
        target_es_fields_to_fetch_set.add(rule['target_column'])
    target_es_fields_to_fetch = list(target_es_fields_to_fetch_set)

    if not target_es_fields_to_fetch:
        logger.warning(f"No target ES fields to fetch (including ID) for {partition_id_for_log}. This is unusual. Skipping partition.")
        return iter([])    


    # Iterate over source rows in the partition
    for source_row_spark_obj in partition_iter:
        source_row_dict = source_row_spark_obj.asDict()
        source_id_val = source_row_dict.get(workflow_cfg['id_source_table'])

        if source_id_val is None: # Should not happen with clean data
            logger.warning(f"Source row encountered with None ID ('{workflow_cfg['id_source_table']}'). Skipping row: {source_row_dict}")
            continue

        es_query_body = create_es_query_for_phase(
            source_row_dict=source_row_dict,
            rules_dicts=phase_cfg['rules'],
            target_es_fields_to_fetch=target_es_fields_to_fetch,
            candidate_limit=phase_cfg['candidate_limit']
        )

        if es_query_body:
            # Add msearch header (index to target)
            msearch_operations_body.append({"index": workflow_cfg['target_es_index']})
            # Add msearch query body
            msearch_operations_body.append(es_query_body)
            source_rows_in_batch.append(source_row_dict)

        # If batch is full, execute msearch and process results
        if len(source_rows_in_batch) >= msearch_batch_size:
            try:
                msearch_result = es_client.msearch(body=msearch_operations_body, index=workflow_cfg['target_es_index'])
                yield from _process_msearch_batch_responses(
                    msearch_result.get('responses', []),
                    source_rows_in_batch,
                    workflow_cfg,
                    phase_cfg
                )
            except Exception as e_batch:
                # Log error and potentially skip batch, or re-raise to fail the partition/job
                # depending on error handling policy.
                logger.error(f"Error processing msearch batch in {partition_id_for_log}: {e_batch}", exc_info=True)
                # Re-raising could stop the Spark job: raise e_batch
            finally: # Clear batch for next set of operations
                msearch_operations_body = []
                source_rows_in_batch = []

    # Process any remaining records in the last batch
    if source_rows_in_batch:
        try:
            msearch_result = es_client.msearch(body=msearch_operations_body, index=workflow_cfg['target_es_index'])
            yield from _process_msearch_batch_responses(
                msearch_result.get('responses', []),
                source_rows_in_batch,
                workflow_cfg,
                phase_cfg
            )
        except Exception as e_final_batch:
            logger.error(f"Error processing final msearch batch in {partition_id_for_log}: {e_final_batch}", exc_info=True)
            # Re-raising could stop the Spark job: raise e_final_batch
    logger.info(f"Processing partition for {partition_id_for_log} finished.")