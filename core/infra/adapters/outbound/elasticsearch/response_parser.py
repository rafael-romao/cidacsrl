# src/es/response_parser.py

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

def extract_hits_from_es_response(
    single_es_response: Dict[str, Any],
    source_record_id_for_log: Any  # ID of the source record, for contextual logs
) -> List[Dict[str, Any]]:
    """
    Extrai informações relevantes dos "hits" de um único objeto de resposta
    de busca do Elasticsearch.

    Cada hit é retornado como um dicionário com as chaves 'id', 'score', e 'source'.

    Args:
        single_es_response (Dict[str, Any]): O dicionário representando uma resposta de busca do ES
                                             (geralmente um item da lista 'responses' de um resultado msearch).
        source_record_id_for_log (Any): O ID do registro da fonte que originou esta busca no ES,
                                        usado para fins de logging contextual.

    Returns:
        List[Dict[str, Any]]: Uma lista de dicionários. Cada dicionário representa um hit e contém:
            - 'id': O `_id` (string) do documento do hit.
            - 'score': O `_score` (float) do hit (score de relevância do ES).
            - 'source': O dicionário `_source` do documento do hit.
        Retorna uma lista vazia se não houver hits, a resposta for inválida,
        ou ocorrer um erro durante o processamento dos hits.
    """
    extracted_hits_list: List[Dict[str, Any]] = []

    if not single_es_response or not isinstance(single_es_response, dict):
        logger.warning(
            f"Provided Elasticsearch response is invalid (None or not a dict) "
            f"for source record ID '{source_record_id_for_log}'. Response snippet: {str(single_es_response)[:500]}..."
        )
        return extracted_hits_list

    if "hits" not in single_es_response:
        # Check if it's an ES error response, which might not have 'hits' but 'error'
        if "error" in single_es_response:
            logger.warning(
                f"Elasticsearch response contains an error for source record ID '{source_record_id_for_log}'. "
                f"Error details: {str(single_es_response['error'])[:500]}..."
            )
        else:
            logger.warning(
                f"Elasticsearch response does not contain the 'hits' key for source record ID "
                f"'{source_record_id_for_log}'. Response snippet: {str(single_es_response)[:500]}..."
            )
        return extracted_hits_list

    hits_container = single_es_response["hits"]
    if not isinstance(hits_container, dict) or "hits" not in hits_container:
        logger.warning(
            f"The 'hits' section of the Elasticsearch response is not a dictionary or lacks the inner 'hits' list "
            f"for source record ID '{source_record_id_for_log}'. Hits container snippet: {str(hits_container)[:500]}..."
        )
        return extracted_hits_list

    actual_hits_data_list = hits_container.get("hits")
    if not isinstance(actual_hits_data_list, list):
        logger.warning(
            f"The 'hits.hits' field in the Elasticsearch response is not a list "
            f"for source record ID '{source_record_id_for_log}'. Actual hits data snippet: {str(actual_hits_data_list)[:500]}..."
        )
        return extracted_hits_list

    if not actual_hits_data_list:
        total_hits_info = hits_container.get("total", 0) # ES can return total as int or dict
        total_hits_value = 0
        if isinstance(total_hits_info, dict):
            total_hits_value = total_hits_info.get("value", 0)
        elif isinstance(total_hits_info, (int, float)):
            total_hits_value = int(total_hits_info)

        logger.debug(
            f"No hits found in the 'hits.hits' list for source record ID '{source_record_id_for_log}' "
            f"(total hits reported by ES: {total_hits_value})."
        )
        return extracted_hits_list

    for hit_data_dict in actual_hits_data_list:
        if not isinstance(hit_data_dict, dict):
            logger.warning(f"Encountered an item that is not a dictionary in the hits list for source record ID "
                           f"'{source_record_id_for_log}'. Item snippet: {str(hit_data_dict)[:500]}...")
            continue
        try:
            hit_id = str(hit_data_dict["_id"]) # _id should always be present and convertible to string

            hit_score_raw = hit_data_dict.get("_score") # _score might be None for non-scoring queries or filter contexts
            hit_score = 0.0  # Default to 0.0 if _score is None
            if hit_score_raw is not None:
                hit_score = float(hit_score_raw)

            hit_source_content = hit_data_dict.get("_source")
            if hit_source_content is None:
                # This can happen if _source is disabled in the query or if the document had no _source.
                logger.debug(f"Hit '{hit_id}' for source record ID '{source_record_id_for_log}' has no _source field. Using empty dictionary.")
                hit_source_content = {}
            elif not isinstance(hit_source_content, dict):
                logger.warning(f"Hit '{hit_id}' for source record ID '{source_record_id_for_log}' has a _source field that is not a dictionary. "
                               f"Type: {type(hit_source_content)}. Using empty dictionary.")
                hit_source_content = {}

            extracted_hits_list.append({
                "id": hit_id,
                "score": hit_score,
                "source": hit_source_content
            })

        except KeyError as e:
            logger.warning(
                f"Malformed hit found for source record ID '{source_record_id_for_log}'. "
                f"Missing key: {e}. Hit data snippet: {str(hit_data_dict)[:500]}..."
            )
        except (ValueError, TypeError) as e:
             logger.warning(
                f"Type error processing a field from a hit for source record ID '{source_record_id_for_log}'. "
                f"Error: {e}. Hit data snippet: {str(hit_data_dict)[:500]}..."
            )
        except Exception as e: # Catch-all for any other unexpected errors during hit processing
            logger.error(
                f"Unexpected error processing a hit for source record ID '{source_record_id_for_log}': {e}. "
                f"Hit data snippet: {str(hit_data_dict)[:500]}...",
                exc_info=True
            )

    if extracted_hits_list: # Log only if something was actually extracted, to avoid double logging "0 hits"
        logger.debug(f"Extracted {len(extracted_hits_list)} hits for source record ID '{source_record_id_for_log}'.")
    return extracted_hits_list