import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

def create_es_query_for_phase(
    source_row_dict: Dict[str, Any],
    rules_dicts: List[Dict[str, Any]],
    target_es_fields_to_fetch: List[str],
    candidate_limit: int,
    filter_query: dict,
) -> Optional[Dict[str, Any]]:
    """
    Cria o corpo da consulta Elasticsearch para um único registro da fonte,
    baseado em uma lista de regras de comparação (dicionários de `ComparisonRule`) para uma fase,
    utilizando o campo `es_clause_type` de cada regra.

    Args:
        source_row_dict (Dict[str, Any]): Dicionário representando o registro da fonte.
        rules_dicts (List[Dict[str, Any]]): Lista de dicionários de regras de comparação para a fase.
        target_es_fields_to_fetch (List[str]): Lista de campos do índice alvo no Elasticsearch a serem retornados.
        candidate_limit (int): Número máximo de candidatos a serem retornados pela consulta.

    Returns:
        Optional[Dict[str, Any]]: Um dicionário representando o corpo da consulta Elasticsearch,
                                   ou `None` se nenhuma consulta válida puder ser gerada.
    """
    must_clauses: List[Dict[str, Any]] = []
    should_clauses: List[Dict[str, Any]] = []
    filter_clauses: List[Dict[str, Any]] = []
    must_not_clauses: List[Dict[str, Any]] = []

    if not rules_dicts:
        logger.debug("No comparison rules (rules_dicts) provided for the phase. Cannot generate ES query.")
        return None

    for rule_dict in rules_dicts:
        source_column_name = rule_dict.get('source_column')
        target_es_field_name = rule_dict.get('target_column')

        if not source_column_name or not target_es_field_name:
            logger.warning(f"Incomplete comparison rule dictionary: {rule_dict}. Skipping this rule.")
            continue

        source_value = source_row_dict.get(source_column_name)

        # If source value is None or an empty/whitespace-only string, skip this rule for ES query generation.
        # This prevents overly broad or meaningless queries for empty/null values.
        if source_value is None or (isinstance(source_value, str) and not source_value.strip()):
            logger.debug(f"Source value for '{source_column_name}' is null or empty. Skipping rule for ES query.")
            continue

        clause_type_from_rule = rule_dict.get('es_clause_type')
        if clause_type_from_rule is None:
            logger.debug(f"Rule for '{source_column_name}' has null 'es_clause_type'. It will not contribute to the bool query.")
            continue

        clause_type = str(clause_type_from_rule).lower() # e.g., 'must', 'should', 'filter', 'must_not'

        query_type = rule_dict.get('query_type', 'match').lower() # e.g., 'match', 'term', 'prefix'
        is_fuzzy = rule_dict.get('is_fuzzy', False)
        boost = float(rule_dict.get('boost', 1.0)) # Boost factor for the query clause

        query_clause_content: Dict[str, Any] = {}
        specific_query_details: Dict[str, Any] = {}

        # --- Build the specific query clause (term, match, prefix, etc.) ---
        if query_type == "term":
            specific_query_details = {"value": source_value}
            # Boost is typically not applicable directly to 'term' queries in 'filter' context,
            # but can be applied if 'term' is used in 'should' or 'must' for scoring.
            if clause_type in ["should", "must"] and boost != 1.0:
                 specific_query_details["boost"] = boost
            query_clause_content = {"term": {target_es_field_name: specific_query_details}}

        elif query_type == "match":
            specific_query_details = {"query": str(source_value)}
            if is_fuzzy:
                specific_query_details["fuzziness"] = "AUTO" # Common setting for fuzziness
            if clause_type in ["should", "must"] and boost != 1.0: # Boost for scoring clauses
                specific_query_details["boost"] = boost
            query_clause_content = {"match": {target_es_field_name: specific_query_details}}

        elif query_type == "prefix":
            specific_query_details = {"value": str(source_value)}
            # Boost can be relevant if used in scoring contexts
            if clause_type in ["should", "must"] and boost != 1.0:
                specific_query_details["boost"] = boost
            query_clause_content = {"prefix": {target_es_field_name: specific_query_details}}

        else:
            logger.warning(f"Unsupported query type: '{query_type}' for target field '{target_es_field_name}'. Skipping this rule.")
            continue
        # --- End of specific query clause construction ---

        if not query_clause_content:
            continue # If for some reason the clause content is empty

        # Add the constructed query clause to the appropriate list based on 'es_clause_type'
        if clause_type == 'must':
            must_clauses.append(query_clause_content)
        elif clause_type == 'should':
            should_clauses.append(query_clause_content)
        elif clause_type == 'filter':
            # Filter clauses are for non-scoring, exact matching criteria
            filter_clauses.append(query_clause_content)
        elif clause_type == 'must_not':
            must_not_clauses.append(query_clause_content)
        else:
            logger.debug(f"Unrecognized ES clause type '{clause_type}' for rule on source column '{source_column_name}'. Rule ignored for bool query.")


    # --- Combine clauses into the final bool query ---
    bool_query_conditions: Dict[str, Any] = {}
    if must_clauses:
        bool_query_conditions["must"] = must_clauses
    if should_clauses:
        bool_query_conditions["should"] = should_clauses
        # If there are no 'must' clauses, at least one 'should' clause must match by default.
        # If 'must' clauses exist, 'minimum_should_match' is not strictly necessary for results,
        # but can be used to refine the logic of 'should' clauses.
        # A common default is 1 if only 'should' clauses are present.
        if not must_clauses and "minimum_should_match" not in bool_query_conditions:
             bool_query_conditions["minimum_should_match"] = 1

    # if filter_clauses:
    #     bool_query_conditions["filter"] = filter_clauses

    if filter_query:
        if filter_query.get('query'):
            bool_query_conditions['filter'] = filter_query.get('query')
        elif filter_query.get('column'):
            col_filter = filter_query.get('column')
            bool_query_conditions['filter'] = [{
                'term': {
                    col_filter: source_row_dict.get(col_filter) # tolink_cols_dict[col_filter]
                }
            }]
        else:
            raise Exception(f"Estrutura do filter inválida: {filter_query}")

    if must_not_clauses:
        bool_query_conditions["must_not"] = must_not_clauses

    if not bool_query_conditions:
        source_id_for_log = source_row_dict.get(next(iter(source_row_dict)), 'N/A_SOURCE_ID') if source_row_dict else 'N/A_SOURCE_ID'

        logger.debug(
            f"No effective query clauses (must, should, filter, must_not) were generated for the source record "
            f"(example source ID value: '{source_id_for_log}'). "
            f"No ES search will be performed for this record in this phase."
        )
        return None

    final_es_query_structure = {"bool": bool_query_conditions}
    search_body: Dict[str, Any] = {"query": final_es_query_structure}

    if candidate_limit > 0:
        search_body["size"] = candidate_limit
    else:        
        logger.warning(f"Candidate limit is {candidate_limit} for the phase. No ES search will be performed or no results will be fetched.")
        return None

    if target_es_fields_to_fetch:
        search_body["_source"] = target_es_fields_to_fetch
    else:
        # If no specific fields are requested, ES returns all. To avoid this if not intended:
        search_body["_source"] = False # Set to False to not return _source field at all
        logger.debug("No target_es_fields_to_fetch specified; ES query will set _source to False.")

    logger.debug(f"Generated ES query body for the phase: {search_body}")
    return search_body