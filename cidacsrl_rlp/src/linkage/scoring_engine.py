# src/linkage/scoring_engine.py

import logging
from typing import Dict, Any, List, Callable

from cidacsrl_rlp.src.linkage.similarity_functions import (
    jaro_winkler_score_func,
    hamming_score_func,
    exact_score_func
)

logger = logging.getLogger(__name__)

# Mapping from configuration strings to actual similarity functions
SIMILARITY_FUNCTION_MAP: Dict[str, Callable[[Any, Any], float]] = {
    "overlap": exact_score_func,  # 'overlap' often implies exact match for structured data elements
    "exact": exact_score_func,
    "jaro_winkler": jaro_winkler_score_func,
    "hamming": hamming_score_func,
    # Add other similarity functions here as they are implemented
    # "levenshtein": levenshtein_normalized_similarity, # Example
}

def calculate_pair_scores_and_similarities(
    source_row_dict: Dict[str, Any],
    candidate_data_dict_prefixed: Dict[str, Any],  # Contains candidate fields already prefixed
    phase_rules_dicts: List[Dict[str, Any]],  # List of ComparisonRule as dictionaries
    workflow_config_dict: Dict[str, Any]  # Workflow configuration, to access _candidate_prefix
) -> Dict[str, Any]:
    """
    Calcula scores de similaridade individuais e um score composto final para um par fonte-candidato,
    baseado nas configurações das colunas (regras) da fase de linkage.

    Args:
        source_row_dict (Dict[str, Any]): Dicionário representando o registro da fonte.
        candidate_data_dict_prefixed (Dict[str, Any]): Dicionário representando o registro do candidato,
                                                       com nomes de campo já prefixados (ex: "candidate_nome").
        phase_rules_dicts (List[Dict[str, Any]]): Lista de dicionários, onde cada um é a configuração de uma
                                                  `ComparisonRule` para a fase atual. Chaves esperadas incluem:
                                                  'source_column', 'target_column', 'similarity',
                                                  'weight', 'penalty'.
        workflow_config_dict (Dict[str, Any]): Dicionário de configuração do workflow, usado para obter
                                               o `_candidate_prefix`.

    Returns:
        Dict[str, Any]: Um dicionário contendo:
            - "match_score": O score composto final do linkage para o par nesta fase (anteriormente "score").
            - "sim_{source_column_name}": Scores de similaridade individuais para cada regra.
    """
    individual_similarity_scores: Dict[str, float] = {}
    current_pair_total_raw_score: float = 0.0
    total_max_configured_positive_weight: float = 0.0
    any_penalty_applied_to_pair: bool = False # Tracks if any rule triggered a penalty due to nulls

    candidate_prefix = workflow_config_dict.get('_candidate_prefix', 'candidate_')

    if not phase_rules_dicts:
        logger.warning("No scoring rules (phase_rules_dicts) provided. Returning zero score.")
        return {"match_score": 0.0} # Corresponds to COMPOSITE_SCORE_FIELD from schema_helpers

    # First, calculate the sum of all positive weights configured for this phase.
    # This sum is used for normalizing the raw score.
    for rule_dict in phase_rules_dicts:
        weight = float(rule_dict.get('weight', 0.0))
        if weight > 0: # Only sum positive weights for normalization base
            total_max_configured_positive_weight += weight

    # Calculate similarity scores for each rule and accumulate the raw weighted score
    for rule_dict in phase_rules_dicts:
        source_column_name = rule_dict.get('source_column')
        target_column_name = rule_dict.get('target_column')

        if not source_column_name or not target_column_name:
            logger.warning(f"Scoring rule dictionary is incomplete (missing 'source_column' or "
                           f"'target_column'): {rule_dict}. Skipping this rule.")
            continue

        # Construct the prefixed field name for the candidate's data
        candidate_prefixed_field_name = f"{candidate_prefix}{target_column_name}"

        source_value = source_row_dict.get(source_column_name)
        candidate_value = candidate_data_dict_prefixed.get(candidate_prefixed_field_name)

        similarity_function_key = rule_dict.get('similarity', 'exact').lower() # Default to 'exact'
        weight = float(rule_dict.get('weight', 0.0))
        penalty = float(rule_dict.get('penalty', 0.0)) # Penalty for null comparison

        current_similarity_for_column: float = 0.0

        # Handle null values: if a penalty is configured, apply it. Similarity is 0 for nulls.
        if source_value is None or candidate_value is None:
            if penalty > 0: # Apply penalty if configured
                current_pair_total_raw_score -= penalty # Subtract penalty from raw score
                any_penalty_applied_to_pair = True
            current_similarity_for_column = 0.0 # Similarity is 0 if any value is null
            logger.debug(f"Null value in source ('{source_column_name}') or candidate ('{candidate_prefixed_field_name}'). "
                         f"Penalty applied: {penalty}. Column similarity set to 0.0.")
        else:
            # Both values are present, calculate similarity
            sim_func = SIMILARITY_FUNCTION_MAP.get(similarity_function_key)
            if sim_func:
                try:
                    # Ensure values are strings for most similarity functions
                    current_similarity_for_column = sim_func(str(source_value), str(candidate_value))
                except Exception as e:
                    logger.warning(
                        f"Error calculating similarity '{similarity_function_key}' for "
                        f"source:'{source_value}' vs candidate:'{candidate_value}'. Error: {e}. "
                        f"Defaulting to similarity 0.0 for this column.",
                        exc_info=True
                    )
                    current_similarity_for_column = 0.0
            else:
                logger.warning(f"Similarity function '{similarity_function_key}' not found in map. "
                               f"Defaulting to similarity 0.0 for column '{source_column_name}'.")
                current_similarity_for_column = 0.0

            # Add the weighted similarity to the total raw score for the pair
            # Note: weight can be negative if a dissimilarity contributes negatively
            current_pair_total_raw_score += current_similarity_for_column * weight

        # Store individual similarity score for the column (before weighting)
        individual_similarity_scores[f"sim_{source_column_name}"] = round(current_similarity_for_column, 6)

    # Normalize the raw score and clamp it
    final_composite_score: float
    if total_max_configured_positive_weight > 0:
        # Normalize score against the sum of positive weights
        normalized_score = current_pair_total_raw_score / total_max_configured_positive_weight
    elif current_pair_total_raw_score <= 0: # No positive weights, and score is zero or negative (due to penalties)
        normalized_score = current_pair_total_raw_score # Keep as is, likely negative or zero
    else: # No positive weights, but raw score is somehow positive (should not happen if weights are non-positive)
        normalized_score = 0.0
        logger.warning(f"Total max configured positive weight is 0, but raw score ({current_pair_total_raw_score}) is positive. "
                       f"This indicates an issue with weight configuration. Final score will be 0 for this component.")

    # Clamp the normalized score
    # If a penalty was applied, the score can be negative. It's clamped only at the upper bound of 1.0.
    # If no penalty was applied, the score is clamped between 0.0 and 1.0.
    if any_penalty_applied_to_pair:
        final_composite_score = min(1.0, normalized_score) # Can be < 0 if penalties are large
    else:
        final_composite_score = max(0.0, min(1.0, normalized_score)) # Clamped between 0 and 1

    output_scores = {
        "match_score": round(final_composite_score, 6),  # Main composite score for the phase
        **individual_similarity_scores # Include all individual sim_{column} scores
    }

    return output_scores