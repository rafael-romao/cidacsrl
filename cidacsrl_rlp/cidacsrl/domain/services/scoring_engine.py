import logging
from typing import Dict, Any, List, Callable

from cidacsrl_rlp.cidacsrl.domain.services.similarity_functions import (
    jaro_winkler_score_func,
    hamming_score_func,
    exact_score_func
)
from cidacsrl_rlp.cidacsrl.domain.models.matching_rules import ComparisonRule

logger = logging.getLogger(__name__)

SIMILARITY_FUNCTION_MAP: Dict[str, Callable[[Any, Any], float]] = {
    "overlap": exact_score_func,
    "exact": exact_score_func,
    "jaro_winkler": jaro_winkler_score_func,
    "hamming": hamming_score_func,
}

def calculate_pair_scores_and_similarities(
    source_row_dict: Dict[str, Any],
    candidate_data_dict: Dict[str, Any], 
    rules: List[ComparisonRule]
) -> Dict[str, Any]:
    if not rules:
        return {"match_score": 0.0} 

    total_max_configured_weight = sum(rule.weight for rule in rules)
    individual_similarity_scores: Dict[str, float] = {}
    current_pair_total_raw_score: float = 0.0
    any_penalty_applied: bool = False 

    for rule in rules:
        source_value = source_row_dict.get(rule.source_column)
        candidate_value = candidate_data_dict.get(rule.target_column)
        current_similarity: float = 0.0

        if source_value is None or candidate_value is None:
            if rule.penalty > 0:
                current_pair_total_raw_score -= rule.penalty
                any_penalty_applied = True
        else:
            sim_func = SIMILARITY_FUNCTION_MAP[rule.similarity]
            
            try:
                current_similarity = sim_func(str(source_value), str(candidate_value))
            except Exception as e:
                logger.warning(f"Error calculating {rule.similarity} on '{rule.source_column} vs {rule.target_column}': {e}")
            
            current_pair_total_raw_score += current_similarity * rule.weight

        individual_similarity_scores[f"sim_{rule.source_column}"] = round(current_similarity, 6)

    normalized_score = (
        current_pair_total_raw_score / total_max_configured_weight 
        if total_max_configured_weight > 0 
        else 0.0
    )

    final_score = min(1.0, normalized_score) if any_penalty_applied else max(0.0, min(1.0, normalized_score))

    return {
        "match_score": round(final_score, 6),
        **individual_similarity_scores
    }