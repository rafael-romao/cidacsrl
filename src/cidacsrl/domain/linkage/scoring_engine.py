import logging
from typing import Any, Callable, Dict, List

from cidacsrl.domain.linkage.matching_rules import ComparisonRule
from cidacsrl.domain.linkage.similarity_functions import (
    exact_score_func,
    hamming_score_func,
    jaro_winkler_score_func,
)

logger = logging.getLogger("Service: ScoringEngine")

SIMILARITY_FUNCTION_MAP: Dict[str, Callable[[Any, Any], float]] = {
    "overlap": exact_score_func,
    "exact": exact_score_func,
    "jaro_winkler": jaro_winkler_score_func,
    "hamming": hamming_score_func,
}

def calculate_pair_scores_and_similarities(
    source_row_dict: Dict[str, Any],
    candidate_data_dict: Dict[str, Any], 
    rules: List[ComparisonRule],
    debug: bool = False,
) -> Dict[str, Any]:
    if not rules:
        if debug:
            return {
                "match_score": 0.0,
                "_debug": {
                    "raw_score": 0.0,
                    "total_weight": 0.0,
                    "normalized_score": 0.0,
                    "any_penalty_applied": False,
                    "rules": [],
                },
            }
        return {"match_score": 0.0}

    total_max_configured_weight = sum(rule.weight for rule in rules)
    individual_similarity_scores: Dict[str, float] = {}
    current_pair_total_raw_score: float = 0.0
    any_penalty_applied: bool = False
    debug_rules_breakdown: List[Dict[str, Any]] = []

    for rule in rules:
        source_value = source_row_dict.get(rule.source_column)
        candidate_value = candidate_data_dict.get(rule.target_column)
        current_similarity: float = 0.0
        penalty_applied: float = 0.0
        weighted_contribution: float = 0.0
        similarity_error: str | None = None

        if source_value is None or candidate_value is None:
            if rule.penalty > 0:
                current_pair_total_raw_score -= rule.penalty
                penalty_applied = rule.penalty
                any_penalty_applied = True
        else:
            sim_func = SIMILARITY_FUNCTION_MAP[rule.similarity]
            
            try:
                current_similarity = sim_func(str(source_value), str(candidate_value))
            except Exception as e:
                logger.warning(f"Error calculating {rule.similarity} on '{rule.source_column} vs {rule.target_column}': {e}")
                similarity_error = str(e)
            
            weighted_contribution = current_similarity * rule.weight
            current_pair_total_raw_score += weighted_contribution

        individual_similarity_scores[f"sim_{rule.source_column}"] = round(current_similarity, 6)

        if debug:
            debug_rules_breakdown.append(
                {
                    "source_column": rule.source_column,
                    "target_column": rule.target_column,
                    "similarity": rule.similarity,
                    "weight": rule.weight,
                    "penalty": rule.penalty,
                    "source_value": source_value,
                    "candidate_value": candidate_value,
                    "current_similarity": round(current_similarity, 6),
                    "weighted_contribution": round(weighted_contribution, 6),
                    "penalty_applied": round(penalty_applied, 6),
                    "running_raw_score": round(current_pair_total_raw_score, 6),
                    "error": similarity_error,
                }
            )

    normalized_score = (
        current_pair_total_raw_score / total_max_configured_weight 
        if total_max_configured_weight > 0 
        else 0.0
    )

    final_score = min(1.0, normalized_score) if any_penalty_applied else max(0.0, min(1.0, normalized_score))

    result = {
        "match_score": round(final_score, 6),
        **individual_similarity_scores
    }

    if debug:
        result["_debug"] = {
            "raw_score": round(current_pair_total_raw_score, 6),
            "total_weight": round(total_max_configured_weight, 6),
            "normalized_score": round(normalized_score, 6),
            "any_penalty_applied": any_penalty_applied,
            "rules": debug_rules_breakdown,
        }

    return result