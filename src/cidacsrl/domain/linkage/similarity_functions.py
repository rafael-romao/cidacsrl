import logging
from typing import Any, Optional

import jellyfish

logger = logging.getLogger("Service: SimilarityFunctions")


def exact_score_func(s1: Any, s2: Any) -> float:
    """
    Calcula um score de similaridade exata.

    Retorna 1.0 se as representações em string de s1 e s2 são idênticas (e não nulas),
    e 0.0 caso contrário. Qualquer valor `None` em s1 ou s2 resultará em 0.0.

    Args:
        s1 (Any): O primeiro valor para comparação.
        s2 (Any): O segundo valor para comparação.

    Returns:
        float: Score de similaridade, 1.0 para correspondência exata (não nula), 0.0 caso contrário.
    """
    if s1 is None or s2 is None: # If either or both are None, it's not a positive match.
        return 0.0

    try:
        # Both s1 and s2 are not None at this point.
        return 1.0 if str(s1) == str(s2) else 0.0
    except Exception as e:
        # Catch string conversion errors, though rare for common types if not None.
        logger.error(f"Error converting non-None values to string in exact_score_func for '{s1}' vs '{s2}': {e}", exc_info=True)
        return 0.0


def jaro_winkler_score_func(s1: str, s2: str) -> float:
    """
    Calcula a similaridade Jaro-Winkler entre duas strings.

    Espera-se que as entradas já sejam strings e não `None` pelo chamador (`scoring_engine`).
    Se ocorrer um erro durante o cálculo, retorna 0.0. Assume-se que `jellyfish` está instalado.

    Args:
        s1 (str): A primeira string.
        s2 (str): A segunda string.

    Returns:
        float: O score de similaridade Jaro-Winkler, ou 0.0 em caso de erro.
    """
    # Conversion to str() is expected from the caller (scoring_engine), but for safety:
    s1_str = str(s1) if s1 is not None else ""
    s2_str = str(s2) if s2 is not None else ""

    if not s1_str and not s2_str:  # Both are empty or were None
        return 1.0  # Two empty strings are perfectly similar
    if not s1_str or not s2_str:  # One is empty/None, the other is not
        return 0.0

    try:
        return jellyfish.jaro_winkler_similarity(s1_str, s2_str)
    except Exception as e:
        logger.error(f"Error calculating Jaro-Winkler similarity for '{s1_str}' vs '{s2_str}': {e}", exc_info=True)
        return 0.0


def hamming_score_func(s1: str, s2: str) -> float:
    """
    Calcula a similaridade baseada na distância de Hamming normalizada (1 - dist/len).
    As strings DEVEM ter o mesmo comprimento para que a distância de Hamming seja significativa.

    Espera-se que as entradas já sejam strings e não `None` pelo chamador (`scoring_engine`).
    Retorna 0.0 se os comprimentos das strings diferirem ou se ocorrer um erro.
    Assume-se que `jellyfish` está instalado.

    Args:
        s1 (str): A primeira string.
        s2 (str): A segunda string.

    Returns:
        float: O score de similaridade baseado em Hamming, ou 0.0.
    """
    # Conversion to str() is expected from the caller, but for safety:
    s1_str = str(s1) if s1 is not None else ""
    s2_str = str(s2) if s2 is not None else ""

    if len(s1_str) != len(s2_str):
        logger.debug(f"For Hamming distance, strings must have the same length. "
                     f"Received: '{s1_str}' (len {len(s1_str)}) and '{s2_str}' (len {len(s2_str)}). Returning 0.0.")
        return 0.0

    if not s1_str:  # Both are "" (and therefore of same length 0)
        return 1.0  # Considered perfectly similar

    try:
        distance = jellyfish.hamming_distance(s1_str, s2_str)
        # Normalize the distance to a similarity score
        return 1.0 - (float(distance) / len(s1_str))
    except Exception as e:
        logger.error(f"Error calculating Hamming similarity for '{s1_str}' vs '{s2_str}': {e}", exc_info=True)
        return 0.0