import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Literal

logger = logging.getLogger(__name__)


@dataclass
class ComparisonRule:
    """
    Define uma regra de comparação entre uma coluna da fonte e uma coluna do alvo.

    Atributos:
        source_column (str): Nome da coluna na tabela de origem.
        target_column (str): Nome da coluna na tabela de destino (ex: campo no Elasticsearch).
        es_clause_type (str): Tipo de cláusula Elasticsearch a ser usada (ex: 'must', 'should', 'filter').
        query_type (str): Tipo de query Elasticsearch (ex: 'match', 'term', 'prefix').
        similarity (str): Chave da função de similaridade a ser usada para pontuação (ex: 'jaro_winkler', 'exact').
        weight (float): Peso desta regra no cálculo do score composto.
        penalty (float): Penalidade a ser aplicada se uma das colunas for nula (default: 0.0).
        is_fuzzy (bool): Indica se a query `match` deve usar `fuzziness` (default: False).
        boost (Optional[float]): Fator de boost para a cláusula na query Elasticsearch (default: 1.0).
    """
    source_column: str
    target_column: str
    es_clause_type: Literal["must", "should", "filter", "must_not"] = "must"
    query_type: Literal["match", "term", "match_phrase", "prefix"] = "match"
    similarity: str
    weight: float
    penalty: float = 0.0
    is_fuzzy: bool = False
    boost: Optional[float] = 1.0

    def __post_init__(self):
        if self.weight < 0:
            raise ValueError("weight must be greater than or equal to 0.")
        if self.penalty < 0:
            raise ValueError("penalty must be greater than or equal to 0.")
        if self.boost is not None and self.boost <= 0:
            raise ValueError("boost must be greater than 0 when provided.")
        if self.is_fuzzy and self.query_type != "match":
            raise ValueError("is_fuzzy can only be used with query_type='match'.")

    @classmethod
    def from_dict(cls, rule_dict: Dict[str, Any]) -> 'ComparisonRule':
        parsed = rule_dict.copy()
        if "es_clause_type" in parsed and isinstance(parsed["es_clause_type"], str):
            parsed["es_clause_type"] = parsed["es_clause_type"].lower()
        if "query_type" in parsed and isinstance(parsed["query_type"], str):
            parsed["query_type"] = parsed["query_type"].lower()
        return cls(**parsed)

@dataclass
class BlockingPhase:
    """
    Configura uma fase (ou estratégia) de blocking dentro do workflow de linkage.
    Cada fase define um conjunto de regras para encontrar candidatos e calcular scores.

    Atributos:
        phase_name (str): Nome único para a fase de blocking.
        phase_description (Optional[str]): Descrição opcional da fase.
        enabled (bool): Se esta fase está habilitada para execução (default: True).
        candidate_limit (int): Número máximo de candidatos a serem recuperados do Elasticsearch por registro fonte (default: 10).
        strong_match_score_threshold (float): Limiar de score (entre 0 e 1) para considerar um par como "strong match" nesta fase (default: 0.9).
        rules (List[ComparisonRule]): Lista de `ComparisonRule` para esta fase.
    """
    phase_name: str
    phase_description: Optional[str] = None
    enabled: bool = True
    candidate_limit: int = 10
    strong_match_score_threshold: float = 0.9
    target_fields: Optional[List[str]] = None
    rules: List[ComparisonRule] = field(default_factory=list)

    def __post_init__(self):
        if not self.rules:
            logger.warning(f"BlockingPhase '{self.phase_name}' has no ComparisonRules defined.")
        if self.target_fields is None:
            self.target_fields = list(dict.fromkeys(rule.target_column for rule in self.rules))
        if self.candidate_limit <= 0:
            raise ValueError("candidate_limit must be greater than 0.")
        if not 0 <= self.strong_match_score_threshold <= 1:
            # This validation ensures the threshold is a valid probability/score.
            raise ValueError("strong_match_score_threshold must be between 0 and 1.")
    
    @classmethod
    def from_dict(cls, phase_dict: Dict[str, Any]) -> 'BlockingPhase':
        phase_dict = phase_dict.copy()
        rules_data = phase_dict.pop("rules", [])
        rules = [ComparisonRule.from_dict(rule) for rule in rules_data]
        return cls(rules=rules, **phase_dict)