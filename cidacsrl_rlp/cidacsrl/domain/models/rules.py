import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

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
    es_clause_type: str
    query_type: str
    similarity: str
    weight: float
    penalty: float = 0.0
    is_fuzzy: bool = False
    boost: Optional[float] = 1.0

    @classmethod
    def from_dict(cls, rule_dict: Dict[str, Any]) -> 'ComparisonRule':
        return cls(**rule_dict)

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
    rules: List[ComparisonRule] = field(default_factory=list)

    def __post_init__(self):
        if not self.rules:
            logger.warning(f"BlockingPhase '{self.phase_name}' has no ComparisonRules defined.")
        if not 0 <= self.strong_match_score_threshold <= 1:
            # This validation ensures the threshold is a valid probability/score.
            raise ValueError("strong_match_score_threshold must be between 0 and 1.")
    
    @classmethod
    def from_dict(cls, phase_dict: Dict[str, Any]) -> 'BlockingPhase':
        phase_dict = phase_dict.copy()
        rules_data = phase_dict.pop("rules", [])
        rules = [ComparisonRule.from_dict(rule) for rule in rules_data]
        return cls(rules=rules, **phase_dict)