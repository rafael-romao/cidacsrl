import pytest
from unittest.mock import patch

from cidacsrl_rlp.cidacsrl.domain.services.scoring_engine import calculate_pair_scores_and_similarities
from cidacsrl_rlp.cidacsrl.domain.models.rules import ComparisonRule

pytestmark = pytest.mark.unit

# ==========================================
# FIXTURES (Mocks das Funções de Similaridade)
# ==========================================

MOCK_SIMILARITY_MAP = {
    "exact": lambda s, c: 1.0 if s == c else 0.0,
    "mock_jaro": lambda s, c: 0.85,  
    "mock_error": lambda s, c: 1 / 0 
}

@pytest.fixture
def base_rules():
    """Retorna um conjunto padrão de regras para testes básicos."""
    return [
        ComparisonRule(source_column="nome", target_column="nome", similarity="exact", weight=2.0, penalty=0.0),
        ComparisonRule(source_column="idade", target_column="idade", similarity="exact", weight=1.0, penalty=0.0)
    ]

# ==========================================
# TESTES
# ==========================================

def test_calculate_with_empty_rules():
    """Caso 1: Lista de regras vazia deve retornar score 0.0 imediatamente."""
    result = calculate_pair_scores_and_similarities(
        source_row_dict={"nome": "João"},
        candidate_data_dict={"nome": "João"},
        rules=[]
    )
    assert result == {"match_score": 0.0}

@patch("cidacsrl_rlp.cidacsrl.domain.services.scoring_engine.SIMILARITY_FUNCTION_MAP", MOCK_SIMILARITY_MAP)
def test_calculate_perfect_match(base_rules):
    """Caso 2: Match exato em todas as colunas. O score deve ser 1.0."""
    result = calculate_pair_scores_and_similarities(
        source_row_dict={"nome": "João", "idade": 30},
        candidate_data_dict={"nome": "João", "idade": 30},
        rules=base_rules
    )
    
    assert result["match_score"] == 1.0
    assert result["sim_nome"] == 1.0
    assert result["sim_idade"] == 1.0

@patch("cidacsrl_rlp.cidacsrl.domain.services.scoring_engine.SIMILARITY_FUNCTION_MAP", MOCK_SIMILARITY_MAP)
def test_calculate_complete_mismatch(base_rules):
    """Caso 3: Diferença total em todas as colunas. O score deve ser 0.0."""
    result = calculate_pair_scores_and_similarities(
        source_row_dict={"nome": "João", "idade": 30},
        candidate_data_dict={"nome": "Maria", "idade": 25},
        rules=base_rules
    )
    
    assert result["match_score"] == 0.0
    assert result["sim_nome"] == 0.0
    assert result["sim_idade"] == 0.0

@patch("cidacsrl_rlp.cidacsrl.domain.services.scoring_engine.SIMILARITY_FUNCTION_MAP", MOCK_SIMILARITY_MAP)
def test_calculate_partial_match_with_mocked_jaro():
    """Caso 4: Match parcial com pesos diferentes."""
    rules = [
        ComparisonRule(source_column="nome", target_column="nome", similarity="mock_jaro", weight=10.0, penalty=0.0)
    ]
    
    result = calculate_pair_scores_and_similarities(
        source_row_dict={"nome": "Joãozinho"},
        candidate_data_dict={"nome": "Joaozinho"},
        rules=rules
    )
    
    assert result["match_score"] == 0.85
    assert result["sim_nome"] == 0.85

@patch("cidacsrl_rlp.cidacsrl.domain.services.scoring_engine.SIMILARITY_FUNCTION_MAP", MOCK_SIMILARITY_MAP)
def test_null_values_without_penalty(base_rules):
    """Caso 5: Valor nulo presente, mas a regra NÃO possui penalidade."""
    result = calculate_pair_scores_and_similarities(
        source_row_dict={"nome": "João", "idade": None},
        candidate_data_dict={"nome": "João", "idade": 30},
        rules=base_rules
    )
    
    assert result["match_score"] == pytest.approx(0.666667, rel=1e-5)
    assert result["sim_nome"] == 1.0
    assert result["sim_idade"] == 0.0

@patch("cidacsrl_rlp.cidacsrl.domain.services.scoring_engine.SIMILARITY_FUNCTION_MAP", MOCK_SIMILARITY_MAP)
def test_null_values_with_penalty():
    """Caso 6: Valor nulo presente, e a regra possui penalidade."""
    rules = [
        ComparisonRule(source_column="nome", target_column="nome", similarity="exact", weight=2.0, penalty=0.0),
        ComparisonRule(source_column="cpf", target_column="cpf", similarity="exact", weight=1.0, penalty=1.5)
    ]
    
    result = calculate_pair_scores_and_similarities(
        source_row_dict={"nome": "João", "cpf": None},
        candidate_data_dict={"nome": "João", "cpf": "123"},
        rules=rules
    )
    
    assert result["match_score"] == pytest.approx(0.166667, rel=1e-5)
    assert result["sim_nome"] == 1.0
    assert result["sim_cpf"] == 0.0

@patch("cidacsrl_rlp.cidacsrl.domain.services.scoring_engine.SIMILARITY_FUNCTION_MAP", MOCK_SIMILARITY_MAP)
def test_negative_score_due_to_penalty():
    """Caso 7: Penalidades superam os acertos, resultando em score negativo."""
    rules = [
        ComparisonRule(source_column="nome", target_column="nome", similarity="exact", weight=1.0, penalty=0.0),
        ComparisonRule(source_column="cpf", target_column="cpf", similarity="exact", weight=1.0, penalty=5.0)
    ]
    
    result = calculate_pair_scores_and_similarities(
        source_row_dict={"nome": "João", "cpf": None},
        candidate_data_dict={"nome": "Maria", "cpf": "123"},
        rules=rules
    )
    
    assert result["match_score"] == -2.5

@patch("cidacsrl_rlp.cidacsrl.domain.services.scoring_engine.SIMILARITY_FUNCTION_MAP", MOCK_SIMILARITY_MAP)
def test_resilience_to_exceptions_in_similarity_functions(caplog):
    """Caso 8: Uma função matemática de similaridade levanta um erro inesperado."""
    rules = [
        ComparisonRule(source_column="campo", target_column="campo", similarity="mock_error", weight=1.0, penalty=0.0)
    ]
    
    result = calculate_pair_scores_and_similarities(
        source_row_dict={"campo": "A"},
        candidate_data_dict={"campo": "B"},
        rules=rules
    )
    
    assert result["match_score"] == 0.0
    assert result["sim_campo"] == 0.0
    assert "Error calculating mock_error" in caplog.text

def test_missing_similarity_key_raises_error():
    """Caso 9: O nome da função de similaridade não existe no mapa."""
    rules = [
        ComparisonRule(source_column="nome", target_column="nome", similarity="funcao_inexistente", weight=1.0, penalty=0.0)
    ]
    
    with pytest.raises(KeyError):
        calculate_pair_scores_and_similarities(
            source_row_dict={"nome": "A"},
            candidate_data_dict={"nome": "A"},
            rules=rules
        )