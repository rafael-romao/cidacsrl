import pytest
from cidacsrl_rlp.src.es.query_builder import create_es_query_for_phase

# Dados de exemplo para os testes
@pytest.fixture
def sample_source_row():
    return {
        "nome": "Maria Souza",
        "nome_mae": "Ana Souza",
        "data_nasc": "1990-05-15",
        "municipio": "Salvador",
        "cpf": "12345678900"
    }

@pytest.fixture
def sample_rules():
    return [
        {"source_column": "nome", "target_column": "nome_completo", "es_clause_type": "must", "query_type": "match", "boost": 2.0},
        {"source_column": "nome_mae", "target_column": "nome_mae", "es_clause_type": "should", "query_type": "match", "is_fuzzy": True},
        {"source_column": "data_nasc", "target_column": "dt_nasc", "es_clause_type": "filter", "query_type": "term"},
        {"source_column": "cpf", "target_column": "nu_cpf", "es_clause_type": "must_not", "query_type": "term"},
    ]

def test_full_query_generation(sample_source_row, sample_rules):
    """Testa a geração de uma consulta complexa com todos os tipos de cláusulas."""
    query = create_es_query_for_phase(
        source_row_dict=sample_source_row,
        rules_dicts=sample_rules,
        target_es_fields_to_fetch=["nome_completo", "nome_mae", "dt_nasc"],
        candidate_limit=10
    )

    assert query is not None
    assert query["size"] == 10
    assert query["_source"] == ["nome_completo", "nome_mae", "dt_nasc"]

    bool_query = query["query"]["bool"]
    assert len(bool_query["must"]) == 1
    assert bool_query["must"][0] == {"match": {"nome_completo": {"query": "Maria Souza", "boost": 2.0}}}

    assert len(bool_query["should"]) == 1
    assert bool_query["should"][0] == {"match": {"nome_mae": {"query": "Ana Souza", "fuzziness": "AUTO"}}}

    assert len(bool_query["filter"]) == 1
    assert bool_query["filter"][0] == {"term": {"dt_nasc": {"value": "1990-05-15"}}}

    assert len(bool_query["must_not"]) == 1
    assert bool_query["must_not"][0] == {"term": {"nu_cpf": {"value": "12345678900"}}}

def test_no_rules_returns_none():
    """Testa se a função retorna None quando nenhuma regra é fornecida."""
    query = create_es_query_for_phase(
        source_row_dict={"nome": "Teste"},
        rules_dicts=[],
        target_es_fields_to_fetch=["nome"],
        candidate_limit=10
    )
    assert query is None

def test_null_source_value_skips_rule(sample_rules):
    """Testa se uma regra é ignorada se o valor correspondente na fonte for None."""
    source_row = {"nome": "Maria", "nome_mae": None} # nome_mae é None
    rules = [sample_rules[0], sample_rules[1]] # Apenas regras de nome e nome_mae

    query = create_es_query_for_phase(
        source_row_dict=source_row,
        rules_dicts=rules,
        target_es_fields_to_fetch=["nome_completo"],
        candidate_limit=5
    )

    bool_query = query["query"]["bool"]
    assert "should" not in bool_query # A cláusula 'should' para nome_mae deve ser ignorada
    assert len(bool_query["must"]) == 1 # A cláusula 'must' para nome deve existir

def test_only_should_clauses_adds_minimum_should_match():
    """Testa se 'minimum_should_match' é adicionado quando apenas cláusulas 'should' existem."""
    source_row = {"nome": "Maria", "nome_mae": "Ana"}
    rules = [
        {"source_column": "nome", "target_column": "nome_completo", "es_clause_type": "should", "query_type": "match"},
        {"source_column": "nome_mae", "target_column": "nome_mae", "es_clause_type": "should", "query_type": "match"},
    ]
    query = create_es_query_for_phase(source_row, rules, [], 10)
    
    bool_query = query["query"]["bool"]
    assert "must" not in bool_query
    assert len(bool_query["should"]) == 2
    assert bool_query["minimum_should_match"] == 1

def test_invalid_candidate_limit_returns_none(sample_source_row, sample_rules):
    """Testa se um limite de candidatos inválido (<=0) retorna None."""
    query_zero = create_es_query_for_phase(sample_source_row, sample_rules, [], 0)
    query_negative = create_es_query_for_phase(sample_source_row, sample_rules, [], -1)
    
    assert query_zero is None
    assert query_negative is None