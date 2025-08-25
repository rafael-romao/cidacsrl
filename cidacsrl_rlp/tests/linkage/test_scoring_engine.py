import pytest
from faker import Faker
from cidacsrl_rlp.src.linkage.scoring_engine import calculate_pair_scores_and_similarities

# Instância do Faker para gerar dados de teste
fake = Faker('pt_BR')

@pytest.fixture
def workflow_config():
    """
    Fixture do Pytest que fornece uma configuração de workflow simulada.
    """
    return {
        'candidate_prefix': 'candidate_'
    }

def test_calculate_pair_scores_perfect_match(workflow_config):
    """
    Testa um cenário de correspondência perfeita usando dados gerados pelo Faker.
    O score final e todas as similaridades individuais devem ser 1.0.
    """
    # 1. Geração de Dados
    source_name = fake.name()
    source_dob = fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y%m%d')
    source_mother_name = fake.name()

    source_row = {
        'nome': source_name,
        'nasc': source_dob,
        'nome_mae': source_mother_name
    }
    # O candidato é uma cópia exata da fonte
    candidate_row = {
        'candidate_nome': source_name,
        'candidate_nasc': source_dob,
        'candidate_nome_mae': source_mother_name
    }

    # 2. Definição das Regras
    rules = [
        {'source_column': 'nome', 'target_column': 'nome', 'similarity': 'jaro_winkler', 'weight': 0.5},
        {'source_column': 'nasc', 'target_column': 'nasc', 'similarity': 'exact', 'weight': 0.2},
        {'source_column': 'nome_mae', 'target_column': 'nome_mae', 'similarity': 'jaro_winkler', 'weight': 0.3}
    ]

    # 3. Execução e Asserção
    result = calculate_pair_scores_and_similarities(
        source_row_dict=source_row,
        candidate_data_dict_prefixed=candidate_row,
        phase_rules_dicts=rules,
        workflow_config_dict=workflow_config
    )

    assert result['match_score'] == 1.0
    assert result['sim_nome'] == 1.0
    assert result['sim_nasc'] == 1.0
    assert result['sim_nome_mae'] == 1.0

def test_calculate_pair_scores_no_match(workflow_config):
    """
    Testa um cenário de não correspondência usando dados distintos gerados pelo Faker.
    O score final deve ser 0.0.
    """
    # 1. Geração de Dados (fonte e candidato são pessoas diferentes)
    source_row = {
        'nome': fake.name(),
        'nasc': fake.date_of_birth(minimum_age=18, maximum_age=40).strftime('%Y%m%d'),
    }
    candidate_row = {
        'candidate_nome': fake.name(),
        'candidate_nasc': fake.date_of_birth(minimum_age=50, maximum_age=90).strftime('%Y%m%d'),
    }

    # 2. Definição das Regras
    rules = [
        {'source_column': 'nome', 'target_column': 'nome', 'similarity': 'exact', 'weight': 0.5},
        {'source_column': 'nasc', 'target_column': 'nasc', 'similarity': 'exact', 'weight': 0.5}
    ]

    # 3. Execução e Asserção
    result = calculate_pair_scores_and_similarities(
        source_row_dict=source_row,
        candidate_data_dict_prefixed=candidate_row,
        phase_rules_dicts=rules,
        workflow_config_dict=workflow_config
    )

    # Como os dados são completamente diferentes, o score com 'exact' deve ser 0
    assert result['match_score'] == 0.0
    assert result['sim_nome'] == 0.0
    assert result['sim_nasc'] == 0.0