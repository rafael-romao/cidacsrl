import pytest
from cidacsrl_rlp.src.linkage.similarity_functions import (
    exact_score_func,
    jaro_winkler_score_func,
    hamming_score_func,
)

# Testes para exact_score_func
@pytest.mark.parametrize(
    "s1, s2, expected",
    [
        ("abc", "abc", 1.0),  # Correspondência exata de strings
        ("abc", "def", 0.0),  # Strings diferentes
        (123, 123, 1.0),      # Correspondência exata de inteiros
        (123, "123", 1.0),    # Tipos diferentes, mesma representação de string
        (123, 456, 0.0),      # Inteiros diferentes
        ("abc", None, 0.0),   # Um é None
        (None, "abc", 0.0),   # Outro é None
        (None, None, 0.0),    # Ambos são None
        ("", "", 1.0),        # Strings vazias
        (" ", " ", 1.0),      # Strings de espaço em branco
        ("abc", "ABC", 0.0),  # Sensível a maiúsculas/minúsculas
    ],
)
def test_exact_score_func(s1, s2, expected):
    """Testa a função exact_score_func com vários cenários."""
    assert exact_score_func(s1, s2) == expected


# Testes para jaro_winkler_score_func
@pytest.mark.parametrize(
    "s1, s2, expected",
    [
        ("martha", "marhta", pytest.approx(0.961, abs=1e-3)), 
        ("jones", "johnson", pytest.approx(0.832, abs=1e-3)),
        ("test", "test", 1.0),          # Correspondência exata
        ("apple", "orange", pytest.approx(0.5777, abs=1e-3)), 
        ("", "", 1.0),                  # Strings vazias
        ("abc", "", 0.0),               # Uma vazia
        ("", "abc", 0.0),               # Uma vazia
        (None, None, 1.0),              # Ambos None
        ("abc", None, 0.0),             # Um None
        (None, "abc", 0.0),             # Um None
        (12345, "12345", 1.0),          # Conversão de int para str
    ],
)
def test_jaro_winkler_score_func(s1, s2, expected):
    """Testa a função jaro_winkler_score_func."""
    score = jaro_winkler_score_func(s1, s2)
    if isinstance(expected, float):
        assert score == pytest.approx(expected)
    else:
        assert score == expected


# Testes para hamming_score_func
@pytest.mark.parametrize(
    "s1, s2, expected",
    [
        # Casos válidos (mesmo comprimento)
        ("karolin", "kathrin", 1.0 - (3 / 7)), # Distância 3, comprimento 7
        ("hamming", "hamming", 1.0),           # Distância 0, correspondência exata
        ("1011101", "1001001", 1.0 - (2 / 7)), # Distância 2, comprimento 7
        ("", "", 1.0),                         # Strings vazias (comprimento 0)

        # Casos inválidos (comprimentos diferentes)
        ("abc", "ab", 0.0),
        ("short", "longer", 0.0),
        ("", "a", 0.0),
        ("a", "", 0.0),

        # Casos com None
        (None, None, 1.0),      # Tratados como ("", ""), mesmo comprimento
        ("abc", None, 0.0),     # Tratados como ("abc", ""), comprimentos diferentes
        (None, "abc", 0.0),     # Tratados como ("", "abc"), comprimentos diferentes
    ],
)
def test_hamming_score_func(s1, s2, expected):
    """Testa a função hamming_score_func."""
    score = hamming_score_func(s1, s2)
    assert score == pytest.approx(expected)