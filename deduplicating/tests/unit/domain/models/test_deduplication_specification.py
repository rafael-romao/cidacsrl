import pytest

from cidacsrl.domain.deduplication.deduplication_specification import DeduplicationSpecification

pytestmark = pytest.mark.unit


def test_valid_spec_is_built():
    spec = DeduplicationSpecification(
        id_source_column="id_table",
        id_target_column="candidate_id_table",
    )
    assert spec.id_source_column == "id_table"
    assert spec.id_target_column == "candidate_id_table"
    assert spec.output_group_id_column == "cidacs_cluster_id"


def test_default_output_group_id_column():
    spec = DeduplicationSpecification(id_source_column="src", id_target_column="dst")
    assert spec.output_group_id_column == "cidacs_cluster_id"


def test_custom_output_group_id_column():
    spec = DeduplicationSpecification(
        id_source_column="src",
        id_target_column="dst",
        output_group_id_column="grupo_id",
    )
    assert spec.output_group_id_column == "grupo_id"


def test_raises_when_columns_are_equal():
    with pytest.raises(ValueError, match="não podem ser iguais"):
        DeduplicationSpecification(id_source_column="id", id_target_column="id")


def test_raises_when_id_source_column_is_empty():
    with pytest.raises(ValueError, match="'id_source_column' não pode ser vazio"):
        DeduplicationSpecification(id_source_column="", id_target_column="dst")


def test_raises_when_id_target_column_is_empty():
    with pytest.raises(ValueError, match="'id_target_column' não pode ser vazio"):
        DeduplicationSpecification(id_source_column="src", id_target_column="")


def test_from_dict_happy_path():
    data = {
        "id_source_column": "id_table",
        "id_target_column": "candidate_id_table",
        "output_group_id_column": "grupo_id",
    }
    spec = DeduplicationSpecification.from_dict(data)
    assert spec.id_source_column == "id_table"
    assert spec.id_target_column == "candidate_id_table"
    assert spec.output_group_id_column == "grupo_id"


def test_from_dict_uses_default_group_column():
    spec = DeduplicationSpecification.from_dict(
        {"id_source_column": "src", "id_target_column": "dst"}
    )
    assert spec.output_group_id_column == "cidacs_cluster_id"


def test_from_dict_raises_on_missing_id_source_column():
    with pytest.raises(KeyError):
        DeduplicationSpecification.from_dict({"id_target_column": "dst"})


def test_from_dict_raises_on_missing_id_target_column():
    with pytest.raises(KeyError):
        DeduplicationSpecification.from_dict({"id_source_column": "src"})
