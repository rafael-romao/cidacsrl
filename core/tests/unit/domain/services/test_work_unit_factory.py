import pytest
from core.application.domain.models.tracking.work_unit_factory import WorkUnitFactory

def test_create_execution_scope_global_when_no_partition_column():
    result = WorkUnitFactory.create_execution_scope(
        partition_column=None,
        filter_partitions=[]
    )
    
    assert len(result) == 1
    assert result[0].unit_id == "global"
    assert result[0].filters == {}

def test_create_execution_scope_partitioned_with_valid_values():
    result = WorkUnitFactory.create_execution_scope(
        partition_column="uf_internacao",
        filter_partitions=["BA", "SP MOCK"]
    )
    
    assert len(result) == 2
    
    assert result[0].unit_id == "uf_internacao_BA"
    assert result[0].filters == {"uf_internacao": "BA"}
    
    assert result[1].unit_id == "uf_internacao_SP_MOCK"
    assert result[1].filters == {"uf_internacao": "SP MOCK"}

def test_create_execution_scope_raises_value_error_when_partitions_empty():
    with pytest.raises(ValueError) as exc_info:
        WorkUnitFactory.create_execution_scope(
            partition_column="uf_internacao",
            filter_partitions=[]
        )
        
    assert "Coluna de partição 'uf_internacao' foi configurada" in str(exc_info.value)