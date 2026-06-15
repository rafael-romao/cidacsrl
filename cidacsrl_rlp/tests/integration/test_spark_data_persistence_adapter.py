import pytest
from pathlib import Path
from pyspark.sql import SparkSession
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_data_persistence_adapter import SparkDataPersistenceAdapter
from cidacsrl_rlp.cidacsrl.infra.configs.loader import parse_output_storage_config


@pytest.fixture(scope="module")
def local_spark():
    """Cria uma sessão local e isolada do Spark para este módulo de integração."""
    spark = SparkSession.builder \
        .appName("CIDACS-RL Integration Testing Persistence") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def simulated_persistence_config(tmp_path):
    output_dir = tmp_path / "output_data"
    output_dir.mkdir()
    
    storage_data = {
        "output_path": str(output_dir),
        "output_format": "parquet"
    }
    
    return {
        "config": parse_output_storage_config(storage_data),
        "output_dir": output_dir
    }


def test_save_linkage_output_consolidates_and_persists_data_slices_correctly(
    local_spark, 
    simulated_persistence_config
):
    unit_id = "uf_internacao_BA"
    
    data_phase_1 = [{"codigo_internacao": "I01", "codigo_nascimento": "N01", "score": 1.0}]
    data_phase_2 = [{"codigo_internacao": "I02", "codigo_nascimento": "N02", "score": 0.85}]
    
    df_1 = local_spark.createDataFrame(data_phase_1)
    df_2 = local_spark.createDataFrame(data_phase_2)
    
    adapter = SparkDataPersistenceAdapter(
        spark_session=local_spark,
        config=simulated_persistence_config["config"]
    )
    
    total_records = adapter.save_linkage_output(
        phase_outputs=[df_1, df_2],
        unit_id=unit_id
    )
    
    assert total_records == 2
    
    expected_unit_path = simulated_persistence_config["output_dir"] / f"unit_{unit_id}"
    assert expected_unit_path.exists()
    assert expected_unit_path.is_dir()
    
    persisted_df = local_spark.read.format("parquet").load(str(expected_unit_path))
    rows = persisted_df.collect()
    
    assert len(rows) == 2
    ids_internacao = [row["codigo_internacao"] for row in rows]
    assert "I01" in ids_internacao
    assert "I02" in ids_internacao


def test_save_linkage_output_returns_zero_when_phase_outputs_is_empty(
    local_spark,
    simulated_persistence_config
):
    adapter = SparkDataPersistenceAdapter(
        spark_session=local_spark,
        config=simulated_persistence_config["config"]
    )
    
    total_records = adapter.save_linkage_output(
        phase_outputs=[],
        unit_id="uf_empty"
    )
    
    assert total_records == 0
    
    expected_unit_path = simulated_persistence_config["output_dir"] / "unit_uf_empty"
    assert not expected_unit_path.exists()