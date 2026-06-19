import pytest
from pathlib import Path
from pyspark.sql import SparkSession, Row
from core.infra.adapters.outbound.spark_data_persistence_adapter import SparkDataPersistenceAdapter
from cidacsrl.config.models.storage_config import OutputStorageConfig


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder \
        .appName("cidacsrl-test-persistence-adapter") \
        .master("local[2]") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    yield session
    session.stop()


def test_save_phase_output_without_partition(spark, tmp_path):
    config = OutputStorageConfig(
        output_path=str(tmp_path / "output"),
        output_format="parquet"
    )
    adapter = SparkDataPersistenceAdapter(output_config=config)

    df_test = spark.createDataFrame([
        Row(source_id="A1", candidate_id="B1", match_score=0.98, phase_match="fase_teste")
    ])

    records_saved = adapter.save_phase_output(
        df=df_test,
        project_name="linkage_internacao_nascimentos",
        phase_name="fase_e2e_nome",
    )

    assert records_saved == 1

    expected_path = tmp_path / "output" / "linkage_internacao_nascimentos" / "fase_e2e_nome"
    assert expected_path.exists()
    assert expected_path.is_dir()
    parquet_files = list(expected_path.glob("*.parquet"))
    assert len(parquet_files) > 0


def test_save_phase_output_with_hive_partition(spark, tmp_path):
    config = OutputStorageConfig(
        output_path=str(tmp_path / "output"),
        output_format="parquet"
    )
    adapter = SparkDataPersistenceAdapter(output_config=config)

    df_test = spark.createDataFrame([
        Row(source_id="A1", candidate_id="B1", match_score=0.98, phase_match="fase_teste", uf="BA")
    ])

    records_saved = adapter.save_phase_output(
        df=df_test,
        project_name="linkage_internacao_nascimentos",
        phase_name="fase_e2e_nome",
        partition_column="uf",
    )

    assert records_saved == 1

    hive_partition_path = (
        tmp_path / "output" / "linkage_internacao_nascimentos" / "fase_e2e_nome" / "uf=BA"
    )
    assert hive_partition_path.exists()
    assert hive_partition_path.is_dir()
    parquet_files = list(hive_partition_path.glob("*.parquet"))
    assert len(parquet_files) > 0


def test_save_phase_output_dynamic_partition_overwrite(spark, tmp_path):
    config = OutputStorageConfig(
        output_path=str(tmp_path / "output"),
        output_format="parquet"
    )
    adapter = SparkDataPersistenceAdapter(output_config=config)

    df_ba = spark.createDataFrame([Row(source_id="A1", candidate_id="B1", match_score=0.98, uf="BA")])
    df_sp = spark.createDataFrame([Row(source_id="A2", candidate_id="B2", match_score=0.95, uf="SP")])

    adapter.save_phase_output(
        df=df_ba,
        project_name="linkage_test",
        phase_name="fase_nome",
        partition_column="uf",
    )
    adapter.save_phase_output(
        df=df_sp,
        project_name="linkage_test",
        phase_name="fase_nome",
        partition_column="uf",
    )

    base_path = tmp_path / "output" / "linkage_test" / "fase_nome"

    # Ambas as partições devem coexistir
    assert (base_path / "uf=BA").exists()
    assert (base_path / "uf=SP").exists()

    # Reescrever BA não deve apagar SP
    df_ba_updated = spark.createDataFrame([Row(source_id="A3", candidate_id="B3", match_score=0.99, uf="BA")])
    adapter.save_phase_output(
        df=df_ba_updated,
        project_name="linkage_test",
        phase_name="fase_nome",
        partition_column="uf",
    )

    assert (base_path / "uf=BA").exists()
    assert (base_path / "uf=SP").exists()
