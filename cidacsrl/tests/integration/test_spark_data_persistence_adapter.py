import pytest
from pathlib import Path
from pyspark.sql import SparkSession, Row
from cidacsrl.cidacsrl.infra.adapters.outbound.spark_data_persistence_adapter import SparkDataPersistenceAdapter
from cidacsrl.cidacsrl.infra.configs.models.storage_config import OutputStorageConfig


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder \
        .appName("cidacsrl-test-persistence-adapter") \
        .master("local[2]") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    yield session
    session.stop()


def test_save_phase_output_materializes_isolated_directory_tree(spark, tmp_path):
    # Configuração de infraestrutura pura apontando para o diretório temporário do teste
    config = OutputStorageConfig(
        output_path=str(tmp_path / "output"),
        output_format="parquet"
    )
    
    adapter = SparkDataPersistenceAdapter(output_config=config)
    
    # Cria uma massa de dados de teste de pares identificados
    df_test = spark.createDataFrame([
        Row(source_id="A1", candidate_id="B1", match_score=0.98, phase_match="fase_teste")
    ])
    
    # Executa a escrita passando as chaves lógicas isoladas
    records_saved = adapter.save_phase_output(
        df=df_test,
        project_name="linkage_internacao_nascimentos",
        job_id="job_2026_test",
        unit_id="unit_BA",
        phase_name="fase_e2e_nome"
    )
    
    # Validações estruturais e físicas
    assert records_saved == 1
    
    expected_path = (
        tmp_path / "output" / "linkage_internacao_nascimentos" / "job_2026_test" / "unit_BA" / "fase_e2e_nome"
    )
    
    assert expected_path.exists()
    assert expected_path.is_dir()
    
    # Garante que o Spark gerou os fragmentos físicos reais lá dentro
    parquet_files = list(expected_path.glob("*.parquet"))
    assert len(parquet_files) > 0