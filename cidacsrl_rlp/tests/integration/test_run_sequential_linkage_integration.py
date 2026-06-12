import pytest
from pathlib import Path

from cidacsrl_rlp.cidacsrl.infra.configs.loader import load_yaml
from cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper import bootstrap_sequential_linkage

pytestmark = pytest.mark.e2e

SHARED_CONFIGS_DIR = Path(__file__).parent.parent / "configs" / "shared"


def test_run_sequential_linkage_reads_configs_and_executes_full_pipeline(
    storage_config_data, es_config_data, test_paths
):
    """
    Teste E2E/Integração do RunSequentialLinkageUseCase.

    Pré-requisitos:
    - Container Elasticsearch rodando (CIDACSRL_ES_URL ou http://localhost:9200)
    - Índice 'nascimentos_example_index' já populado (100 documentos)
    - Parquet 'internacao_example' presente em tests/data/input/

    O teste:
    1. Lê a especificação de linkage e o config Spark a partir dos YAMLs compartilhados
    2. Executa o use-case completo via bootstrapper (ingestão → busca ES → scoring → persistência)
    3. Valida que os arquivos Parquet de saída foram gerados para cada fase habilitada
    """
    linkage_spec_data = load_yaml(SHARED_CONFIGS_DIR / "linkage_spec_e2e.yml")
    spark_config_data = load_yaml(SHARED_CONFIGS_DIR / "spark_local.yml")

    # ------------------------------------------------------------------
    # Execução completa: ingestion → get_candidates → scoring → write
    # ------------------------------------------------------------------
    bootstrap_sequential_linkage(
        storage_config_data=storage_config_data,
        linkage_spec_data=linkage_spec_data,
        es_config_data=es_config_data,
        spark_config_data=spark_config_data,
    )

    # ------------------------------------------------------------------
    # Valida persistência dos resultados por fase habilitada
    # ------------------------------------------------------------------
    output_data_dir = test_paths["output_data"]
    enabled_phases = [
        (i + 1, phase["phase_name"])
        for i, phase in enumerate(linkage_spec_data["blocking_phases"])
        if phase.get("enabled", True)
    ]

    assert enabled_phases, "Nenhuma fase habilitada encontrada na especificação de linkage."

    for phase_index, phase_name in enabled_phases:
        phase_output_dir = output_data_dir / f"phase_{phase_index}_{phase_name}"

        assert phase_output_dir.exists(), (
            f"Diretório de saída da fase {phase_index} ({phase_name}) não foi gerado: {phase_output_dir}"
        )

        parquet_files = list(phase_output_dir.glob("*.parquet"))
        assert parquet_files, (
            f"Nenhum arquivo Parquet gerado para a fase {phase_index} ({phase_name}) em {phase_output_dir}."
        )