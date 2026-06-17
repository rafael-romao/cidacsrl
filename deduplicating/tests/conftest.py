import pytest
from core.infra.configs.logging_config import configure_logging

configure_logging()


@pytest.fixture
def valid_workflow_config_data() -> dict:
    return {
        "app_name": "Test Deduplication",
        "storage": {
            "source_path": "data/linked.parquet",
            "output_path": "data/deduped.parquet",
            "source_format": "parquet",
            "output_format": "parquet",
        },
        "spark": {
            "spark_configs": {
                "spark.master": "local[*]",
                "spark.ui.enabled": "false",
            }
        },
        "deduplication": {
            "id_source_column": "id_table",
            "id_target_column": "candidate_id_table",
            "output_group_id_column": "cidacs_cluster_id",
        },
    }
