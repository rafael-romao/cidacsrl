import pytest

from cidacsrl.config.models.execution_config import DataPartitioningConfig, ExecutionConfig
from cidacsrl.config.models.storage_config import OutputStorageConfig, SourceStorageConfig

pytestmark = pytest.mark.unit


class TestSourceStorageConfig:
    def test_valid_construction(self):
        cfg = SourceStorageConfig(source_path="data/input.parquet")
        assert cfg.source_path == "data/input.parquet"
        assert cfg.source_format == "parquet"

    def test_empty_source_path_raises(self):
        with pytest.raises(ValueError, match="'source_path' é obrigatório"):
            SourceStorageConfig(source_path="")

    def test_from_dict_valid(self):
        cfg = SourceStorageConfig.from_dict({"source_path": "s3://bucket/data", "source_format": "csv"})
        assert cfg.source_path == "s3://bucket/data"
        assert cfg.source_format == "csv"

    def test_from_dict_missing_source_path_raises(self):
        with pytest.raises(ValueError, match="'source_path' é obrigatório"):
            SourceStorageConfig.from_dict({})

    def test_from_dict_none_raises(self):
        with pytest.raises(ValueError, match="'source_path' é obrigatório"):
            SourceStorageConfig.from_dict(None)


class TestOutputStorageConfig:
    def test_valid_construction(self):
        cfg = OutputStorageConfig(output_path="data/output.parquet")
        assert cfg.output_path == "data/output.parquet"
        assert cfg.output_format == "parquet"

    def test_empty_output_path_raises(self):
        with pytest.raises(ValueError, match="'output_path' é obrigatório"):
            OutputStorageConfig(output_path="")

    def test_from_dict_missing_output_path_raises(self):
        with pytest.raises(ValueError, match="'output_path' é obrigatório"):
            OutputStorageConfig.from_dict({})


class TestDataPartitioningConfig:
    def test_valid_with_column_and_filters(self):
        cfg = DataPartitioningConfig(partition_column="uf", filter_partitions=["SP", "RJ"])
        assert cfg.has_filters is True

    def test_valid_with_column_no_filters(self):
        cfg = DataPartitioningConfig(partition_column="uf")
        assert cfg.has_filters is False

    def test_valid_empty(self):
        cfg = DataPartitioningConfig()
        assert cfg.partition_column is None
        assert cfg.filter_partitions == []

    def test_filter_partitions_without_column_raises(self):
        with pytest.raises(ValueError, match="'filter_partitions' requer 'partition_column'"):
            DataPartitioningConfig(partition_column=None, filter_partitions=["SP"])


class TestExecutionConfig:
    def test_valid_with_all_fields(self):
        cfg = ExecutionConfig(job_id="job_1", sample_fraction=0.5, audit_log_path="/tmp/audit")
        assert cfg.job_id == "job_1"
        assert cfg.sample_fraction == 0.5

    def test_job_id_auto_generated_when_absent(self):
        cfg = ExecutionConfig()
        assert cfg.job_id is not None
        assert cfg.job_id.startswith("job_")

    def test_sample_fraction_of_one_is_valid(self):
        cfg = ExecutionConfig(sample_fraction=1.0)
        assert cfg.sample_fraction == 1.0

    def test_sample_fraction_above_one_raises(self):
        with pytest.raises(ValueError, match="'sample_fraction'"):
            ExecutionConfig(sample_fraction=1.1)

    def test_sample_fraction_zero_raises(self):
        with pytest.raises(ValueError, match="'sample_fraction'"):
            ExecutionConfig(sample_fraction=0.0)

    def test_sample_fraction_negative_raises(self):
        with pytest.raises(ValueError, match="'sample_fraction'"):
            ExecutionConfig(sample_fraction=-0.5)

    def test_sample_fraction_none_is_valid(self):
        cfg = ExecutionConfig(sample_fraction=None)
        assert cfg.sample_fraction is None
