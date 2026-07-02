import pytest
from unittest.mock import MagicMock, patch

from cidacsrl.adapters.outbound.elasticsearch.executors import (
    MultiSearchExecutor,
    SingleSearchExecutor,
)
from cidacsrl.adapters.outbound.telemetry.composite_linkage_telemetry_adapter import (
    CompositeLinkageTelemetryAdapter,
)
from cidacsrl.adapters.outbound.telemetry.formatted_log_telemetry_adapter import (
    FormattedLogTelemetryAdapter,
)
from cidacsrl.adapters.outbound.telemetry.jsonl_telemetry_adapter import (
    JsonlLinkageTelemetryAdapter,
)
from cidacsrl.bootstrap.linkage_bootstrap import (
    _build_telemetry_adapter,
    _resolve_search_executor,
)
from cidacsrl.config.models.execution_config import ExecutionConfig

pytestmark = pytest.mark.unit

_MODULE = "cidacsrl.bootstrap.linkage_bootstrap"


class TestResolveSearchExecutor:
    def test_multisearch_returns_multi_executor(self):
        executor = _resolve_search_executor({"es_connection_url": "http://es:9200", "search_strategy": "multisearch"})
        assert isinstance(executor, MultiSearchExecutor)

    def test_single_returns_single_executor(self):
        executor = _resolve_search_executor({"es_connection_url": "http://es:9200", "search_strategy": "single"})
        assert isinstance(executor, SingleSearchExecutor)

    def test_strategy_is_case_insensitive(self):
        executor = _resolve_search_executor({"es_connection_url": "http://es:9200", "search_strategy": "MULTISEARCH"})
        assert isinstance(executor, MultiSearchExecutor)

    def test_default_strategy_is_multisearch(self):
        executor = _resolve_search_executor({"es_connection_url": "http://es:9200"})
        assert isinstance(executor, MultiSearchExecutor)

    def test_unknown_strategy_raises_value_error(self):
        with pytest.raises(ValueError, match="desconhecida"):
            _resolve_search_executor({"es_connection_url": "http://es:9200", "search_strategy": "invalid"})


class TestBuildTelemetryAdapter:
    def _make_spec(self, project_name: str = "proj") -> MagicMock:
        spec = MagicMock()
        spec.linkage_project_name = project_name
        return spec

    def test_without_audit_log_returns_composite_with_only_log_adapter(self):
        config = ExecutionConfig(audit_log_path=None)
        adapter = _build_telemetry_adapter(config, self._make_spec(), job_id="job_1")

        assert isinstance(adapter, CompositeLinkageTelemetryAdapter)
        assert len(adapter._adapters) == 1
        assert isinstance(adapter._adapters[0], FormattedLogTelemetryAdapter)

    @patch(f"{_MODULE}.JsonlLinkageTelemetryAdapter")
    def test_with_audit_log_returns_composite_with_two_adapters(self, mock_jsonl_cls):
        config = ExecutionConfig(audit_log_path="/tmp/audit")
        spec = self._make_spec(project_name="my_project")
        adapter = _build_telemetry_adapter(config, spec, job_id="job_42")

        assert isinstance(adapter, CompositeLinkageTelemetryAdapter)
        assert len(adapter._adapters) == 2
        assert isinstance(adapter._adapters[0], FormattedLogTelemetryAdapter)
        assert adapter._adapters[1] is mock_jsonl_cls.return_value

    @patch(f"{_MODULE}.JsonlLinkageTelemetryAdapter")
    def test_with_audit_log_passes_correct_paths(self, mock_jsonl_cls):
        config = ExecutionConfig(audit_log_path="/tmp/audit")
        spec = self._make_spec(project_name="my_project")
        _build_telemetry_adapter(config, spec, job_id="job_42")

        expected_dir = "/tmp/audit/my_project/job_42"
        mock_jsonl_cls.assert_called_once_with(
            phases_path=f"{expected_dir}/phases.jsonl",
            units_path=f"{expected_dir}/units.jsonl",
            job_path=f"{expected_dir}/job.jsonl",
        )
