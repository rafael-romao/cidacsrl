from unittest.mock import MagicMock

import pytest

from cidacsrl.adapters.outbound.telemetry.composite_linkage_telemetry_adapter import (
    CompositeIndexingTelemetryAdapter,
)

pytestmark = pytest.mark.unit


def test_log_index_ensured_forwards_source_table_to_all_adapters():
    sub_adapter_1 = MagicMock()
    sub_adapter_2 = MagicMock()
    composite = CompositeIndexingTelemetryAdapter(adapters=[sub_adapter_1, sub_adapter_2])

    composite.log_index_ensured(source_table="pacientes", index_name="pacientes_index", duration=1.5)

    sub_adapter_1.log_index_ensured.assert_called_once_with("pacientes", "pacientes_index", 1.5)
    sub_adapter_2.log_index_ensured.assert_called_once_with("pacientes", "pacientes_index", 1.5)
