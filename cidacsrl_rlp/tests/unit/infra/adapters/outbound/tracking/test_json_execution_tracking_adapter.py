import json
import pytest
from datetime import datetime
from cidacsrl_rlp.cidacsrl.domain.models.tracking.work_unit import WorkUnitMetadata, WorkUnitStatus, WorkUnitStatus
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.json_execution_tracking_adapter import JSONExecutionTrackingAdapter

@pytest.fixture
def tracking_dir(tmp_path):
    return tmp_path / "tracking_logs"

@pytest.fixture
def adapter(tracking_dir):
    return JSONExecutionTrackingAdapter(tracking_directory=str(tracking_dir))

def test_initialize_job_state_creates_new_json_file(adapter, tracking_dir):
    job_id = "job_test_01"
    work_units = [
        WorkUnitMetadata(unit_id="uf_BA", filters={"uf": "BA"}),
        WorkUnitMetadata(unit_id="uf_SP", filters={"uf": "SP"})
    ]
    
    adapter.initialize_job_state(job_id, work_units)
    
    expected_file = tracking_dir / f"job_{job_id}_state.json"
    assert expected_file.exists()
    
    with open(expected_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    assert "uf_BA" in data
    assert "uf_SP" in data
    assert data["uf_BA"]["status"] == WorkUnitStatus.PENDING.value
    assert data["uf_BA"]["filters"] == {"uf": "BA"}
    assert data["uf_SP"]["status"] == WorkUnitStatus.PENDING.value

def test_initialize_job_state_does_not_overwrite_existing_progress(adapter, tracking_dir):
    job_id = "job_resilient_02"
    file_path = tracking_dir / f"job_{job_id}_state.json"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    pre_existing_state = {
        "uf_BA": {
            "unit_id": "uf_BA",
            "filters": {"uf": "BA"},
            "status": WorkUnitStatus.COMPLETED.value,
            "records_processed": 500,
            "started_at": "2026-06-15T10:00:00",
            "finished_at": "2026-06-15T10:05:00",
            "error_message": None
        }
    }
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(pre_existing_state, f)
        
    work_units = [WorkUnitMetadata(unit_id="uf_BA", filters={"uf": "BA"})]
    
    adapter.initialize_job_state(job_id, work_units)
    
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    assert data["uf_BA"]["status"] == WorkUnitStatus.COMPLETED.value
    assert data["uf_BA"]["records_processed"] == 500

def test_get_pending_work_units_filters_out_completed_records(adapter, tracking_dir):
    job_id = "job_filter_03"
    file_path = tracking_dir / f"job_{job_id}_state.json"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    mixed_state = {
        "uf_BA": {"unit_id": "uf_BA", "status": WorkUnitStatus.COMPLETED.value, "filters": {"uf": "BA"}},
        "uf_SP": {"unit_id": "uf_SP", "status": WorkUnitStatus.PENDING.value, "filters": {"uf": "SP"}},
        "uf_RJ": {"unit_id": "uf_RJ", "status": WorkUnitStatus.FAILED.value, "filters": {"uf": "RJ"}, "error_message": "Memory Out"}
    }
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(mixed_state, f)
        
    pending = adapter.get_pending_work_units(job_id)
    
    assert len(pending) == 2
    pending_ids = [p.unit_id for p in pending]
    assert "uf_SP" in pending_ids
    assert "uf_RJ" in pending_ids
    assert "uf_BA" not in pending_ids

def test_update_work_unit_status_transitions_correctly(adapter, tracking_dir):
    job_id = "job_transition_04"
    work_units = [WorkUnitMetadata(unit_id="uf_BA", filters={"uf": "BA"})]
    adapter.initialize_job_state(job_id, work_units)
    
    adapter.update_work_unit_status(job_id, "uf_BA", WorkUnitStatus.PROCESSING)
    
    file_path = tracking_dir / f"job_{job_id}_state.json"
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["uf_BA"]["status"] == WorkUnitStatus.PROCESSING.value
    assert data["uf_BA"]["started_at"] is not None
    assert data["uf_BA"]["error_message"] is None
    
    adapter.update_work_unit_status(job_id, "uf_BA", WorkUnitStatus.COMPLETED, records_processed=1250)
    
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["uf_BA"]["status"] == WorkUnitStatus.COMPLETED.value
    assert data["uf_BA"]["records_processed"] == 1250
    assert data["uf_BA"]["finished_at"] is not None

def test_update_work_unit_status_raises_value_error_if_unit_not_mapped(adapter):
    with pytest.raises(ValueError) as exc_info:
        adapter.update_work_unit_status("invalid_job", "missing_unit", WorkUnitStatus.PROCESSING)
    assert "não mapeada no Job" in str(exc_info.value)