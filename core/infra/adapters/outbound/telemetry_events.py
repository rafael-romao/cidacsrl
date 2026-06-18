from dataclasses import dataclass, field
from datetime import datetime, timezone


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


_LINKAGE_SCHEMA_VERSION = "1"
_INDEXING_SCHEMA_VERSION = "1"


# ── Linkage events ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class JobStartEvent:
    job_id: str
    project_name: str
    total_units: int
    ts: str = field(default_factory=_ts)
    event: str = field(default="job_start", init=False)
    schema_version: str = field(default=_LINKAGE_SCHEMA_VERSION, init=False)


@dataclass(frozen=True)
class UnitStartEvent:
    job_id: str
    unit_id: str
    pending_count: int
    ts: str = field(default_factory=_ts)
    event: str = field(default="unit_start", init=False)
    schema_version: str = field(default=_LINKAGE_SCHEMA_VERSION, init=False)


@dataclass(frozen=True)
class PhaseSkippedEvent:
    job_id: str
    unit_id: str
    phase_index: int
    phase_name: str
    ts: str = field(default_factory=_ts)
    event: str = field(default="phase_skipped", init=False)
    schema_version: str = field(default=_LINKAGE_SCHEMA_VERSION, init=False)


@dataclass(frozen=True)
class PhaseExhaustedEvent:
    job_id: str
    unit_id: str
    phase_index: int
    phase_name: str
    ts: str = field(default_factory=_ts)
    event: str = field(default="phase_exhausted", init=False)
    schema_version: str = field(default=_LINKAGE_SCHEMA_VERSION, init=False)


@dataclass(frozen=True)
class PhaseTelemetryEvent:
    job_id: str
    unit_id: str
    phase_index: int
    phase_name: str
    records_in: int
    candidates_found: int
    records_out: int
    duration_s: float
    search_duration_s: float
    persist_duration_s: float
    ts: str = field(default_factory=_ts)
    event: str = field(default="phase_telemetry", init=False)
    schema_version: str = field(default=_LINKAGE_SCHEMA_VERSION, init=False)


@dataclass(frozen=True)
class UnitCompleteEvent:
    job_id: str
    unit_id: str
    total_links: int
    remaining: int
    duration_s: float
    ts: str = field(default_factory=_ts)
    event: str = field(default="unit_complete", init=False)
    schema_version: str = field(default=_LINKAGE_SCHEMA_VERSION, init=False)


@dataclass(frozen=True)
class UnitFailureEvent:
    job_id: str
    unit_id: str
    error_message: str
    duration_s: float
    ts: str = field(default_factory=_ts)
    event: str = field(default="unit_failure", init=False)
    schema_version: str = field(default=_LINKAGE_SCHEMA_VERSION, init=False)


@dataclass(frozen=True)
class JobCompleteEvent:
    job_id: str
    total_units: int
    duration_s: float
    ts: str = field(default_factory=_ts)
    event: str = field(default="job_complete", init=False)
    schema_version: str = field(default=_LINKAGE_SCHEMA_VERSION, init=False)


# ── Indexing events ───────────────────────────────────────────────────────────

@dataclass(frozen=True)
class IndexingStartEvent:
    source_table: str
    index_name: str
    column_count: int
    ts: str = field(default_factory=_ts)
    event: str = field(default="indexing_start", init=False)
    schema_version: str = field(default=_INDEXING_SCHEMA_VERSION, init=False)


@dataclass(frozen=True)
class IndexEnsuredEvent:
    index_name: str
    duration_s: float
    ts: str = field(default_factory=_ts)
    event: str = field(default="index_ensured", init=False)
    schema_version: str = field(default=_INDEXING_SCHEMA_VERSION, init=False)


@dataclass(frozen=True)
class IndexingCompleteEvent:
    source_table: str
    index_name: str
    total_duration_s: float
    ts: str = field(default_factory=_ts)
    event: str = field(default="indexing_complete", init=False)
    schema_version: str = field(default=_INDEXING_SCHEMA_VERSION, init=False)
