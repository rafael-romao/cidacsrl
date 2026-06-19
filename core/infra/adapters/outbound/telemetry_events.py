from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


_LINKAGE_SCHEMA_VERSION = "1"
_INDEXING_SCHEMA_VERSION = "1"


# ── Linkage records (Option B: one file per dimension) ────────────────────────

@dataclass(frozen=True)
class PhaseRecord:
    job_id: str
    project_name: str
    unit_id: str
    phase_index: int
    phase_name: str
    status: str  # "completed" | "skipped" | "exhausted"
    records_in: Optional[int] = None
    candidates_found: Optional[int] = None
    records_out: Optional[int] = None
    duration_s: Optional[float] = None
    search_duration_s: Optional[float] = None
    persist_duration_s: Optional[float] = None
    ts: str = field(default_factory=_ts)
    schema_version: str = field(default=_LINKAGE_SCHEMA_VERSION, init=False)


@dataclass(frozen=True)
class UnitRecord:
    job_id: str
    project_name: str
    unit_id: str
    started_at: str
    completed_at: str
    status: str  # "completed" | "failed"
    duration_s: float
    total_links: Optional[int] = None
    remaining: Optional[int] = None
    schema_version: str = field(default=_LINKAGE_SCHEMA_VERSION, init=False)


@dataclass(frozen=True)
class JobRecord:
    job_id: str
    project_name: str
    total_units: int
    started_at: str
    completed_at: str
    duration_s: float
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
