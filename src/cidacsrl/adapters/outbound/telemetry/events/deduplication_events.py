from dataclasses import dataclass, field
from datetime import datetime, timezone


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


_DEDUP_SCHEMA_VERSION = "1"


@dataclass(frozen=True)
class DeduplicationStartEvent:
    run_id: str
    id_source: str
    id_target: str
    output_col: str
    ts: str = field(default_factory=_ts)
    event: str = field(default="deduplication_start", init=False)
    schema_version: str = field(default=_DEDUP_SCHEMA_VERSION, init=False)


@dataclass(frozen=True)
class PairsLoadedEvent:
    run_id: str
    duration_s: float
    ts: str = field(default_factory=_ts)
    event: str = field(default="pairs_loaded", init=False)
    schema_version: str = field(default=_DEDUP_SCHEMA_VERSION, init=False)


@dataclass(frozen=True)
class ClustersFoundEvent:
    run_id: str
    duration_s: float
    ts: str = field(default_factory=_ts)
    event: str = field(default="clusters_found", init=False)
    schema_version: str = field(default=_DEDUP_SCHEMA_VERSION, init=False)


@dataclass(frozen=True)
class DeduplicationCompleteEvent:
    run_id: str
    total_duration_s: float
    ts: str = field(default_factory=_ts)
    event: str = field(default="deduplication_complete", init=False)
    schema_version: str = field(default=_DEDUP_SCHEMA_VERSION, init=False)
