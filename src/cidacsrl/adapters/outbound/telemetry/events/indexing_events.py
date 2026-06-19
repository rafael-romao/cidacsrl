from dataclasses import dataclass, field
from datetime import datetime, timezone


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


_INDEXING_SCHEMA_VERSION = "1"


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
    source_table: str
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
