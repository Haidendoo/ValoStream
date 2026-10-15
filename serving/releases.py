"""Shared, versioned release pointer. Readers see a complete old or new document."""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import tempfile


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def timestamp(value):
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("Release timestamps must include a timezone")
    return parsed


@dataclass(frozen=True)
class Release:
    schema: str
    source_ref: str
    source_hash: str
    source_schema: str
    built_at: str

    def __post_init__(self):
        if not re.fullmatch(r"vs_release_[0-9a-f]{32}", self.schema):
            raise ValueError("Invalid managed release schema")
        if not self.source_ref or not re.fullmatch(r"[0-9a-fA-F]+", self.source_hash):
            raise ValueError("Missing source snapshot identity")
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", self.source_schema):
            raise ValueError("Invalid source schema")
        timestamp(self.built_at)


@dataclass(frozen=True)
class ReleaseState:
    active: Release
    previous: Release | None
    published_at: str
    version: int = 1

    def __post_init__(self):
        if self.version != 1:
            raise ValueError("Unsupported release manifest version")
        timestamp(self.published_at)
        if self.previous and self.previous.schema == self.active.schema:
            raise ValueError("Active and previous releases must differ")


def read_state(path):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return ReleaseState(
        active=Release(**data["active"]),
        previous=Release(**data["previous"]) if data["previous"] is not None else None,
        published_at=data["published_at"], version=data["version"],
    )


def write_state(path, state):
    """Caller holds the writer lock. Never truncate the currently visible file."""
    path = Path(path)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=f".{path.name}.", delete=False) as handle:
            temporary = Path(handle.name)
            json.dump(asdict(state), handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        # Persist the rename as well as the file's content on local POSIX storage.
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
