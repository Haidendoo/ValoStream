"""Shared, versioned release pointer. Readers see a complete old or new document."""

from dataclasses import asdict, dataclass
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
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
    watermarks: dict[str, str | None] | None = None

    def __post_init__(self):
        if not re.fullmatch(r"vs_release_[0-9a-f]{32}", self.schema):
            raise ValueError("Invalid managed release schema")
        if not self.source_ref or not re.fullmatch(r"[0-9a-fA-F]+", self.source_hash):
            raise ValueError("Missing source snapshot identity")
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", self.source_schema):
            raise ValueError("Invalid source schema")
        timestamp(self.built_at)
        if self.watermarks is not None:
            if set(self.watermarks) != {"interactions", "couriers"}:
                raise ValueError("Expected both event stream watermarks")
            for value in self.watermarks.values():
                if value is not None:
                    timestamp(value)


@dataclass(frozen=True)
class RetiredRelease:
    release: Release
    retired_at: str

    def __post_init__(self):
        timestamp(self.retired_at)


@dataclass(frozen=True)
class ReleaseState:
    active: Release
    previous: Release | None
    published_at: str
    version: int = 2
    retired: tuple[RetiredRelease, ...] = ()

    def __post_init__(self):
        if type(self.version) is not int or self.version not in (1, 2):
            raise ValueError("Unsupported release manifest version")
        timestamp(self.published_at)
        if self.previous and self.previous.schema == self.active.schema:
            raise ValueError("Active and previous releases must differ")
        schemas = [self.active.schema]
        if self.previous:
            schemas.append(self.previous.schema)
        schemas.extend(entry.release.schema for entry in self.retired)
        if len(schemas) != len(set(schemas)):
            raise ValueError("Release ownership entries must be unique")


def read_state(path):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return ReleaseState(
        active=Release(**data["active"]),
        previous=Release(**data["previous"]) if data["previous"] is not None else None,
        published_at=data["published_at"], version=data["version"],
        retired=tuple(RetiredRelease(Release(**entry["release"]), entry["retired_at"])
                      for entry in data.get("retired", [])),
    )


@contextmanager
def release_lease(manifest, schema, exclusive=False):
    """Protect a request's schema across processes; GC takes an exclusive lease."""
    if not re.fullmatch(r"vs_release_[0-9a-f]{32}", schema):
        raise ValueError("Invalid managed release schema")
    manifest = Path(manifest)
    directory = manifest.with_name(manifest.name + ".leases")
    directory.mkdir(parents=True, exist_ok=True)
    # Never unlink a lease file: another process may still hold its inode open.
    with (directory / schema).open("a") as handle:
        operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        fcntl.flock(handle, operation | fcntl.LOCK_NB)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


@contextmanager
def selected_release(manifest):
    for _ in range(3):
        state = read_state(manifest)
        with release_lease(manifest, state.active.schema):
            current = read_state(manifest)
            # A collector could have won the race before we took the lease.
            if current.active.schema != state.active.schema:
                continue
            yield current.active
            return
    raise ValueError("Serving release changed repeatedly during selection")


@dataclass(frozen=True)
class FreshnessPolicy:
    max_build_age_seconds: int = 0
    max_event_age_seconds: int = 0

    def __post_init__(self):
        if self.max_build_age_seconds < 0 or self.max_event_age_seconds < 0:
            raise ValueError("Freshness limits must be nonnegative; zero disables a limit")

    def evaluate(self, release, now=None):
        now = now or datetime.now(timezone.utc)
        build_age = (now - timestamp(release.built_at)).total_seconds()
        watermarks = release.watermarks or {}
        event_ages = {stream: ((now - timestamp(watermarks[stream])).total_seconds()
                              if watermarks.get(stream) is not None else None)
                      for stream in ("interactions", "couriers")}
        checks = []
        if self.max_build_age_seconds:
            checks.append((build_age, self.max_build_age_seconds))
        if self.max_event_age_seconds:
            checks += [(age, self.max_event_age_seconds) for age in event_ages.values()]
        status = "ok" if checks else "unchecked"
        if any(age is None for age, _ in checks):
            status = "unknown"
        elif any(age < -300 for age, _ in checks):
            status = "clock_skew"
        elif any(age > limit for age, limit in checks):
            status = "stale"
        return {"status": status, "build_age_seconds": build_age, "event_age_seconds": event_ages}


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
